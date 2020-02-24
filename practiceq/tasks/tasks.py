import random

import requests
from celery import chain
from celery.task import task
from PIL import Image
from subprocess import check_call, check_output
from tempfile import NamedTemporaryFile
import os,boto3,shutil
import glob as glob
import yaml
import logging
import re
from collections import OrderedDict


#basedir = "/data/web_data/static"
hostname = "https://cc.lib.ou.edu"


def _formatextension(imageformat):
    """ get common file extension of image format """
    imgextensions = {"JPEG": "jpg",
                     "TIFF": "tif",
                             }
    try:
        return imgextensions[imageformat.upper()]
    except KeyError:
        return imageformat.lower()

def _params_as_string(outformat="", filter="", scale=None, crop=None):
    """
    Internal function to return image processing parameters as a single string
    Input: outformat="TIFF", filter="ANTIALIAS", scale=0.5, crop=[10, 10, 200, 200]
    Returns: tiff_050_antialias_10_10_200_200
    """
    imgformat = outformat.lower()
    imgfilter = filter.lower() if scale else None  # do not include filter if not scaled
    imgscale = str(int(scale * 100)).zfill(3) if scale else "100"
    imgcrop = "_".join((str(x) for x in crop)) if crop else None
    return "_".join((x for x in (imgformat, imgscale, imgfilter, imgcrop) if x))

def _processimage(inpath, outpath, outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Internal function to create image derivatives

    jpg as outformat isn't available . So checking whether outformat is jpg or 'tif' so that we can save it directly
    with the outpath extension available. Else save it with image.save(outpath,outformat).
    """
    image = Image.open(inpath)

    if crop:
        image = image.crop(crop)

    if scale:
        try:
            imagefilter = getattr(Image, filter.upper())
        except(AttributeError):
            print("Please Provide the correct filter for Image e.g - ANTIALIAS")
        size = [x * scale for x in image.size]
        image.thumbnail(size, imagefilter)
    if(outformat == 'jpg' or outformat=='tif'):
        image.save(outpath)
    else:
        try:
            image.save(outpath,outformat)
        except KeyError:
            print("Please Provide the correct OutputFormat for the image")

@task()
def processimage(inpath, outpath, outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Digilab TIFF derivative Task

    args:
      inpath - path string to input image
      outpath - path string to output image
      outformat - string representation of image format - default is "TIFF"
      scale - percentage to scale by represented as a decimal
      filter - string representing filter to apply to resized image - default is "ANTIALIAS"
      crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
    """

    task_id = str(processimage.request.id)
    #create Result Directory
    #resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)
    #os.makedirs(resultpath)

    _processimage(inpath=inpath,
                  outpath=outpath,
                  outformat=outformat,
                  filter=filter,
                  scale=scale,
                  crop=crop
                  )

    return "{0}/oulib_tasks/{1}".format(hostname, task_id)



bagList=[]

def getAllBags():
    response = requests.get('https://cc.lib.ou.edu/api/catalog/data/catalog/digital_objects/?query={"filter":{"department":"DigiLab","project":{"$ne":"private"},"locations.s3.exists":{"$eq":true},"derivatives.jpeg_040_antialias.recipe":{"$exists":false}}}&format=json&page_size=0')
    jobj = response.json()
    results=jobj.get('results')
    for obj in results:
        yield obj.get('bag')

@task
def getSample():
    try:
        return list(random.sample(list(getAllBags()), 4))
    except:
        return getAllBags()
@task
def automate():
    """
    This automates the process of derivative creation.
    :return: string "kicked off or not"
    """
    result = chain(getSample.s(),readSource_updateDerivative.s())
    result.delay()
    return "automate kicked off"

def get_mmsid(path_to_bag):
    #s3_bucket='ul-bagit'
    #s3 = boto3.resource('s3')
    #s3_key = "{0}/{1}/{2}".format('source', bag, 'bag-info.txt')
    #recipe_obj = s3.Object(s3_bucket, s3_key)
    fh =open(path_to_bag+"bag-info.txt")
    bag_info = yaml.load(fh)
    try:
        mmsid = bag_info['FIELD_EXTERNAL_DESCRIPTION'].split()[-1].strip()
    except KeyError:
        logging.error("Cannot determine mmsid for bag: {0}".format(bag))
        return None
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None


@task
def readSource_updateDerivative(bags,s3_source="source",s3_destination="derivative",outformat="TIFF",filter='ANTALIAS',scale=None, crop=None):
    """
    bagname = List containing bagnames eg : [bag1,bag2...]
    source = source file.
    outformat = "TIFF or jpeg or jpg or tif"
    filter = "ANTALIAS" - filter type of the image.
    scale = At what scale do you need the reduction of the size - eg 0.40 or o.20
    crop = size in which the image needs to be cropped, Provide it as a list - eg - [10,10,20,40]

    """
    bags_with_mmsids = OrderedDict()
    for bag in bags:
        task_id = str(readSource_updateDerivative.request.id)
        formatparams = _params_as_string(outformat,filter,scale,crop)

        path_to_bag = "/mnt/{0}/{1}/".format(s3_source,bag)
        mmsid =get_mmsid(path_to_bag)
        bags_with_mmsids[bag]=OrderedDict()
        bags_with_mmsids[bag]['mmsid']=mmsid
        path_to_tif_files_of_bag = "/mnt/{0}/{1}/data/*.tif".format(s3_source,bag)
        outdir = "/mnt/{0}/{1}/data/{2}".format(s3_destination,bag,formatparams)
        #print(os.getuid(), os.getgid())
        #print(check_output(['ls','-l','/mnt/']))
        if "data" not in str(check_output(["ls","-l","/mnt/derivative/{0}/".format(bag)])):
            os.makedirs(outdir)
        #print(glob.glob(path))

        for file in glob.glob(path_to_tif_files_of_bag):
            outpath = '/mnt/{0}/{1}/data/{2}/{3}.{4}'.format("derivative",bag,formatparams,file.split('/')[-1].split('.')[0].lower(),_formatextension(outformat))
            processimage(inpath=file,outpath=outpath,outformat=_formatextension(outformat))
    return {"local_derivatives": "{0}/oulib_tasks/{1}".format(hostname, task_id), "s3_destination": s3_destination,
            "task_id": task_id,"bags":bags_with_mmsids,"format_params":formatparams}

"""
@task
def generate_recipe(derivative_args):
"""
"""
    This function generates the recipe file and returns the json structure for each bag.

    params:
    derivative_args:The arguments returned by readSource_updateDerivative function.
"""
"""
    task_id= derivative_args.get('task_id')
    bags = derivative_args.get('bags') #bags = { "bagname1" : { "mmsid": value} , "bagName2":{"mmsid":value}, ..}
    formatparams = derivative_args.get('format_params')

"""



