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
import bagit
import json
from json import loads,dumps
from celery import Celery
import celeryconfig
from bson.objectid import ObjectId
from uuid import uuid5, NAMESPACE_DNS
from lxml import etree as ET
#import xml.etree.cElementTree as ET
from operator import is_not
from functools import partial
from string import whitespace
import datetime
import jinja2
from inspect import cleandoc


repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"


app = Celery()
app.config_from_object(celeryconfig)

ou_derivative_bag_url = "https://bag.ou.edu/derivative"
recipe_url = ou_derivative_bag_url + "/{0}/{1}/{2}.json"

#basedir = "/data/web_data/static"
#hostname = "https://cc.lib.ou.edu"
base_url = "https://cc.lib.ou.edu"
api_url = "{0}/api".format(base_url)
catalog_url = "{0}/catalog/data/catalog/digital_objects/.json".format(api_url)
search_url = "{0}?query={{\"filter\": {{\"bag\": \"{1}\"}}}}"
#apikeypath = "/code/alma_api_key"
#cctokenfile = "/code/cybercom_token"


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

    return "{0}/oulib_tasks/{1}".format(base_url, task_id)



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

def get_mmsid(path_to_bag,bagName):
    #s3_bucket='ul-bagit'
    #s3 = boto3.resource('s3')
    #s3_key = "{0}/{1}/{2}".format('source', bag, 'bag-info.txt')
    #recipe_obj = s3.Object(s3_bucket, s3_key)

    mmsid = re.findall("(?<!^)(?<!\d)\d{8,19}(?!\d)", bagName)
    if mmsid:
        return mmsid[-1]
    fh =open(path_to_bag+"bag-info.txt")
    bag_info = yaml.load(fh)
    try:
        mmsid = bag_info['FIELD_EXTERNAL_DESCRIPTION'].split()[-1].strip()
    except KeyError:
        logging.error("Cannot determine mmsid for bag from bag-info: {0}".format(bagName))
        return None
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None

def searchcatalog(bag):
    #resp = requests.get(search_url.format(catalog_url, bag))
    db_client = app.backend.database.client
    collection = db_client.cybercom.catalog
    query = {"bag":bag}
    catalogCursor = collection.find(query)
    if catalogCursor.count():
        return catalogCursor.__getitem__(0)


def updateCatalog(bag,paramstring,mmsid=None):

    catalogitem = searchcatalog(bag)
    if catalogitem == None:
        return False
    if paramstring not in catalogitem["derivatives"]:
        catalogitem["derivatives"][paramstring]={}
    ####   Check for error.

    path = "/mnt/{0}/{1}/".format("derivative", bag)
    db_client = app.backend.database.client
    collection = db_client.cybercom.catalog
    if mmsid == None:
        if "error" in catalogitem["application"]["islandora"].keys():
            catalogitem["application"]["islandora"]["error"].append("mmsid not found")
            #collection.update_one(,catalogitem)
        else:
            catalogitem["application"]["islandora"].update({"error":["mmsid not found"]})
        return True
    catalogitem["derivatives"][paramstring]["recipe"] = recipe_url.format(bag, paramstring, bag.lower())
    catalogitem["derivatives"][paramstring]["datetime"] = datetime.datetime.utcnow().isoformat()
    #catalogitem["derivatives"][paramstring]["pages"] = listpagefiles(bag, paramstring)

    #token = open(cctokenfile).read().strip()
    #headers = {"Content-Type": "application/json", "Authorization": "Token {0}".format(token)}
    #req = requests.post(catalog_url, data=dumps(catalogitem), headers=headers)
    #req.raise_for_status()
    return True


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
        mmsid =get_mmsid(path_to_bag,bag)
        if mmsid:
            bags_with_mmsids[bag]=OrderedDict()
            bags_with_mmsids[bag]['mmsid']=mmsid
            path_to_tif_files_of_bag = "/mnt/{0}/{1}/data/*.tif".format(s3_source,bag)
            outdir = "/mnt/{0}/{1}/{2}".format(s3_destination,bag,formatparams)
            if formatparams not in str(check_output(["ls","-l","/mnt/derivative/{0}/".format(bag)])):
                os.makedirs(outdir)
            for file in glob.glob(path_to_tif_files_of_bag):
                outpath = '/mnt/{0}/{1}/data/{2}/{3}.{4}'.format("derivative",bag,formatparams,file.split('/')[-1].split('.')[0].lower(),_formatextension(outformat))
                processimage(inpath=file,outpath=outpath,outformat=_formatextension(outformat))
        else:
            updateCatalog(bag,formatparams,mmsid)
    return {"local_derivatives": "{0}/oulib_tasks/{1}".format(base_url, task_id), "s3_destination": s3_destination,
            "task_id": task_id,"bags":bags_with_mmsids,"format_params":formatparams}

def bag_derivative(bag_name,update_manifest=True):
    """
        This methods create a bag for the derivative folder
        and updates the bag-info.txt generated
        args :
            bagName: str
            update_manifest : boolean
    """

    path = "/mnt/{0}/{1}".format("derivative",bag_name)
    try:
        bag=bagit.Bag(path)
    except bagit.BagError:
        bag = bagit.make_bag(path)

    bag.info['External-Description'] = bag_name
    bag.info['External-Identifier'] = 'University of Oklahoma Libraries'

    try:
        bag.save(manifests=update_manifest)
    except IOError as err:
        logging.error(err)

def recipe_file_creation(bag_name,mmsid,formatparams,title=None):
    """
        This method creates the recipe.json file and updates it into the derivative folder of the bag
        args:
            bag_name: str - name of the bag
            mmsid: dictionary "mmsid":value
            formatparams :  str eg . jpeg_040_antialias
    """
    path = "/mnt/{0}/{1}".format("derivative",bag_name)
    try:
        bag = bagit.Bag(path)
        payload = bag.payload_entries()
        recipefile = "{0}/{1}.json".format(path,bag_name)
        recipe=make_recipe(bag_name,mmsid,payload,formatparams,title)
        logging.debug("Writing recipe to: {0}".format(recipefile))
        with open(recipefile,"w") as f:
            f.write(recipe)
        bag.save()
    except bagit.BagError:
        logging.debug("Not a bag: {0}".format(path))
    except IOError as err:
        logging.error(err)

def make_recipe(bag_name,mmsid,payload,formatparams,title):
    """
        This file returns a dictionary with all the details needed by the recipe
        args:
            bag_name = str
            mmsid = dictionary , "mmsid":value
            payload = dictionary , directory structure inside the /data of the bag
            formatparams :  str eg . jpeg_040_antialias
    """
    meta = OrderedDict()
    meta['recipe']= OrderedDict()
    meta['recipe']['import'] = 'book'
    meta['recipe']['update'] = 'false'
    meta['recipe']['uuid'] = str(uuid5(repoUUID,bag_name))
    meta['recipe']['label'] = title

    bib = get_bib_record(mmsid["mmsid"])
    path = "/mnt/{0}/{1}".format("derivative", bag_name)
    meta['recipe']['metadata']=OrderedDict();
    if get_marc_xml(mmsid["mmsid"],path,bib):
        meta['recipe']['metadata']['marcxml'] = "{0}/{1}/{2}/marc.xml".format(ou_derivative_bag_url, bag_name, formatparams)
    else:
        meta['recipe']['metadata']['marcxml'] = "{0}/{1}/marc.xml".format(ou_derivative_bag_url, bag_name)
    if title is None:
        logging.debug("Getting title from marc file")
        meta['recipe']['label']= get_title_from_marc(bib)
    meta['recipe']['pages'] = process_manifest(bag_name, payload, formatparams)
    logging.debug("Generated JSON:\n{0}".format(dumps(meta, indent=4)))
    return dumps(meta, indent=4, ensure_ascii=False).encode("UTF-8")


def process_manifest(bag_name,payload,formatparams=None):
    template = """
    	{"label" : {{ idx }},"file" : {% if formatparams %} "{{"{}/{}/{}/{}".format(ou_derivative_bag_url, bagname, formatparams, file[0])}}" {% else %} "{{"{}/{}/{}".format(ou_derivative_bag_url, bagname, filename)}}"{% endif%},{% for hash_key,hash_value in file[1].items() %}"{{ hash_key }}" : "{{ hash_value }}",{% endfor%} "exif":"{{"{}.exif.txt".format(file[0].split("/")[1])}}"}
    """
    pages=[]
    env = jinja2.Environment()
    tmplt = env.from_string(cleandoc(template))
    for idx, file in enumerate(payload.items()):
        page_str = tmplt.render(ou_derivative_bag_url=ou_derivative_bag_url, bagname=bag_name, idx=idx,
                                   formatparams=formatparams, file=file)
        # print(page)
        page = json.loads(page_str)
        page['uuid'] = str(uuid5(repoUUID, "{0}/{1}".format(bag_name, file[0])))
        pages.append[page]
    return pages

def get_marc_datafield(tag_id, xml_tree):
    try:
        return xml_tree.xpath("record/datafield[@tag={0}]".format(tag_id))[0]
    except IndexError:
        return None

def get_marc_subfield_text(tag_id, sub_code, xml_tree):
    try:
        return xml_tree.xpath("record/datafield[@tag={0}]/subfield[@code='{1}']".format(tag_id, sub_code))[0].text
    except IndexError:
        return None

def get_title_from_marc(xml):
    tag_preferences = OrderedDict([
        # tag id, [ subfield codes ]
        (130, ['a']),
        (240, ['a']),
        (245, ['a', 'b'])
    ])
    xml_tree = ET.XML(xml)
    for tag in tag_preferences.keys():
        if get_marc_datafield(tag, xml_tree) is not None:
            title_parts = [get_marc_subfield_text(tag, code, xml_tree) for code in tag_preferences[tag]]
            title_parts = list(filter(partial(is_not, None), title_parts))  # remove None values
            if len(title_parts) > 1:
                title = " ".join(title_parts)
            else:
                title = title_parts[0]
            return title.strip(whitespace + "/,")

def get_marc_xml(mmsid,path,bib):
    if bib is None:
        return False
    record = ET.fromstring(bib).find("record")
    record.attrib['xmlns'] = "http://www.loc.gov/MARC21/slim"
    if record.find(".//*[@tag='001']") is None and mmsid is not None:
        controlfield = ET.Element("controlfield",tag="001")
        controlfield.text=mmsid
        record.insert(1,controlfield)
    marc21 = ET.ElementTree(record)
    try:
        marc21.write(path+"/marc.xml", encoding='utf-8', xml_declaration=True)
        return True
    except IOError as err:
        logging.error(err)
        return False

def get_bib_record(mmsid):
    """
        Queries the ALMA with MMS ID to obtain corresponding MARC XML
    """
    url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"

    apikey = os.environ.get('ALMA_KEY')
    if not apikey:
        logging.error("Could not get Alma key")
    if apikey and mmsid:
        try:
            response = requests.get(url.format(mmsid,apikey))
            if response.status_code == requests.codes.ok:
                return response.content
            else:
                logging.error(response.content)
                return {"error":"Alma server returned code: {0}".format(response.status_code)}
        except requests.ConnectionError as error:
            logging.error("Alma Connection Error")
            return {"error":"Alma connection error - Try Again Later"}


@task
def process_recipe(derivative_args):

    """
        This function generates the recipe file and returns the json structure for each bag.

        params:
        derivative_args:The arguments returned by readSource_updateDerivative function.
    """

    #task_id= derivative_args.get('task_id')
    #bags = derivative_args.get('bags') #bags = { "bagname1" : { "mmsid": value} , "bagName2":{"mmsid":value}, ..}
    #formatparams = derivative_args.get('format_params')
    formatparams="jpeg_040_antalias"
    bags={"Abbati_1703":{"mmsid":9932140502042}}
    for bag_name,mmsid in bags.items():
        bag_derivative(bag_name)
        recipe_file_creation(bag_name,mmsid,formatparams)
        #updateCatalog(bag_name,formatparams,mmsid["mmsid"])
        return "derivative bag info generated"
    

"""
@task
def insert_data_into_mongoDB():
    response = requests.get()
    jobj = response.json()
    results = jobj.get('results')
    db_client = app.backend.database.client
    print(db_client.database_names())
    database = db_client["cybercom"]

    if not "catalog" in db_client.cybercom.collection_names():
        mycol = database["catalog"]
        for data in results:
            mycol.insert_one(data)
        return "successful"
    return "already exists"
   # mydict = {"name": "hello", "address": "Norman"}
    #x = mycol.insert_one(mydict)

"""
