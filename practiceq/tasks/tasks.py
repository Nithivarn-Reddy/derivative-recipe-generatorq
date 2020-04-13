import random
from celery import chain
from celery.task import task
from subprocess import check_call, check_output
import glob as glob
from celery import Celery
import celeryconfig
from uuid import uuid5, NAMESPACE_DNS
import datetime
from .derivative_utils import _params_as_string,_formatextension,processimage
from .utils import *
from .recipe_utils import *



repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"


app = Celery()
app.config_from_object(celeryconfig)

ou_derivative_bag_url = "https://bag.ou.edu/derivative"
recipe_url = ou_derivative_bag_url + "/{0}/{1}/{2}.json"
base_url = "https://cc.lib.ou.edu"
api_url = "{0}/api".format(base_url)
search_url = "{0}?query={{\"filter\": {{\"bag\": \"{1}\"}}}}"
#apikeypath = "/code/alma_api_key"
#cctokenfile = "/code/cybercom_token"

bagList=[]

def getAllBags():
    response = requests.get('https://cc.lib.ou.edu/api/catalog/data/catalog/digital_objects/?query={"filter":{"department":"DigiLab","project":{"$ne":"private"},"locations.s3.exists":{"$eq":true},"derivatives.jpeg_040_antialias.recipe":{"$exists":false,"$error":ne}}}&format=json&page_size=0')
    jobj = response.json()
    results=jobj.get('results')
    for obj in results:
        yield obj.get('bag')

@task
def getSample():
    try:
        #list(random.sample(list(getAllBags()), 4))
        return ['Apian_1545','Abbati_1703']
    except:
        return getAllBags()
@task
def automate(outformat,filter,scale,crop):
    """
    This automates the process of derivative creation.
    :return: string "kicked off or not"
    """
    result = chain(getSample.s(),read_source_update_derivative.s(outformat,filter,scale,crop))
    result.delay()
    return "automate kicked off"


def update_catalog(bag,paramstring,mmsid=None):


    db_client = app.backend.database.client
    collection = db_client.cybercom.catalog
    query = {"bag": bag}
    document = collection.find_one(query)
    if document == None:
        return False
    document_id = document['_id']
    myquery = {"_id":document_id}

    if mmsid == None:
        if "error" in document["application"]["islandora"].keys():
            document["application"]["islandora"]["error"].append("mmsid not found")
        else:
            document["application"]["islandora"].update({"error": ["mmsid not found"]})
        update_mmsid_error = {
            "$set":
                {
                    "application":
                        {
                            "islandora": document["application"]["islandora"]
                        }
                }
        }
        status = collection.update_one(myquery,update_mmsid_error)
        return status.raw_result['nModified'] != 0
    if paramstring not in document["derivatives"]:
        document["derivatives"][paramstring]={}


    #path = "/mnt/{0}/{1}/".format("derivative", bag)


    document["derivatives"][paramstring]["recipe"] = recipe_url.format(bag, paramstring, bag.lower())
    document["derivatives"][paramstring]["datetime"] = datetime.datetime.utcnow().isoformat()
    document["derivatives"][paramstring]["pages"] = [page['file'] for page in pages_list()]
    update_derivative_values = {
        "$set":
            {
                "derivatives":
                    {
                        paramstring: document["derivatives"][paramstring]
                    }
            }
    }
    general_update_status = collection.update_one(myquery,update_derivative_values)
    return general_update_status.raw_result['nModified'] !=0


@task
def read_source_update_derivative(bags,s3_source="source",s3_destination="derivative",outformat="JPEG",filter='ANTIALIAS',scale=None, crop=None):
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
        task_id = str(read_source_update_derivative.request.id)
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
                outpath = '/mnt/{0}/{1}/{2}/{3}.{4}'.format("derivative",bag,formatparams,file.split('/')[-1].split('.')[0].lower(),_formatextension(outformat))
                processimage(inpath=file,outpath=outpath,outformat=_formatextension(outformat))
        else:
            update_catalog(bag,formatparams,mmsid)
    return {"local_derivatives": "{0}/oulib_tasks/{1}".format(base_url, task_id), "s3_destination": s3_destination,
            "task_id": task_id,"bags":bags_with_mmsids,"format_params":formatparams}

@task
def process_recipe(derivative_args):

    """
        This function generates the recipe file and returns the json structure for each bag.

        params:
        derivative_args:The arguments returned by readSource_updateDerivative function.
    """

    #task_id= derivative_args.get('task_id')
    bags = derivative_args.get('bags') #bags = { "bagname1" : { "mmsid": value} , "bagName2":{"mmsid":value}, ..}
    formatparams = derivative_args.get('format_params')
    #formatparams="jpeg_040_antialias"
    #bags={"Abbati_1703":{"mmsid":9932140502042}}
    for bag_name,mmsid in bags.items():
        bag_derivative(bag_name,formatparams)
        recipe_file_creation(bag_name,mmsid,formatparams)
        update_catalog(bag_name,formatparams,mmsid["mmsid"])
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
