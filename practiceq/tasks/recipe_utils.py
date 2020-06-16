import bagit
import logging
from collections import OrderedDict
import json
from json import loads,dumps
from .utils import *
import jinja2
from inspect import cleandoc
from uuid import uuid5, NAMESPACE_DNS
repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')

ou_derivative_bag_url = "https://bag.ou.edu/derivative"

def bag_derivative(bag_name,formatparams,update_manifest=True):
    """
        This methods create a bag for the derivative folder
        and updates the bag-info.txt generated
        args :
            bagName: str
            update_manifest : boolean
    """

    path = _get_path(bag_name,formatparams)
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

#TODO: change over here
def _get_path(bag_name,formatparams):
    return "/mnt/derivative/{0}/{1}".format(bag_name,formatparams)

def recipe_file_creation(bag_name,mmsid,formatparams,title=None):
    """
        This method creates the recipe.json file and updates it into the derivative folder of the bag
        args:
            bag_name: str - name of the bag
            mmsid: dictionary "mmsid":value
            formatparams :  str eg . jpeg_040_antialias
    """
    path = _get_path(bag_name,formatparams)
    try:
        bag = bagit.Bag(path)
        payload = bag.payload_entries()
        recipefile = "{0}/{1}.json".format(path,bag_name)
        recipe=make_recipe(bag_name,mmsid,payload,formatparams,title)
        logging.debug("Writing recipe to: {0}".format(recipefile))
        with open(recipefile,"w") as f:
            f.write(recipe.decode("UTF-8"))
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
    path = _get_path(bag_name,formatparams)
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
        page = json.loads(page_str)
        page['uuid'] = str(uuid5(repoUUID, "{0}/{1}".format(bag_name, file[0])))
        pages.append(page)
    return pages
