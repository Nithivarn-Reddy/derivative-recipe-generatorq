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

def _get_path(bag_name,formatparams):
    return "/mnt/derivative/{0}/{1}".format(bag_name,formatparams)

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
        page_str = tmplt.render(ou_derivative_bag_url=ou_derivative_bag_url, bagname=bag_name, idx=idx+1,
                                   formatparams=formatparams, file=file)
        page = json.loads(page_str)
        page['uuid'] = str(uuid5(repoUUID, "{0}/{1}".format(bag_name, file[0])))
        pages.append(page)
    return pages
