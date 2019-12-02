import json
from celery.task import task
from celery import chain,group
import requests

def jprint(obj):
    str = json.dumps(obj,sort_keys=True,indent=4)
    print(str)
bagList=[]
def getBags(results):
    """
      :param results: results list from each json object
      :return: list of bags
    """
    global bagList
    for idx,dic in enumerate(results):
        bagList.append(dic.get('bag'))





@task
def initialize():
    response = requests.get('https://cc.lib.ou.edu/api/catalog/data/catalog/digital_objects/?query={"filter":{"department":"DigiLab","project":{"$ne":"private"},"locations.s3.exists":{"$eq":true},"derivatives.jpeg_040_antialias.recipe":{"$exists":false}}}&format=json')
    print(response.status_code)
    jobj = response.json()
    getBags(jobj.get('results'))
    str = jobj.get('next')
    return str
#jprint(response.json())



#print(type(jobj))

#print(jobj.keys())


#pageList=[]

#results = jobj.get('results')
#print(type(results))
#print(len(results))


#print("page-1\n"+bagList)
#code for looping through next

#print(len(bagList))

#i = 2

@task
def getAllBags(str):
    while str:
        res = requests.get(str+'&format=json')
        jobj = res.json()
        results = jobj.get('results')
        getBags(results)
       # print("No of bags collected by page {0} {1} \n".format(i,len(bagList)))
        #i+=1
        str=jobj.get('next')
    return bagList

    #pageList.append(res.status_code)
#print(len(pageList))
#print("\ncount of bags{}".format(len(bagList)))
dervdic =[]
@task
def gen_derivative(bags):
    global dervdic
    for i,bag in enumerate(bags):
        dervdic.append({'derivative':"derivative/"+bag,'status':True})
        status_check_gen_recipe(dervdic[i],i)
    return dervdic
#print(gen_derivative(bagList))
@task
def automate():
    """
    This automates the process of derivative creation.
    :return: string "kicked off or not"
    """
    result = chain(initialize.s(),getAllBags.s(),gen_derivative.s())
    result.delay()
    return "automate kicked off"
"""
str = initialize()
li=getAllBags(str)
derv=gen_derivative(li)

print(len(derv))
"""
@task
def updateCatalog(bag):
    if bag['status']:
        print("yes")
@task
def gen_recipe(bag,i):
    if bag['status']:
        dervdic[i].update({'recipe':"recipe/"+bag['derivative'].split('/')[1]})

@task
def status_check_gen_recipe(bag,i):
    grp = group(updateCatalog.s(bag),gen_recipe.s(bag,i))
    grp.delay()
    return "kicked off updatecatalog and gen_recipe"

