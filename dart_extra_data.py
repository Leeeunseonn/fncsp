import pandas as pd
import numpy as np
import os
import re
import json
import pprint

import pymysql
from elasticsearch import Elasticsearch
import itertools
from datetime import datetime
import elasticsearch_dsl



from total_funcs import ES_func
import asyncio
from itertools import chain
from time import time

import elasticsearch.helpers
from elasticsearch import helpers 
from tqdm import tqdm


# es_Function_parameter=Function_parameter()
es_func=ES_func()


# def get_es_conn():
#     es = None
#     try:
#         es = Elasticsearch(
#             host=es_Function_parameter.es_host,
#             port=es_Function_parameter.es_port,
#             http_auth=es_Function_parameter.es_http_auth,
#             timeout=es_Function_parameter.es_timeout,
#             max_retries=es_Function_parameter.es_max_retries,
#             retry_on_timeout=es_Function_parameter.es_retry_on_timeout,
#         )
#     except Exception as e:
#         print(e)
#     return es

##############################################

def source_count_query():
    return {
        "track_scores": True,
        "track_total_hits": True,
        "query": {           
            "bool":{
                "must":[
                    {"match":{"DataType":"nicednb_fnl"}}
                ]
            }
        }
    }

# def view_count_query():
#     return {
#         "track_scores": True,
#         "track_total_hits": True,
#         "query":{
#             "match_all":{}
#         }
#     }

##############################################




##############################################

def source_include_query():
    return {   
        "track_total_hits": True, 
        "_source":{
            "includes":["BusinessNum"]
        },
        "collapse": {"field": "BusinessNum"},
        "query": {           
            "bool":{
                "must":[
                    {"match":{"DataType":"nicednb_fnl"}}
                ]
            }
        }
    }


################################################


es_async = ES_func().connect_async_es()


async def source_data_biznum():
    count = 0
    query = {
        "size": 10000,
        "_source": ["BusinessNum"],
        "collapse": {"field": "BusinessNum"},
        "query": {           
            "bool":{
                "must":[
                    {"match":{"DataType":"nicednb_fnl"}}
                ]
            }
        },
        "sort": {"BusinessNum": "asc"},
    }
    data = await es_async.search(index="source_data", body=query)

    print(data)
    
    yield [i["_source"]["BusinessNum"] for i in data["hits"]["hits"]]
    
    while data["hits"]["hits"]:
        try:
            count += 1
            print(count)
            
            sort_key = data["hits"]["hits"][-1]["sort"]
            query["search_after"] = sort_key
            data = await es_async.search(index="source_data", body=query)
            yield [i["_source"]["BusinessNum"] for i in data["hits"]["hits"]]
        except:
            break


def get_query(biz_no):
    query = {
        "_source": ["BusinessNum"],
        "sort": {"SearchDate": "desc"},
        "size": 10000,
        "collapse": {"field": "BusinessNum"},
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"DataType": ["nicednb_enterprise"]}},
                    {"terms": {"BusinessNum": biz_no}},
                ]
            }
        },
    }
    return query





async def add():
    
    try:
        a=[]

        biz_num = source_data_biznum()
        
        async for i in biz_num:
            a.append(i)

        
        a = list(itertools.chain(*a))
        print(a)
        print(len(a))

    except Exception as ex:
        print(ex)




if __name__=="__main__":
    # asyncio.run(add())
    # from urllib.request import urlopen
    # from bs4 import BeautifulSoup

    # html = urlopen("https://opendart.fss.or.kr/disclosureinfo/fnltt/singlacnt/main.do")  
    # bsObject = BeautifulSoup(html, "html.parser") 

    # print(bsObject)
    count = es_func.get_numData_from_es("source_data",source_count_query())
    print("source_data",count)


    

    # 대략 10분~15분 소요
