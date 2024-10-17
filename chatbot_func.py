from elasticsearch import helpers,Elasticsearch
import csv
import json

# es_host='20.194.17.221'
# es_port=9200
# es_timeout = 36000
# es_max_retries = 3
# es_retry_on_timeout = True

# # 데이터 스크롤 options
# es_scroll = "60m"
# es_scroll_size = 10000
# es_scroll_timeout = "60m"

# 엘라스틱서치 접속정보
es_host = "61.78.63.51"
es_port = 9200
es_http_auth = ("sycns", "rltnfdusrnth")
es_timeout = 36000
es_max_retries = 3
es_retry_on_timeout = True

# 데이터 스크롤 options
es_scroll = "60m"
es_scroll_size = 10000
es_scroll_timeout = "60m"




index="kujoin_chatbot"

should_list = {
        "무엇", "어떤", "선택", "어떻게",
        "의미", "뭔", "뭣", "뭐", "뭔가요", "왜", "어디서",
        "가능", "어디","얼마나","나요","대해서","대한",
        "이란", "란"
    }

def get_es_conn():
    es = None
    try:
        es = Elasticsearch(
            host=es_host,
            port=es_port,
            http_auth=es_http_auth,
            timeout=es_timeout,
            max_retries=es_max_retries,
            retry_on_timeout=es_retry_on_timeout,
        )
        
    except Exception as e:
        pass
        # print(e)
    return es

def index_setting():
    
    es = get_es_conn()
    
    doc={
            "settings":{
                "index.number_of_shards": 1, 
                "index.number_of_replicas": 0, 
                "index":{
                    "analysis":{
                        "tokenizer": {
                            "nori_tokenizer_mixed": {
                                "type": "nori_tokenizer",
                                "decompound_mode": "mixed"
                            }
                        },
                        
                        "analyzer":{
                            "nori":{
                                "type":"custom",
                                "tokenizer":"nori_tokenizer_mixed",
                                "filter":["lowercase","my_pos_f"]
                            }
                        },
                        
                        "filter":{
                            "my_pos_f":{
                                "type":"nori_part_of_speech",
                                "stoptags":[
                                    "VA", "VX", "VCP", "VCN", "MAJ", "J", "XPN", "XSA", "XSN", "XSV", "SP", "SSC", "SSO", "SC", "SE", "UNA"
                                ]
                            }
                        }
                    }
                }
            },
            
            "mappings":{
                "dynamic": False,
                "properties":{
                    "No":{
                        "type": "integer"
                    },
                    "Question":{
                        "type": "text",
                        "analyzer": "nori",
                        "fields": {
                            "keyword": {
                            "type": "keyword"
                            }
                        }
                    },
                    "Answer":{
                        "type": "text",
                        "analyzer": "nori",
                        "fields": {
                            "keyword": {
                            "type": "keyword"
                            }
                        }
                    },
                    "Url":{
                        "type": "text"
                    }
                }
            }
        }
    
    if es.indices.exists(index=index):
        pass
    else:
        es.indices.create(index=index, body=doc)
    es.close()
    
def csv_conn():
    
    es = get_es_conn()
    with open('230110_chatbot_data.csv',"r",encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index=index)
    es.close()
    
def deleteIndex():
    
    es = get_es_conn()
    if es.indices.exists(index=index):
        es.indices.delete(index=index, ignore=[400, 404])
    else:
        pass
    es.close()


def searchData(input):
    
    try:
        result={"flag":None,"Data":None}
        flag, data = "fail", []
        
        # 검색할 단어 추출
        must, should = [], []
        
        es = get_es_conn()
        
        if es:
            # 토큰 어널라이저
            res_words = es.indices.analyze(index=index,
                body={
                    "analyzer": "nori",
                    "text": [input]
                }
            )
            
            # print(res_words)
            
            if res_words["tokens"]:
                for w in res_words["tokens"]:
                    if w["token"] in should_list:
                        should.append(w["token"])
                    else:
                        must.append(w["token"])
        else:
            should = input.split(" ")
        
        must= ' '.join(s for s in must)
        should= ' '.join(s for s in should)
        
        # refresh
        es.indices.refresh(index=index)
        
        # print("must:",must)
        # print("should:",should)
        
        # 검색
        query = {
            "size":5,
            "sort":{"_score":"desc","Question.keyword":"asc"},
            "query": {
                "bool": {
                    "must": [{"match": {"Question": must}}],
                    "should": [{"match": {"Question": should}}]
                }
            }
        }
        
        # 쿼리 실행
        res = es.search(index=index, body=query)
        
        # print(res)
        
        # # 갯수
        # print(res['hits']['total']['value'])
        
        # 검색결과파싱
        if res["hits"]["hits"]:
            
            flag= "success"
            data = [i['_source'] for i in res["hits"]["hits"]]
            
            for i in data:
                if i["Answer"]:
                    i["Answer"]=i["Answer"].strip()
                    
        else:
            flag= "noData"
            
        # result dictionary
        result['flag']=flag
        result['Data']=data
            
        # 엘라스틱서치 연결 종료
        es.close()
        
        #dic to json
        # result_json=json.dumps(result,ensure_ascii=False,)
        # return result_json,
    
        
    except Exception as error:
        # print(error)
        
        # result dictionary
        result['flag']='fail'
        result['Data']=[]
        
    return str(result),

def main():
    
    # 컴파일할때마다 기존 인덱스가 삭제되고 새로 만들어서 csv파일안의 내용이 들어갑니다.
    # 서치만 하고 싶으시면 searchData()만 실행시켜주세요.
    
    # deleteIndex()
    # index_setting()
    # csv_conn()
    
    input="열 길이"
    print(searchData(input))

if __name__ =="__main__":
    main()
    
