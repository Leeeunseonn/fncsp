"""
Moduel version :
elasticsearch                      7.15.2
elasticsearch-dsl                  7.4.0
mysql                              0.0.3
mysql-connector                    2.2.9
mysql-connector-python             8.0.27
mysqlclient                        2.1.0
urllib3                            1.26.7
"""
# Global moduel
import datetime
import re
import copy
import datetime
import os
import functools

# Elastic moduel
from elasticsearch_dsl import Search, A, Q
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Mysql modeul
import mysql.connector as mysql
import pymysql
import MySQLdb

MYSQL_MODUEL = pymysql

LOCAL_HOST = True


class Function_parameter:
    def __init__(self):
        if LOCAL_HOST:
            # 로컬에서 실행
            self.es_host = "61.78.63.51"
            self.sql_host = "61.78.63.52"
            self.sql_user = "root"
            self.sql_pw = "[1SycnsDev20220404!@#]"
        else:
            # 외부에서 실행
            self.es_host = "192.168.120.159"
            self.sql_host = "192.168.120.160"
            # self.sql_user = "fncsp"
            # self.sql_pw = "fncsp123!"
        self.es_port = 9200
        self.es_http_auth = ("sycns", "rltnfdusrnth")
        self.es_timeout = 100
        self.es_max_retries = 5
        self.es_retry_on_timeout = True
        self.es_scroll = "1m"
        self.es_scroll_size = 10000
        # self.sql_db = "nia"
        self.sql_connect_timeout = 36000


func_para = Function_parameter()

# Elasticsearch class
class ES_func:  # 동기로 처리하는 파일용
    def __init__(self):
        self.es_host = func_para.es_host
        self.es_port = func_para.es_port
        self.es_http_auth = func_para.es_http_auth
        self.es_timeout = func_para.es_timeout
        self.es_max_retries = func_para.es_max_retries
        self.es_retry_on_timeout = func_para.es_retry_on_timeout

        # 데이터 스크롤 options
        self.es_scroll = func_para.es_scroll
        self.es_scroll_size = func_para.es_scroll_size

    def connect_es(self):
        count = 0
        while True:
            try:
                if count > 5:
                    # print(code)
                    return code
                else:
                    # raise
                    es = Elasticsearch(
                        host=self.es_host,
                        port=self.es_port,
                        http_auth=self.es_http_auth,
                        timeout=self.es_timeout,
                        max_retries=self.es_max_retries,
                        retry_on_timeout=self.es_retry_on_timeout,
                    )
                    return es
            except Exception as e:
                code = e
                count += 1

    def es_search(self, index, size, query, es=None):
        if es:
            es = es
        else:
            es = self.connect_es()
        count = 0
        while True:
            try:
                if count > 5:
                    # print(code)
                    return code
                else:
                    if ("collapse" or "rescore") in query.keys():
                        data = es.search(index=index, size=size, body=query)
                    elif "aggs" in query.keys():
                        data = es.search(index=index, size=0, body=query)
                    else:
                        data = es.search(
                            index=index, scroll=self.es_scroll, size=size, body=query
                        )
                    return data
            except Exception as e:
                code = e
                count += 1

    def get_data_from_es(self, index, query, es=None):
        count = 0
        Data = []
        while True:
            try:
                if count > 5:
                    # print(code)
                    return None
                else:
                    if es:
                        es = es
                    else:
                        es = self.connect_es()

                    if es is not None:
                        # 한번에 가져올 데이터 수 (사이즈가 작을수록 빠르게 처리)
                        # 엘라스틱서치 호출
                        size = self.es_scroll_size
                        data = self.es_search(index, size, query)

                        if "aggs" in query.keys():
                            if data["aggregations"]["dedup"]["sum_other_doc_count"] > 0:
                                # 스크롤 시작
                                # idx = 0
                                sid = data.get("_scroll_id")
                                size = len(data["aggregations"]["dedup"]["buckets"])
                                while size > 0:
                                    Data += data["aggregations"]["dedup"]["buckets"]
                                    data = es.scroll(
                                        scroll_id=sid, scroll=self.es_scroll
                                    )
                                    sid = data["_scroll_id"]
                                    size = len(data["aggregations"]["dedup"]["buckets"])
                                es.clear_scroll(scroll_id=sid)
                            else:
                                sid = data.get("_scroll_id")
                                es.clear_scroll(scroll_id=sid)
                        elif "collapse" in query.keys():
                            return data["hits"]["hits"]
                        else:
                            if data["hits"]["total"]["value"] > 0:
                                # 스크롤 시작
                                # idx = 0
                                sid = data.get("_scroll_id")
                                size = len(data["hits"]["hits"])
                                # print(size)
                                while size > 0:
                                    Data += data["hits"]["hits"]
                                    data = es.scroll(
                                        scroll_id=sid, scroll=self.es_scroll
                                    )
                                    sid = data["_scroll_id"]
                                    size = len(data["hits"]["hits"])
                                    # print(sid,size)
                                es.clear_scroll(scroll_id=sid)
                            else:
                                sid = data.get("_scroll_id")
                                es.clear_scroll(scroll_id=sid)
                    return Data

            except Exception as e:
                code = e
                count += 1

    # 1개 데이터 호출
    def get_data1_from_es(self, index, query):
        result = None
        try:
            res = self.es_search(index=index, size=1, query=query)
            # res = es.search(index=index, body=query, size=1)
            if res["hits"]["total"]["value"] > 0:
                result = res["hits"]["hits"][0]
            # es.close()
        except Exception as e:
            print(e)
        return result

    # 데이터 개수 호출
    def get_numData_from_es(self, index, query):
        try:
            es = self.connect_es()
            if es is not None:
                res = self.es_search(index=index, size=1, query=query)
                # res = es.search(index=index, body=query, size=1)
                return res["hits"]["total"]["value"]
        except Exception as e:
            print(e)
            return None

    # 데이터 적재
    def save_data_to_es(self, index, data, id=None):
        try:
            es = self.connect_es()
            if es is not None:
                if id is not None:
                    result = es.index(index=index, id=id, body=data)
                else:
                    result = es.index(index=index, body=data)
                if result["_shards"]["successful"] > 0:
                    return "success", None
                else:
                    return "fail", result
        except Exception as e:
            return "fail", e

    # 이미 적재된 데이터인지 체크
    def check_already_saved(self, index, id):
        try:
            es = self.connect_es()
            if es is not None:
                result = es.get(index=index, id=id)
                # print(result)
                if result["found"]:
                    return False
                else:
                    return True
        except Exception as e:
            # print(e)
            return True

    # 엘라스틱서치 인덱스 새로고침
    def refresh_es(self, index):
        try:
            es = self.connect_es()
            if es is not None:
                es.indices.refresh(index=index)
        except Exception as e:
            print(e)

    # 엘라스틱서치 데이터 업데이트
    def update_data_from_es(self, index, id, update_data):
        try:
            es = self.connect_es()
            if es is not None:
                body = {"doc": update_data}
                response = es.update(index=index, id=id, body=body)
                print(response["result"])
                return response["result"]
        except Exception as e:
            print(e)
            return "fail"

    ##################### CODE MAPPING #####################
    # [기업의 산업분류코드, 10차산업분류코드(2자리), ecos분류(알파벳), istans(4자리) 리스트, 코드세부설명]
    # ex) ['47312', '47', 'G', ['2101', '2100'], '도매 및 소매업']
    # ex) [None, 'etc', 'etc', ['etc'], '']
    def get_indust_code(self, biz_no):
        es = self.connect_es()
        CompanyIndustCode, IndustCode, EcosCode, IstansCode, Describe = (
            None,
            "etc",
            "etc",
            [],
            "",
        )

        # 사업자번호로 기업의 산업분류코드 조회
        query = {
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"BusinessNum": biz_no}},
                        {"match": {"DataType": "nicednb_enterprise"}},
                    ]
                }
            },
        }
        nicednb_enterprise_data = self.get_data_from_es(
            index="source_data", query=query
        )
        if nicednb_enterprise_data:
            code = nicednb_enterprise_data[0]["_source"]["Data"]["indCd1"]
            if code is not None and code != "":
                CompanyIndustCode = code
                IndustCode = CompanyIndustCode[:2]

        # 기업의 산업분류코드 앞 2자리로 매핑코드 조회
        query = {
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {"bool": {"must": [{"match": {"Data.IndustCode": IndustCode}}]}},
        }
        code_mapping = self.get_data_from_es(index="indust_code", query=query)
        if code_mapping:
            for data in code_mapping:
                Describe = data["_source"]["Data"]["Describe"]
                EcosCode = data["_source"]["Data"]["EcosCode"]
                IstansCode.append(data["_source"]["Data"]["IstansCode"])
        if len(IstansCode) < 1:
            IstansCode = ["etc"]

        return [CompanyIndustCode, IndustCode, EcosCode, IstansCode, Describe]

    ##################### SOURCE_DATA #####################
    # kisti 특허 데이터 가져오기
    def get_kisti_patent_data(self, applicationNo):
        query = {
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"DataType": "kisti_patent"}},
                        {"match": {"Data.applicationNumber": applicationNo}},
                    ]
                }
            },
        }
        data = self.get_data1_from_es(index="source_data", query=query)
        if data:
            return data
        else:
            return None

    # google 특허 데이터 가져오기
    def get_google_patent_data(self, applicationNo):
        query = {
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"DataType": "google_patent"}},
                        {"match": {"Data.ApplicationNumber": applicationNo}},
                    ]
                }
            },
        }
        data = self.get_data1_from_es(index="source_data", query=query)
        if data:
            return data
        else:
            return None

    # kipris 패밀리특허 데이터(특허수, 국가수) 가져오기
    def get_kipris_family_data(self, applicationNo):
        applicationNo = applicationNo.replace("-", "")
        count = 0
        familyNum, countryNum = None, None
        es = self.connect_es()
        if es is not None:
            while True:
                if count > 5:
                    return None
                else:
                    try:
                        s = Search(using=es, index="source_data")
                        aggs_query = A(
                            "terms", field="Data.applicationCountryCode", size=s.count()
                        )
                        s.aggs.bucket("aggs_query", aggs_query)
                        s = s.query("match", **{"DataType": "kipris_family"})
                        s = s.query("match", **{"Data.KorNumber": applicationNo})
                        res = s.execute()
                        buckets = res.aggregations["aggs_query"].to_dict()["buckets"]
                        familyNum, countryNum = (
                            res.to_dict()["hits"]["total"]["value"],
                            len(buckets),
                        )
                        # es.close()
                        return familyNum, countryNum
                    except Exception as e:
                        code = e
                        count += 1

    # 재무제표 데이터를 파싱하여 5개년도 계정금액 반환
    # 반환값: {"연도": ["계정금액", "증감율"], ... }
    def find_nicednb_fnl_data(self, biz_no, target):
        flag, finance_data = "fail", dict()

        this_year = int(datetime.datetime.today().year)
        for Year in range(this_year - 5, this_year):
            Year = str(Year)
            nicednb_fnl_query = {
                "sort": [{"SearchDate": {"order": "desc"}}],
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"BusinessNum": biz_no}},
                            {"match": {"DataType": "nicednb_fnl"}},
                            {"match": {"Data.acctNm": re_type(target)}},
                            {"match": {"Data.stYear": Year}},
                        ]
                    }
                },
            }
            try:
                # 반환값: []
                nicednb_fnl_data = self.get_data_from_es(
                    index="source_data", query=nicednb_fnl_query
                )

                if nicednb_fnl_data:
                    flag = True
                    data = nicednb_fnl_data[0]["_source"]["Data"]
                    acctAmt, icdcRate = data["acctAmt"], data["icdcRate"]
                    if icdcRate is not None and icdcRate >= 9999.99:
                        icdcRate = None
                    finance_data[Year] = [acctAmt, icdcRate]
                else:
                    finance_data[Year] = [None, None]
            except:
                finance_data[Year] = [None, None]

        return finance_data, flag

    def get_ecos_data(self, IndustCode, AcctNm, EcosYear):
        result = None
        try:
            query = {
                "sort": [
                    {"SearchDate": {"order": "desc"}},
                    {"Data.AcctAmt": {"order": "desc"}},
                ],
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"Data.IndustCode": IndustCode}},
                            {"match": {"Data.AcctNm": AcctNm}},
                            {"match": {"Data.EcosYear": EcosYear}},
                            {"exists": {"field": "Data.AcctAmt"}},
                        ]
                    }
                },
            }
            data = self.get_data1_from_es(index="source_data", query=query)
            if data is not None:
                amount = data["_source"]["Data"]["AcctAmt"]
                unit = data["_source"]["Data"]["DataUnit"]
                nm = data["_source"]["Data"]["AcctNm"]
                if unit not in ["%", "회", "명"]:
                    try:
                        # 사업체수 구하기
                        query = {
                            "sort": [
                                {"SearchDate": {"order": "desc"}},
                                {"Data.AcctAmt": {"order": "desc"}},
                            ],
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"Data.IndustCode": IndustCode}},
                                        {"match": {"Data.AcctNm": "사업체수"}},
                                        {"match": {"Data.EcosYear": EcosYear}},
                                        {"exists": {"field": "Data.AcctAmt"}},
                                    ]
                                }
                            },
                        }
                        count_data = self.get_data1_from_es(
                            index="source_data", query=query
                        )
                        if count_data is not None:
                            if "1인당" not in nm:
                                count = count_data["_source"]["Data"]["AcctAmt"]
                                # 산업별 사업체수로 나눠서 평균값 구하기
                                if None not in [amount, count]:
                                    result = amount / count
                            if unit == "백만원":
                                # 백만원 > 천원 단위 조정
                                if result is not None:
                                    result = round(result * (10 ** 3), 4)
                    except Exception as e:
                        print(e)
                        pass
                else:
                    result = amount
        except Exception as e:
            print(e)
        return result

    # ISTANS 데이터 호출
    def get_istans_from_es(self, IstansGb, IndustCode, IstansYear, Country="국내"):
        result = None
        try:
            query = {
                "sort": [
                    {"SearchDate": {"order": "desc"}},
                    {"Data.IstansPrice": {"order": "desc"}},
                ],
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"Data.IstansGb": IstansGb}},
                            {"terms": {"Data.IndustCode": IndustCode}},
                            {"match": {"Data.IstansYear": IstansYear}},
                            {"match": {"Data.Country": Country}},
                            {"exists": {"field": "Data.IstansPrice"}},
                        ]
                    }
                },
            }
            data = self.get_data1_from_es(index="source_data", query=query)
            if data is not None:
                amount = data["_source"]["Data"]["IstansPrice"]
                unit = data["_source"]["Data"]["DataUnit"]
                nm = data["_source"]["Data"]["IstansNm"]
                if unit not in ["%", "백만달러", "건/십억원"]:
                    try:
                        # 사업체수 구하기기
                        query = {
                            "sort": [
                                {"SearchDate": {"order": "desc"}},
                                {"Data.IstansPrice": {"order": "desc"}},
                            ],
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"Data.IstansGb": "NB"}},
                                        {"terms": {"Data.IndustCode": IndustCode}},
                                        {"match": {"Data.IstansYear": IstansYear}},
                                        {"exists": {"field": "Data.IstansPrice"}},
                                    ]
                                }
                            },
                        }
                        count_data = self.get_data1_from_es(
                            index="source_data", query=query
                        )
                        if count_data is not None:
                            if "1인당" not in nm:
                                count = count_data["_source"]["Data"]["IstansPrice"]
                                # 산업별 사업체수로 나눠서 평균값 구하기
                                if None not in [amount, count]:
                                    result = amount / count
                            if unit == "백만원":
                                # 백만원 > 천원 단위 조정 (백만달러 > 천달러)
                                if result is not None:
                                    result = round(result * (10 ** 3), 4)
                    except Exception as e:
                        print(e)
                        pass
                else:
                    result = amount
        except Exception as e:
            print(e)
        return result

    ##################### ANALYSIS_DATA #####################
    # 모집단(산업군) 평균 데이터 가져오기
    def get_fa_data(self, AnalType, IndustCode, Year):
        query = {
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"AnalysisData.AnalType": AnalType}},
                        {"match": {"AnalysisData.IndustCode": IndustCode}},
                        {"match": {"AnalysisData.Year": str(Year)}},
                    ]
                }
            },
        }
        data = self.get_data1_from_es(index="analysis_data", query=query)
        if data:
            return data["_source"]
        else:
            return None

    ##################### SURVEY_DATA #####################
    def get_srv_data(self, biz_no):
        query = {
            "sort": [{"CreateDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"ViewID": "SRV"}},
                        {"match": {"BusinessNum": biz_no}},
                    ]
                }
            },
        }
        data = self.get_data1_from_es(index="survey_data", query=query)
        return data

    def bulk_api(self, data):

        """
        data_format = [{}]
        result_format = (n,[])
        """
        try:
            es = self.connect_es()
            result = helpers.bulk(es, data)
            if result[0] > 0:
                return result[0]  # 업데이트 개수
            else:
                return None
        except Exception as e:
            print(e)
            return self.bulk_api(data)


# Mysql class
class SQL_func:
    def __init__(self, sql_db=None, id=None, pw=None):
        self.my_host = func_para.sql_host
        self.my_connect_timeout = func_para.sql_connect_timeout
        if LOCAL_HOST:
            self.my_user = func_para.sql_user
            self.my_passwd = func_para.sql_pw
            self.my_database = sql_db
        else:
            self.my_user = id
            self.my_passwd = pw
            self.my_database = sql_db

    def connect_mysql(self):  # mysql 연결 함수 , 5회까지 접속 재시도
        count = 0
        while True:
            try:
                if count > 5:
                    print(code)
                    return None
                else:
                    conn = MYSQL_MODUEL.connect(
                        host=self.my_host,
                        user=self.my_user,
                        passwd=self.my_passwd,
                        database=self.my_database,
                        connect_timeout=self.my_connect_timeout,
                    )
                    return conn
            except Exception as e:
                code = e
                count += 1

    def sql_search(self, sql, dictionary=None):
        conn = self.connect_mysql()
        if sql:
            if MYSQL_MODUEL == mysql:
                cur = conn.cursor(dictionary=dictionary)
            else:
                cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            cur.close()
            conn.close()
            return result
        else:
            return None

    def get_mysql_col_name(self, table_name):  # mysql 컬럼 이름 출력하는 함수
        conn = self.connect_mysql()
        sql = f"SELECT * FROM {table_name} LIMIT 0"
        try:
            cur = conn.cursor()
            cur.execute(sql)

            result = [i[0] for i in cur.description]
            cur.close()
            conn.close()
            # result = [i for i in result if i != 'COMPANY_NAME' and i != 'CEO_NAME']
            return result
        except:
            return []

    def get_bizNo_mysql(self, DataType, sql_=None):  # mysql 에서 사업자 번호 가져오는 함수
        try:
            conn = self.connect_mysql()
            cur = conn.cursor()
            sql = (
                """
            SELECT BIZ_NO, COMPANY_NAME, CEO_NAME, """
                + DataType
                + """ FROM SOURCE_DATA_STATUS 
                ORDER BY """
                + DataType
                + """ ASC, BIZ_NO ASC"""
            )
            if sql_ is not None:
                sql = sql_
            cur.execute(sql)
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except Exception as e:
            return None

    def get_biz_no_list(
        self, DataType=None, Table="SOURCE_DATA_STATUS"
    ):  # get_bizNo_mysql 함수에서 받아온 리스트 전처리 하는 함수
        try:
            if DataType:
                sql = f"""SELECT BIZ_NO, {DataType} {Table} ORDER BY BIZ_NO ASC"""
                biz_no_list = self.get_bizNo_mysql(DataType="", sql_=sql)
                biz_no_list = [i for i in biz_no_list]
                return biz_no_list
            else:
                sql = f"""SELECT BIZ_NO FROM {Table} ORDER BY BIZ_NO ASC"""
                biz_no_list = self.get_bizNo_mysql(DataType="", sql_=sql)
                biz_no_list = [str(i[0]) for i in biz_no_list]
                return biz_no_list
        except Exception as e:
            return []

    def sql_query(self, sql_):  # mysql 에서 사업자 번호 가져오는 함수
        try:
            conn = self.connect_mysql()
            cur = conn.cursor()
            sql = sql_
            cur.execute(sql)
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except Exception as e:
            return None

    def update_searchDate_mysql(self, DataType, biz_no_lst: list):
        try:
            # conn = mysql.connector.connect(
            conn = MYSQL_MODUEL.connect(
                host=self.my_host,
                user=self.my_user,
                passwd=self.my_passwd,
                database=self.my_database,
                connect_timeout=self.my_connect_timeout,
            )
            cur = conn.cursor()

            for biz_no, search_date in biz_no_lst:
                sql = f"UPDATE SOURCE_DATA_STATUS \
                        SET {DataType} = '{search_date.replace('-','')}' \
                        WHERE BIZ_NO={biz_no}"
                cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
            return "Success"

        except Exception as e:
            print(e)
            # cur.close()
            # conn.close()
            return "False"

    def get_data_from_mysql(self, sql_=None):  # mysql 에서 사업자 번호 가져오는 함수
        try:
            conn = self.connect_mysql()
            cur = conn.cursor()
            sql = sql_
            if sql_ is not None:
                sql = sql_
            cur.execute(sql)
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
        except Exception as e:
            return None

    def df_to_sql(self, df, table_name, if_exists=None):
        from urllib.parse import quote
        from sqlalchemy import create_engine

        db_connection_str = f"mysql+pymysql://{self.my_user}:%s@{self.my_host}/{self.my_database}?charset=utf8"
        db_connection = create_engine(
            db_connection_str % quote(self.my_passwd), encoding="utf-8"
        )
        # db_connection = create_engine("mysql://root:"+"[1SycnsDev20220404!@#]"+"@61.78.63.52:3306/nia?charset=utf8", encoding='utf-8')
        # engine = create_engine("mysql://root:"+"rltnfdusrnth123!!"+"@112.175.39.175:3306/fncsp2?charset=utf8", encoding='utf-8')
        conn = db_connection.connect()
        if if_exists == None:
            df.to_sql(
                name=f"{table_name}", con=db_connection, if_exists="append", index=False
            )
        elif if_exists == "replace":
            df.to_sql(
                name=f"{table_name}",
                con=db_connection,
                if_exists=if_exists,
                index=False,
            )


class sam_function:
    def make_es_type(
        self, input_data, data_type
    ):  # 데이터를 elasticsearch 에 저장할 수 있는 형태로 parsing

        es_data = []

        for i in input_data:
            data = {
                "DataType": data_type,
                "SearchDate": datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                ),  # str(datetime.datetime.now()) 하면 가끔 이상한 값 나옴. 명시적으로 지정할것.
                "SearchID": "autoSystem",
                "Data": i,
            }
            es_data.append(copy.deepcopy(data))
        return es_data

    def make_es_type_need_date(
        self, input_data, data_type, date
    ):  # 데이터를 elasticsearch 에 저장할 수 있는 형태로 parsing

        es_data = []

        for i in input_data:
            data = {
                "DataType": data_type,
                "SearchDate": f"{date} {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f').split()[-1]}",  # str(datetime.datetime.now()) 하면 가끔 이상한 값 나옴. 명시적으로 지정할것.
                "SearchID": "autoSystem",
                "Data": i,
            }
            es_data.append(copy.deepcopy(data))
        return es_data

    def make_seq_number(self, input_data):  # 최종 데이터에 순번 매겨주기

        for idx, i in enumerate(input_data, 1):
            if list(i.keys()) == ["DataType", "SearchDate", "SearchID", "Data"]:
                i["Data"]["SEQ"] = idx
            elif list(i.keys()) == [
                "CD_CLSSC",
                "ITEM_CD",
                "DATA_CLLCT_DT",
                "CD_CLSSC_NM",
                "ITEM_CD_NM",
                "SCRN_SHOW_SEQ",
                "USE_YN",
            ]:
                i["SCRN_SHOW_SEQ"] = idx
            elif "DOC_SEQ" in list(i.keys()):
                i["DOC_SEQ"] = idx
            else:
                i["SEQ"] = idx

    def make_bulk_type(self, input_data, index):  # 데이터를 bulk로 저장할 수 있는 형태로 parsing

        bulk_data = []

        for i in input_data:
            data = {"_index": index, "_type": "_doc", "_source": i}
            bulk_data.append(copy.deepcopy(data))

        return bulk_data

    def create_folder(self, directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print(f"Error: 이미 생성된 폴더가 있습니다. - {directory}")

    def make_sam_file(self, result, DataType, date):
        def make_sam_string(result, date):
            result_line = ""
            for k in [i.values() for i in result]:
                lines = ""
                k = list(k)
                for z in k:
                    if z == None:
                        z = ""
                    if type(z) == list:
                        z = ",".join(z)
                    if type(z) == str:
                        z = (
                            z.replace("\n", " ")
                            .replace("\x0a", " ")
                            .replace("_x000D_", " ")
                            .replace("\x0A", " ")
                            .replace("\r", " ")
                        )
                    line = f"{z}\a"
                    lines += line
                end = f"{'I'}\a{date}\a{'N'}\a{''}\n"
                result_line += lines + end
            return result_line

        sam_file = make_sam_string(result, date)
        with open(f"./SAM/{DataType}.txt", "w", encoding="UTF-8") as f:
            f.write(sam_file)

        # make_backup
        directory = f"./SAM/backup/{date[4:6]}"
        self.create_folder(directory)
        with open(f"{directory}/{DataType}.txt", "w", encoding="UTF-8") as f:
            f.write(sam_file)

        with open(f"./SAM/{DataType}.txt", "r", encoding="UTF-8") as f:
            file_len = len(f.readlines())
        return file_len


##################### ETC FUNCTION#####################
##### timeout function moduel #####
from threading import Thread

##### Error log function moduel #####
import logging

# Timeout function
def timeout(seconds_before_timeout):
    """
    데코레이터 함수 - timeout function
    seconds_before_timeout = 원하는 시간(단위 : 초)
    ex) @timeout(30)
        def sample_function():
            ...
    위와 같이 사용시 함수의 실행시간이 30초를 초과할시 '_TIME_OUT_ERROR_' 라는 string 반환
    """

    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [
                Exception(
                    "function [%s] timeout [%s seconds] exceeded!"
                    % (func.__name__, seconds_before_timeout)
                )
            ]

            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e

            t = Thread(target=newFunc)
            t.name = "Time_check"
            t.daemon = True
            try:
                t.start()
                t.join(seconds_before_timeout)
            except Exception as e:
                print("error starting thread")
                raise e
            ret = res[0]
            if isinstance(ret, BaseException):
                return "_TIME_OUT_ERROR_"
            return ret

        return wrapper

    return deco


# Error log function
def catch_exception(exception=Exception, logger=logging.getLogger(__name__)):
    """
    미완성
    """

    def deco(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if "Error" in type(result).__name__:
                # os._exit('error')
                # sys.exit("exception_error")
                logging.basicConfig(
                    filename=".\error_log\error.log",
                    level=logging.DEBUG,
                    datefmt="%Y-%m-%d %H:%M:%S",
                    format="%(asctime)s %(module)s %(levelname)s: %(message)s",
                )
                logger.error(result, exc_info=True)
                # logger.error(f'{sys._getframe(2).f_code.co_name} - {sys._getframe(1).f_code.co_name} - {func.__name__} - {result}')
            else:
                return result

        return wrapper

    return deco


# 기타 부호 제거
def re_type(word):
    word = word.replace(" ", "")
    word = "".join(word.split("주식회사")).rstrip().lstrip()
    word = word.replace("㈜", "")
    word = re.sub(r"\([ ^)] * \)", "", word)
    return word


# dict(json)형의 val값을 None -> "" / 숫자 -> str 반환
def change_data_format(dict):
    # print(dict, end=" --------> ")
    try:
        for key, val in dict.items():
            if type(val) is not str:
                dict[key] = str(val)
            if val is None or "None" in str(val):
                dict[key] = ""
    except Exception as e:
        print(e)
    # print(dict)
    return dict


from decimal import Decimal


def nested_dict_change_value_type(d):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict):
                nested_dict_change_value_type(v)
            elif isinstance(v, list):
                nested_dict_change_value_type(v)
            else:
                if isinstance(v, float):
                    d[k] = round(v, 2)
                elif isinstance(v, Decimal):
                    v = float(v)
                    d[k] = round(v, 2)
                else:
                    pass
    elif isinstance(d, list):
        for i in d:
            for k, v in i.items():
                if isinstance(v, dict):
                    nested_dict_change_value_type(v)
                elif isinstance(v, list):
                    nested_dict_change_value_type(v)
                else:
                    if isinstance(v, float):
                        i[k] = round(v, 2)
                    elif isinstance(v, Decimal):
                        v = float(v)
                        i[k] = round(v, 2)
                    else:
                        pass
