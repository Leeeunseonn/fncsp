# 반환값에 배열(list)이 있는 경우 return 뒤에 쉼표(,) 붙여야함

############################### Library Import #######################################################
import datetime
from dateutil.relativedelta import relativedelta

# pip install elasticsearch==7.13.3
from elasticsearch import Elasticsearch, helpers

# pip install mysql-connector-python==8.0.27
import mysql.connector

# pip install scipy
import scipy.stats as stats
import bisect
from operator import itemgetter

import copy

############################### Host Setting #######################################################
# 로컬/개발/운영서버 별 자동으로 host(ip) 변경
import socket

curr_host_ip = socket.gethostbyname((socket.gethostname()))
# 운영서버 > 리눅스:127.0.0.1, 윈도우:192.168.120.162
if curr_host_ip in ["127.0.0.1", "127.0.1.1", "192.168.120.162"]:
    # 뷰 데이터 호출용
    es_host1 = "192.168.120.159"
    # 기타 데이터 호출용
    es_host2 = "192.168.120.166"
    # mysql
    my_host = "192.168.120.160"
# 로컬/개발서버
else:
    # 뷰 데이터 호출용
    es_host1 = "61.78.63.51"
    # 기타 데이터 호출용
    es_host2 = "61.78.63.51"
    # mysql
    my_host = "61.78.63.52"

############################### Options #######################################################
_source_data = "source_data"
_analysis_data = "analysis_data"
_view_data = "view_data"
_view_data_egi = "view_data_egi"
_view_comment = "view_comment"
_survey_data = "survey_data"
_brief_data = "brief_data"

# 엘라스틱서치 접속정보
es_port = 9200
es_http_auth = ('sycns', 'rltnfdusrnth')
es_timeout = 36000
es_max_retries = 3
es_retry_on_timeout = True

# 데이터 스크롤 options
es_scroll = '60m'
es_scroll_size = 10000
es_scroll_timeout = '60m'

# mysql 접속정보
my_user = "fncsp"
my_passwd = "fncsp123!"
my_database = "fncsp2"
my_connect_timeout = 36000

# 기본값 : 오늘 년월
today = datetime.datetime.today()
default_yyyymmdd = today.strftime("%Y%m%d")
default_yyyymm = today.strftime("%Y%m")
default_yyyy = today.strftime("%Y")

this_year = int(datetime.datetime.today().year)


# 데이터 호출
# input: 사업자번호(bizNo), 화면ID(viewID), 년월(yyyymm)
# output: (플래그:"success"/"noData"/"fail", 데이터)

############################### 데이터베이스 #######################################################
# 엘라스틱서치 연결
def get_es_conn(host=es_host2):
    es = None
    try:
        es = Elasticsearch(
            host=host,
            port=es_port,
            http_auth=es_http_auth,
            timeout=es_timeout,
            max_retries=es_max_retries,
            retry_on_timeout=es_retry_on_timeout
        )
    except Exception as e:
        pass
        # print(e)
    return es
# 엘라스틱서치 인덱스 새로고침
def refresh_es(index):
    try:
        es = get_es_conn()
        es.indices.refresh(index=index)
        es.close()
    except Exception as e:
        pass
        # print(e)
# mysql 연결
def get_mysql_conn(database="fncsp2"):
    conn = None
    try:
        conn = mysql.connector.connect(
            host=my_host,
            user=my_user,
            passwd=my_passwd,
            database=database,
            connect_timeout=my_connect_timeout
        )
    except Exception as e:
        # print(e)
        pass
    return conn
# [기업의 산업분류코드, 10차산업분류코드(2자리), ecos분류(알파벳), istans(4자리) 리스트, 코드세부설명]
# ex) ['47312', '47', 'G', ['2101', '2100'], '도매 및 소매업']
# ex) [None, 'etc', 'etc', ['etc'], '']
def get_indust_code(bizNo):
    code = "ETC"
    # 사업자번호로 기업의 산업분류코드 조회
    query = {
        "size": 1,
        "query": {"bool": {"must": [
            {"match": {"BusinessNum": bizNo}},
            {"exists": {"field": "Data.indCd1"}}
        ]
        }}}
    es = get_es_conn()
    nicednb_enterprise_data = es.search(index="source_data", body=query)
    if nicednb_enterprise_data and nicednb_enterprise_data["hits"]["hits"]:
        code = nicednb_enterprise_data["hits"]["hits"][0]["_source"]["Data"]["indCd1"]
        if code:
            code = code[:2]
        else:
            code = "ETC"
    return code


############################### 형변환 #######################################################
# 데이터형 모두 str 변환 (None -> "")
def change_none_to_str(Data):
    import ast, json, unicodedata
    try:
        data = str(Data)
        while "None" in data:
            data = data.replace("None", """\"\"""")
        data = ast.literal_eval(data)
        data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
        data = unicodedata.normalize("NFC", data)  # 한글 자음모음 합치기
        Data = json.loads(data)
    except:
        pass
    return Data
# 데이터형 모두 None 변환 ("" -> None)
def change_str_to_none(Data):
    import ast, json, unicodedata
    try:
        data = str(Data)
        while """\"\"""" in data:
            data = data.replace("""\"\"""", "None")
        while """\'\'""" in data:
            data = data.replace("""\'\'""", "None")
        data = ast.literal_eval(data)
        data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
        data = unicodedata.normalize("NFC", data)  # 한글 자음모음 합치기
        Data = json.loads(data)
    except Exception as e:
        # print(e)
        pass
    return Data
# str형을 dict/json 변환
def change_str_to_json(string):
    try:
        # 작은따옴표 -> 큰따옴표 변환해야 json으로 인식
        while "\'" in string:
            string = string.replace("\'", "\"")
        import json
        string = json.loads(string)
    except:
        pass
    return string


############################### 설문조사 #######################################################
# 설문조사데이터 호출
def get_srv_from_es(bizNo, yyyy):
    viewID = "SRV"
    _id = viewID + "_" + str(bizNo) + "_" + str(yyyy)
    flag, Data = "fail", ""
    try:
        es = get_es_conn()
        res = es.get(index=_survey_data, id=_id)
        es.close()
        if res["found"]:
            Data = res["_source"]
            # 데이터형 모두 str 변환 (None -> "")
            Data = change_none_to_str(Data)
            flag = "success"
        else:
            flag = "noData"
    except Exception as e:
        # print(e)
        flag = "fail"
    return str({"flag": flag, "Data": Data}),
# EmpNoDict = {'2020': [1,None,3,None,5,6], '2021': [6개], '2022':[6개]}
def update_srv_rnd(bizNo, EmpNoDict):
    ViewID, flag = "RND", "fail"

    ### 기본 템플릿 ##################################################
    ItTechPw, RndAcc, RndIncr = [], [], []
    PatRnd, UtilRnd, DesignRnd, MarkRnd = [], [], [], []
    for y in range(this_year - 5, this_year):
        ItTechPw.append({
            "Year": y,
            "ItTechPwVal": None
        })
        RndAcc.append({
            "Year": y,
            "RndAccEtc": None,
            "RndAccMke": None,
            "RndAccMti": None
        })
        RndIncr.append({
            "Year": y,
            "RndCost": None
        })
        PatRnd.append({
            "Year": y,
            "PatRndVal": None,
            "PatNum": None,
            "RndCost": None
        })
        UtilRnd.append({
            "Year": y,
            "UtilRndVal": None,
            "UtilNum": None,
            "RndCost": None
        })
        DesignRnd.append({
            "Year": y,
            "DesignRndVal": None,
            "DesignNum": None,
            "RndCost": None
        })
        MarkRnd.append({
            "Year": y,
            "MarkRndVal": None,
            "MarkNum": None,
            "RndCost": None
        })
    ViewData = {
        "FuTechIdx": None,
        "FuTechSc": None,
        "AvrFuTechSc": None,
        "FuTech": {
            "TechPwIdx": None,
            "TechPwSc": None,
            "AvrTechPwSc": None,
            "TechPw": {
                "ItTechPwIdx": None,
                "ItTechPwSc": None,
                "ItTechPw": ItTechPw,
                "LabIdx": None,
                "LabSc": None,
                "LabYn": None
            },
            "FthIndIdx": None,
            "FthIndSc": None,
            "AvrFthIndSc": None,
            "FthInd": {
                "FthImp": None,
                "FthDom": None,
                "FthCmp": None
            },
            "RndAccIdx": None,
            "RndAccSc": None,
            "AvrRndAccSc": None,
            "RndAcc": RndAcc,

            "RndIncrIdx": None,
            "RndIncrSc": None,
            "AvrRndIncrSc": None,
            "RndIncr": RndIncr
        },
        "IpCapIdx": None,
        "IpCapSc": None,
        "AvrIpCapSc": None,
        "IpCap": {
            "PatRndIdx": None,
            "PatRndSc": None,
            "AvrPatRndSc": None,
            "PatRnd": PatRnd,

            "UtilRndIdx": None,
            "UtilRndSc": None,
            "AvrUtilRndSc": None,
            "UtilRnd": UtilRnd,

            "DesignRndIdx": None,
            "DesignRndSc": None,
            "AvrDesignRndSc": None,
            "DesignRnd": DesignRnd,

            "MarkRndIdx": None,
            "MarkRndSc": None,
            "AvrMarkRndSc": None,
            "MarkRnd": MarkRnd
        }
    }

    ### 가장 최근 데이터 호출 ######################################
    res = get_view_from_es(bizNo, ViewID)[0]
    res = change_str_to_json(res)
    # 데이터가 있으면
    if res["flag"] == "success":
        ViewData = res["Data"]["ViewData"]

    ### 설문조사 데이터 적용 ###########################################
    import copy
    ItTechPw = copy.deepcopy(ViewData["FuTech"]["TechPw"]["ItTechPw"][:3])

    # 전체인력수, 연구인력수, 경영지원, 전략기획, 고객지원 서비스 인력, 영업 및 마케팅 인력
    # EmpNoDict = {'2020': [1,None,3,None,5,6], '2021': [6개], '2022':[6개]}
    for year, srv_data_ in EmpNoDict.items():
        ### IT 기술 인력 비중 ###################################################
        try:
            ItTechPwVal = round((srv_data_[1] / srv_data_[0]) * 100, 2)
        except:
            ItTechPwVal = None
        ItTechPw.append({
            "Year": year,
            "ItTechPwVal": ItTechPwVal,
        })
    ViewData["FuTech"]["TechPw"]["ItTechPw"] = ItTechPw

    ### 평가점수 재계산 #############################################################
    # IT 기술 인력 비중
    try:
        tmp = [float(item["ItTechPwVal"]) for item in ItTechPw if item["ItTechPwVal"]]
        if tmp:
            sc = sum(tmp) / len(tmp)
            if sc >= 15:
                ItTechPwIdx = "상"
                ItTechPwSc = 20
            elif sc >= 7.5:
                ItTechPwIdx = "중"
                ItTechPwSc = 14
            else:
                ItTechPwIdx = "하"
                ItTechPwSc = 7
            ViewData["FuTech"]["TechPw"]["ItTechPwIdx"] = str(ItTechPwIdx)
            ViewData["FuTech"]["TechPw"]["ItTechPwSc"] = str(ItTechPwSc)
    except:
        pass
    # 기술인력비중
    try:
        tmp = [
            float(i) for i in
            [ViewData["FuTech"]["TechPw"]["ItTechPwSc"], ViewData["FuTech"]["TechPw"]["LabSc"]]
            if i
        ]
        if tmp:
            TechPwSc = sum(tmp)
            if TechPwSc >= 20:
                TechPwIdx = "상"
            elif TechPwSc >= 15:
                TechPwIdx = "중"
            else:
                TechPwIdx = "하"
            ViewData["FuTech"]["TechPwIdx"] = str(TechPwIdx)
            ViewData["FuTech"]["TechPwSc"] = str(TechPwSc)
    except:
        pass
    # 미래기술 대응 역량 (4차산업 대응 역량)
    try:
        tmp = [
            float(i) for i in
            [
                ViewData["FuTech"]["TechPwSc"],
                ViewData["FuTech"]["FthIndSc"],
                ViewData["FuTech"]["RndAccSc"],
                ViewData["FuTech"]["RndIncrSc"]
            ]
            if i
        ]
        if tmp:
            FuTechSc = sum(tmp)
            if FuTechSc >= 75:
                FuTechIdx = "상"
            elif FuTechSc >= 50:
                FuTechIdx = "중"
            else:
                FuTechIdx = "하"
            ViewData["FuTechIdx"] = str(FuTechIdx)
            ViewData["FuTechSc"] = str(FuTechSc)
    except:
        pass

    ### 데이터 저장 ##################################################3
    RND_data = {
        "BusinessNum": bizNo,
        "ViewID": ViewID,
        "SearchDate": str(datetime.datetime.now()).replace("T", ""),
        "CreateID": "autoSystem",
        "SurveyYn": "y",
        "ViewData": ViewData
    }
    import json, unicodedata
    RND_data = unicodedata.normalize("NFKD", json.dumps(RND_data, ensure_ascii=False))  # 유니코드 normalize
    RND_data = unicodedata.normalize("NFC", RND_data)  # 한글 자음모음 합치기
    RND_data = json.loads(RND_data)

    # 데이터형 모두 None 변환 (""->None)
    RND_data = change_str_to_none(RND_data)

    # 데이터 확인용
    # if False:
    #     print(json.dumps(RND_data, indent=2, ensure_ascii=False))
    #     flag = "success"

    # 엘라스틱서치에 데이터 저장
    if True:
        es_ = get_es_conn()
        # 올해 연도
        default_yyyymm = datetime.datetime.today().strftime("%Y%m")
        _id = ViewID + "_" + str(bizNo) + "_" + default_yyyymm
        # 데이터 저장
        res = es_.index(index=_view_data, body=RND_data, id=_id)
        if res["result"] in ["created", "updated"]:
            flag = "success"
        es_.close()

    return flag
def update_srv_edc001(bizNo, EmpNoDict):
    ViewID, flag = "EDC001", "fail"

    ### 기본 템플릿 ##################################################
    # 작년 포함 5개년
    Growth, Profit, Stability, Activity, Mobility = [], [], [], [], []
    for year in range(this_year - 5, this_year):
        Growth.append({
            "Year": str(year),
            "GrTotAsst": None,
            "GrSales": None
        })
        Profit.append({
            "Year": str(year),
            "CurrRto": None,
            "TotBrr": None,
            "StckAsst": None,
            "DebtRto": None
        })
        Stability.append({
            "Year": str(year),
            "IntCovRto": None,
            "FundRto": None,
            "TotShEqRto": None,
            "NetSalesRto": None
        })
        Activity.append({
            "Year": str(year),
            "AsstTurnRto": None,
            "RecTurnRto": None
        })
        Mobility.append({
            "Year": str(year),
            "CurrRto": None,
            "AcidRto": None
        })
    # 올해 포함 3개년
    SalesWorker, WorkerIncr = [], []
    for year in range(this_year - 2, this_year + 1):
        SalesWorker.append({
            "Year": str(year),
            "PerSales": None,
            "Sales": None,
            "HumNo": None
        })
        WorkerIncr.append({
            "Year": str(year),
            "TotHumNo": None,
            "TotHumNoGr": None
        })
    ViewData = {
        "BizFinIdx": None,
        "BizFinSc": None,
        "BizFin": {
            "GrowthIdx": None,
            "GrowthSc": None,
            "AvrGrowthSc": None,
            "Growth": Growth,
            "ProfitIdx": None,
            "ProfitSc": None,
            "AvrProfitSc": None,
            "Profit": Profit,
            "StabilityIdx": None,
            "StabilitySc": None,
            "AvrStabilitySc": None,
            "Stability": Stability,
            "ActivityIdx": None,
            "ActivitySc": None,
            "AvrActivitySc": None,
            "Activity": Activity,
            "MobilityIdx": None,
            "MobilitySc": None,
            "AvrMobilitySc": None,
            "Mobility": Mobility
        },
        "HumRsrIdx": None,
        "HumRsrSc": None,
        "HumRsr": {
            "MngStrRsrIdx": None,
            "MngStrRsrSc": None,
            "AvrMngStrRsrSc": None,
            "MngStrRsr": {
                "TotHumNo": None,
                "TotMngNo": None,
                "TotStrNo": None
            },
            "SalesWorkerIdx": None,
            "SalesWorkerSc": None,
            "AvrSalesWorkerSc": None,
            "SalesWorker": SalesWorker,
            "WorkerIncrIdx": None,
            "WorkerIncrSc": None,
            "AvrWorkerIncrSc": None,
            "WorkerIncr": WorkerIncr
        }
    }

    ### 가장 최근 데이터 호출 ######################################
    res = get_view_from_es(bizNo, ViewID)[0]
    res = change_str_to_json(res)
    # 데이터가 있으면
    if res["flag"] == "success":
        ViewData = res["Data"]["ViewData"]

    ### 설문조사 데이터 적용 ###########################################
    import copy
    # 전체인력수, 연구인력수, 경영지원, 전략기획, 고객지원 서비스 인력, 영업 및 마케팅 인력
    # EmpNoDict = {'2020': [1,None,3,None,5,6], '2021': [6개], '2022':[6개]}
    ### 경영지원/전략기획 인력 수준 ###################################################
    MngStrRsr = copy.deepcopy(ViewData["HumRsr"]["MngStrRsr"])
    tmp = [EmpNoDict[str(this_year)][0], EmpNoDict[str(this_year)][2], EmpNoDict[str(this_year)][3]]
    if not MngStrRsr["TotHumNo"]:
        ViewData["HumRsr"]["MngStrRsr"]["TotHumNo"] = tmp[0]
    if not MngStrRsr["TotMngNo"]:
        ViewData["HumRsr"]["MngStrRsr"]["TotMngNo"] = tmp[1]
    if not MngStrRsr["TotStrNo"]:
        ViewData["HumRsr"]["MngStrRsr"]["TotStrNo"] = tmp[2]
    # 평가점수/등급 용 점수
    MngStrRsrSc_sc = None
    try:
        tmp = [int(i) if i else 0 for i in tmp]
        MngStrRsrSc_sc = round( ((tmp[1]+tmp[2]) / tmp[0]) * 100, 2)
    except:
        pass

    SalesWorker, WorkerIncr = list(), list()
    SalesList = [i["Sales"] for i in copy.deepcopy(ViewData["HumRsr"]["SalesWorker"])]
    TotHumNoList = [i[0] for i in list(EmpNoDict.values())]
    idx = 0
    for year, srv_data_ in EmpNoDict.items():
        HumNo = TotHumNoList[idx]
        Sales = SalesList[idx]
        ### 종사자 1인당 매출액 수준
        try:
            # 단위: 천원 -> 억
            PerSales = round((Sales / (10 ** 5)) / HumNo, 2)
        except:
            PerSales = None
        SalesWorker.append({
            "Year": year,
            "PerSales": PerSales,
            "Sales": Sales,
            "HumNo": HumNo
        })
        ### 기업 종사자 비율 증가 수준
        try:
            TotHumNoGr = round((TotHumNoList[idx]/TotHumNoList[idx-1])*100-100, 2)
        except:
            TotHumNoGr = None
        WorkerIncr.append({
            "Year": year,
            "TotHumNo": HumNo,
            "TotHumNoGr": TotHumNoGr
        })
        idx += 1
    ViewData["HumRsr"]["SalesWorker"] = SalesWorker
    ViewData["HumRsr"]["WorkerIncr"] = WorkerIncr

    ### 평가점수 재계산 #############################################################
    # 경영지원/전략기획 인력 수준 평가
    try:
        if MngStrRsrSc_sc >= 11:
            MngStrRsrIdx = "상"
            MngStrRsrSc = 20
        elif MngStrRsrSc_sc >= 6.5:
            MngStrRsrIdx = "중"
            MngStrRsrSc = 14
        else:
            MngStrRsrIdx = "하"
            MngStrRsrSc = 7
        ViewData["HumRsr"]["MngStrRsrIdx"] = str(MngStrRsrIdx)
        ViewData["HumRsr"]["MngStrRsrSc"] = str(MngStrRsrSc)
    except:
        pass
    # 종사자 1인당 매출액 수준 평가
    try:
        tmp = [float(i["PerSales"]) for i in SalesWorker if i["PerSales"]]
        if tmp:
            tmp = sum(tmp)/len(tmp)
            if tmp >= 1.8:
                SalesWorkerIdx = "상"
                SalesWorkerSc = 25
            elif tmp >= 0.6:
                SalesWorkerIdx = "중"
                SalesWorkerSc = 16
            else:
                SalesWorkerIdx = "하"
                SalesWorkerSc = 8
            ViewData["HumRsr"]["SalesWorkerIdx"] = str(SalesWorkerIdx)
            ViewData["HumRsr"]["SalesWorkerSc"] = str(SalesWorkerSc)
    except:
        pass
    # 기업 종사자 비율 증가 수준
    try:
        WorkerIncr_Avg = [i["TotHumNo"] for i in WorkerIncr if i["TotHumNo"]]
        if len(WorkerIncr_Avg) > 1:
            tmp = []
            for idx in range(len(WorkerIncr_Avg) - 1):
                try:
                    tmp.append((WorkerIncr_Avg[idx + 1] / WorkerIncr_Avg[idx]) * 100 - 100)
                except:
                    pass
            if tmp:
                sc = round(sum(tmp) / len(tmp), 2)
                if sc >= 4:
                    WorkerIncrIdx, WorkerIncrSc = "상", 25
                elif sc >= 2:
                    WorkerIncrIdx, WorkerIncrSc = "중", 16
                else:
                    WorkerIncrIdx, WorkerIncrSc = "하", 8
                ViewData["HumRsr"]["WorkerIncrIdx"] = WorkerIncrIdx
                ViewData["HumRsr"]["WorkerIncrSc"] = WorkerIncrSc
    except:
        pass
    # 연구개발 역량 평가
    try:
        sc = [float(i) for i in [MngStrRsrSc, SalesWorkerSc, WorkerIncrSc] if i is not None]
        if sc:
            HumRsrSc = sum(sc)
            if HumRsrSc >= 80:
                HumRsrIdx = "상"
            elif HumRsrSc >= 50:
                HumRsrIdx = "중"
            else:
                HumRsrIdx = "하"
            ViewData["HumRsrIdx"] = HumRsrIdx
            ViewData["HumRsrSc"] = HumRsrSc
    except:
        pass

    ### 데이터 저장 ##################################################3
    EDC001_data = {
        "BusinessNum": bizNo,
        "ViewID": ViewID,
        "SearchDate": str(datetime.datetime.now()).replace("T", ""),
        "CreateID": "autoSystem",
        "SurveyYn": "y",
        "ViewData": ViewData
    }
    import json, unicodedata
    EDC001_data = unicodedata.normalize("NFKD", json.dumps(EDC001_data, ensure_ascii=False))  # 유니코드 normalize
    EDC001_data = unicodedata.normalize("NFC", EDC001_data)  # 한글 자음모음 합치기
    EDC001_data = json.loads(EDC001_data)

    # 데이터형 모두 None 변환 (""->None)
    EDC001_data = change_str_to_none(EDC001_data)

    # 데이터 확인용
    # if False:
    #     print(json.dumps(RND_data, indent=2, ensure_ascii=False))
    #     flag = "success"

    es_ = get_es_conn()
    # 엘라스틱서치에 데이터 저장
    if True:
        # 올해 연도
        default_yyyymm = datetime.datetime.today().strftime("%Y%m")
        _id = ViewID + "_" + str(bizNo) + "_" + default_yyyymm
        # 데이터 저장
        res = es_.index(index=_view_data, body=EDC001_data, id=_id)
        if res["result"] in ["created", "updated"]:
            flag = "success"

    ##########################################################################################################
    ##########################################################################################################
    ### 유형3.경영·재무 전략수립(EMF) > 경영·재무 역량 기초진단(EMF001)
    ViewID, flag = "EMF001", "fail"

    ### 기본 템플릿 ##################################################
    # 작년 포함 5개년
    BlncSh, InComnteSh = [], []
    Growth, Profit, Stability, Activity, Mobility = [], [], [], [], []
    for year in range(this_year - 5, this_year):
        BlncSh.append({
            "Year": str(year),
            "AssetAmt": None,
            "ShEqAmt": None,
            "DebtAmt": None
        })
        InComnteSh.append({
            "Year": str(year),
            "SalesAmt": None,
            "OprIcAmt": None,
            "CurrInc": None,
        })

        Growth.append({
            "Year": str(year),
            "GrTotAsst": None,
            "GrSales": None
        })
        Profit.append({
            "Year": str(year),
            "CurrRto": None,
            "TotBrr": None,
            "StckAsst": None,
            "DebtRto": None
        })
        Stability.append({
            "Year": str(year),
            "IntCovRto": None,
            "FundRto": None,
            "TotShEqRto": None,
            "NetSalesRto": None
        })
        Activity.append({
            "Year": str(year),
            "AsstTurnRto": None,
            "RecTurnRto": None
        })
        Mobility.append({
            "Year": str(year),
            "CurrRto": None,
            "AcidRto": None
        })
    # 올해 포함 3개년
    SalesWorker, WorkerIncr = [], []
    for year in range(this_year - 2, this_year + 1):
        SalesWorker.append({
            "Year": str(year),
            "PerSales": None,
            "Sales": None,
            "HumNo": None
        })
        WorkerIncr.append({
            "Year": str(year),
            "TotHumNo": None,
            "TotHumNoGr": None
        })
    ViewData = {
        "SheetStatus": {
            "BlncSh": BlncSh,
            "InComnteSh": InComnteSh,
            "CriGrd": None,
        },
        "FinCnslt": {
            "Growth": Growth,
            "Profit": Profit,
            "Stability": Stability,
            "Activity": Activity,
            "Mobility": Mobility,
            "Vol_IntCovRto": None,
            "Vol_FundRto": None,
            "Vol_AsstTurnRto": None,
            "Vol_RecTurnRto": None,
            "CAGR_CurrRto": None,
            "CAGR_TotBrr": None,
            "CAGR_StckAsst": None,
            "CAGR_DebtRto": None,
            "CAGR_IntCovRto": None,
            "CAGR_FundRto": None,
            "CAGR_TotShEqRto": None,
            "CAGR_NetSalesRto": None,
            "CAGR_AsstTurnRto": None,
            "CAGR_RecTurnRto": None,
            "CAGR_AcidRto": None
        },
        "MngCnslt": {
            "MngStrRsrIdx": None,
            "MngStrRsrSc": None,
            "AvrMngStrRsrSc": None,
            "MngStrRsr": {
                "TotHumNo": None,
                "TotMngNo": None,
                "TotStrNo": None
            },
            "SalesWorkerIdx": None,
            "SalesWorkerSc": None,
            "AvrSalesWorkerSc": None,
            "SalesWorker": SalesWorker,
            "WorkerIncrIdx": None,
            "WorkerIncrSc": None,
            "AvrWorkerIncrSc": None,
            "WorkerIncr": WorkerIncr
        }
    }

    ### 가장 최근 데이터 호출 ######################################
    res = get_view_from_es(bizNo, ViewID)[0]
    res = change_str_to_json(res)
    # 데이터가 있으면
    if res["flag"] == "success":
        ViewData = res["Data"]["ViewData"]
    ViewData["MngCnslt"] = EDC001_data["ViewData"]["HumRsr"]

    ### 데이터 저장 ##################################################3
    EMF001_data = {
        "BusinessNum": bizNo,
        "ViewID": ViewID,
        "SearchDate": str(datetime.datetime.now()).replace("T", ""),
        "CreateID": "autoSystem",
        "SurveyYn": "y",
        "ViewData": ViewData
    }
    import json, unicodedata
    EMF001_data = unicodedata.normalize("NFKD", json.dumps(EMF001_data, ensure_ascii=False))  # 유니코드 normalize
    EMF001_data = unicodedata.normalize("NFC", EMF001_data)  # 한글 자음모음 합치기
    EMF001_data = json.loads(EMF001_data)

    # 데이터형 모두 None 변환 (""->None)
    EMF001_data = change_str_to_none(EMF001_data)

    # 데이터 확인용
    # if False:
    #     print(json.dumps(EMF001_data, indent=2, ensure_ascii=False))
    #     flag = "success"

    # 엘라스틱서치에 데이터 저장
    if True:
        # 올해 연도
        default_yyyymm = datetime.datetime.today().strftime("%Y%m")
        _id = ViewID + "_" + str(bizNo) + "_" + default_yyyymm
        # 데이터 저장
        res = es_.index(index=_view_data, body=EMF001_data, id=_id)
        if res["result"] in ["created", "updated"]:
            flag = "success"
    es_.close()
    return flag
def update_srv_edc002(bizNo, EmpNoDict):
    ViewID, flag = "EDC002", "fail"

    ### 기본 템플릿 ##################################################
    ViewData = {
        "MrIdx": None,
        "MrSc": None,
        "Mr": {
            "CustSuppIdx": None,
            "CustSuppSc": None,
            "AvrCustSuppSc": None,
            "CustSupp": {
                "CustSuppNo": None,
                "TotHumNo": None
            },

            "SaleMarkHumIdx": None,
            "SaleMarkHumSc": None,
            "AvrSaleMarkHumSc": None,
            "SaleMarkHum": {
                "SaleMarkHumNo": None,
                "TotHumNo": None,
            },

            "MrRtoIdx": None,
            "MrRtoSc": None,
            "AvrMrRtoSc": None,
            "MrRto": [{
                "Year": None,
                "TotInv": None,
                "SaleMarkInv": None,
                "SaleMarkInvDom": None,
                "SaleMarkInvGlob": None
            }],

            "MarkNetIdx": None,
            "MarkNetSc": None,
            "AvrMarkNetSc": None,
            "MarkNet": {
                "MnManage": None,
                "MnEx": None,
                "MnCoop": None,
                "MnAs": None,
                "MnConsult": None
            },
        },
        "BmCapIdx": None,
        "BmCapSc": None,
        "BmCap": {
            "EmpPerspIdx": None,
            "EmpPerspSc": None,
            "AvrEmpPerspSc": None,
            "EmpPersp": {
                "EmpTot": None,
                "EmpNew": None,
                "EmpSkilled": None
            },
            "EcoSentIdx": None,
            "EcoSentSc": None,
            "AvrEcoSentSc": None,
            "EcoSent": {
                "Condition": None,
                "SalesPr": None,
                "Shipment": None,
                "Export": None,
                "Inventory": None,
                "Equip": None,
                "Hire": None,
                "Income": None,
                "Finance": None
            },
            "SalesIncIdx": None,
            "SalesIncSc": None,
            "AvrSalesIncSc": None,
            "SalesInc": [{
                "Year": None,
                "Sales": None,
                "IncRto": None
            }],
            "ComntmerceIdx": None,
            "ComntmerceSc": None,
            "AvrComntmerceSc": None,
            "Comntmerce": [{
                "Year": None,
                "PatentNo": None,
                "LicenseNo": None,
                "CertificateNo": None
            }]
        }
    }

    ### 가장 최근 데이터 호출 ######################################
    res = get_view_from_es(bizNo, ViewID)[0]
    res = change_str_to_json(res)
    # 데이터가 있으면
    if res["flag"] == "success":
        ViewData = res["Data"]["ViewData"]

    ### 설문조사 데이터 적용 ###########################################
    import copy
    # 전체인력수, 연구인력수, 경영지원, 전략기획, 고객지원 서비스 인력, 영업 및 마케팅 인력
    # EmpNoDict = {'2020': [1,None,3,None,5,6], '2021': [6개], '2022':[6개]}
    ### 고객응대 서비스 고도화 수준 ###################################################
    tmp = [int(i) if i else 0 for i in EmpNoDict[str(this_year)]]
    if tmp:
        CustSuppNo = tmp[4]
        TotHumNo_ = max(tmp[0], tmp[1]+tmp[2]+tmp[3]+tmp[5]) - CustSuppNo
        ViewData["Mr"]["CustSupp"]["CustSuppNo"] = CustSuppNo
        ViewData["Mr"]["CustSupp"]["TotHumNo"] = TotHumNo_
    ### 영업·마케팅 인력 비중 ###################################################
    tmp = [int(i) if i else 0 for i in EmpNoDict[str(this_year)]]
    if tmp:
        SaleMarkHumNo = tmp[5]
        TotHumNo = max(tmp[0], sum(tmp[1:5])) - SaleMarkHumNo
        ViewData["Mr"]["SaleMarkHum"]["SaleMarkHumNo"] = SaleMarkHumNo
        ViewData["Mr"]["SaleMarkHum"]["TotHumNo"] = TotHumNo

    ### 평가점수 재계산 #############################################################
    ### 고객응대 서비스 고도화 수준 평가
    try:
        tmp = [
            ViewData["Mr"]["CustSupp"]["CustSuppNo"],
            ViewData["Mr"]["CustSupp"]["TotHumNo"]
        ]
        tmp = [float(i) if i else 0 for i in tmp]
        try:
            sc = round((float(tmp[0]) / float(tmp[1])) * 100, 2)
            if sc >= 2:
                CustSuppIdx, CustSuppSc = "상", 30
            elif sc >= 1:
                CustSuppIdx, CustSuppSc = "중", 20
            else:
                CustSuppIdx, CustSuppSc = "하", 10
            ViewData["Mr"]["CustSuppIdx"] = str(CustSuppIdx)
            ViewData["Mr"]["CustSuppSc"] = str(CustSuppSc)
        except:
            pass
    except:
        pass
    ### 영업·마케팅 인력 비중 평가
    try:
        tmp = [
            ViewData["Mr"]["SaleMarkHum"]["SaleMarkHumNo"],
            ViewData["Mr"]["SaleMarkHum"]["TotHumNo"]
        ]
        tmp = [float(i) if i else 0 for i in tmp]
        try:
            sc = round((float(tmp[0]) / float(tmp[1])) * 100, 2)
            if sc >= 12:
                SaleMarkHumIdx, SaleMarkHumSc = "상", 35
            elif sc >= 6:
                SaleMarkHumIdx, SaleMarkHumSc = "중", 25
            else:
                SaleMarkHumIdx, SaleMarkHumSc = "하", 15
            ViewData["Mr"]["SaleMarkHumIdx"] = str(SaleMarkHumIdx)
            ViewData["Mr"]["SaleMarkHumSc"] = str(SaleMarkHumSc)
        except:
            pass
    except:
        pass
    ### 마케팅 역량 평가
    try:
        tmp = [
            ViewData["Mr"]["CustSuppSc"],
            ViewData["Mr"]["SaleMarkHumSc"],
            ViewData["Mr"]["MrRtoSc"]
        ]
        MrSc = sum([float(i) for i in tmp if i])
        try:
            if MrSc >= 70:
                MrIdx = "상"
            elif MrSc >= 40:
                MrIdx = "중"
            else:
                MrIdx = "하"
            ViewData["MrSc"] = str(MrSc)
            ViewData["MrIdx"] = str(MrIdx)
        except:
            pass
    except:
        pass

    ### 데이터 저장 ##################################################3
    EDC002_data = {
        "BusinessNum": bizNo,
        "ViewID": ViewID,
        "SearchDate": str(datetime.datetime.now()).replace("T", ""),
        "CreateID": "autoSystem",
        "SurveyYn": "y",
        "ViewData": ViewData
    }
    import json, unicodedata
    EDC002_data = unicodedata.normalize("NFKD", json.dumps(EDC002_data, ensure_ascii=False))  # 유니코드 normalize
    EDC002_data = unicodedata.normalize("NFC", EDC002_data)  # 한글 자음모음 합치기
    EDC002_data = json.loads(EDC002_data)

    # 데이터형 모두 None 변환 (""->None)
    EDC002_data = change_str_to_none(EDC002_data)

    # 데이터 확인용
    # if False:
    #     print(json.dumps(RND_data, indent=2, ensure_ascii=False))
    #     flag = "success"

    # 엘라스틱서치에 데이터 저장
    if True:
        es_ = get_es_conn()
        # 올해 연도
        default_yyyymm = datetime.datetime.today().strftime("%Y%m")
        _id = ViewID + "_" + str(bizNo) + "_" + default_yyyymm
        # 데이터 저장
        res = es_.index(index=_view_data, body=EDC002_data, id=_id)
        if res["result"] in ["created", "updated"]:
            flag = "success"
        es_.close()

    return flag
def update_srv_edc003(bizNo):
    ViewID, flag = "EDC003", "fail"

    ### 기본 템플릿 ##################################################
    ViewData = {
        "RndCnslt": {
            "FuTechSc": None,
            "AvrFuTechSc": None,
            "FuTech": {
                "TechPwSc": None,
                "FthIndSc": None,
                "RndAccSc": None,
                "RndIncrSc": None
            },
            "IpCapSc": None,
            "AvrIpCapSc": None,
            "IpCap": {
                "PatRndSc": None,
                "UtilRndSc": None,
                "DesignRndSc": None,
                "MarkRndSc": None
            }
        },
        "MhCnslt": {
            "BizFinSc": None,
            "AvrBizFinSc": None,
            "BizFin": {
                "GrowthSc": None,
                "ProfitSc": None,
                "StabilitySc": "",
                "ActivitySc": None,
                "MobilitySc": None
            },
            "HumRsrSc": None,
            "AvrHumRsrSc": None,
            "HumRsr": {
                "MngStrRsrSc": None,
                "SalesWorkerSc": None,
                "WorkerIncrSc": None
            }
        },
        "BizCnslt": {
            "MrSc": None,
            "AvrMrSc": None,
            "Mr": {
                "CustSuppSc": None,
                "SaleMarkHumSc": None,
                "MrRtoSc": None,
                "MarkNetSc": None
            },
            "BmCapSc": None,
            "AvrBmCapSc": None,
            "BmCap": {
                "EmpPerspSc": None,
                "EcoSentSc": None,
                "SalesIncSc": None,
                "ComntmerceSc": None
            }
        },
        "Rslt": {
            "AnalCmpSc": {
                "FuTechSc": None,
                "IpCapSc": None,
                "BizFin": None,
                "HumRsr": None,
                "MrSc": None,
                "BmCap": None
            },
            "AltSc": {
                "FuTechSc": None,
                "IpCapSc": None,
                "BizFin": None,
                "HumRsr": None,
                "MrSc": None,
                "BmCap": None
            },
            "RecConsult1": None,
            "RecConsult1Desc": None,
            "RecConsult2": None,
            "RecConsult2Desc": None,
            "Sim5Rec1Num": None,
            "Sim5Rec2Num": None,
            "SimCmpSc": [],
            "AnalCmpLow3": [],
        }
    }

    ### 가장 최근 데이터 호출 ######################################
    res = get_view_from_es(bizNo, ViewID)[0]
    res = change_str_to_json(res)
    # 데이터가 있으면
    if res["flag"] == "success":
        ViewData = res["Data"]["ViewData"]

    ### 평가점수 재계산 #############################################################
    # R&D 역량 진단
    rnd = get_view_from_es(bizNo, "RND")[0]
    rnd = change_str_to_json(rnd)
    # 데이터가 있으면
    if rnd["flag"] == "success":
        rnd_ViewData = rnd["Data"]["ViewData"]
        ViewData["RndCnslt"]["FuTechSc"] = rnd_ViewData["FuTechSc"]
        ViewData["RndCnslt"]["FuTech"]["TechPwSc"] = rnd_ViewData["FuTech"]["TechPwSc"]
        ViewData["RndCnslt"]["FuTech"]["FthIndSc"] = rnd_ViewData["FuTech"]["FthIndSc"]
        ViewData["RndCnslt"]["FuTech"]["RndAccSc"] = rnd_ViewData["FuTech"]["RndAccSc"]
        ViewData["RndCnslt"]["FuTech"]["RndIncrSc"] = rnd_ViewData["FuTech"]["RndIncrSc"]

    # 경영·인적역량 진단
    edc001 = get_view_from_es(bizNo, "EDC001")[0]
    edc001 = change_str_to_json(edc001)
    # 데이터가 있으면
    if edc001["flag"] == "success":
        edc001_ViewData = edc001["Data"]["ViewData"]
        ViewData["MhCnslt"]["HumRsrSc"] = edc001_ViewData["HumRsrSc"]
        ViewData["MhCnslt"]["HumRsr"]["MngStrRsrSc"] = edc001_ViewData["HumRsr"]["MngStrRsrSc"]
        ViewData["MhCnslt"]["HumRsr"]["SalesWorkerSc"] = edc001_ViewData["HumRsr"]["SalesWorkerSc"]
        ViewData["MhCnslt"]["HumRsr"]["WorkerIncrSc"] = edc001_ViewData["HumRsr"]["WorkerIncrSc"]

    # 사업화역량 진단
    edc002 = get_view_from_es(bizNo, "EDC002")[0]
    edc002 = change_str_to_json(edc002)
    # 데이터가 있으면
    if edc002["flag"] == "success":
        edc002_ViewData = edc002["Data"]["ViewData"]
        ViewData["BizCnslt"]["MrSc"] = edc002_ViewData["MrSc"]
        ViewData["BizCnslt"]["Mr"]["MngStrRsrSc"] = edc002_ViewData["Mr"]["CustSuppSc"]
        ViewData["BizCnslt"]["Mr"]["SalesWorkerSc"] = edc002_ViewData["Mr"]["SaleMarkHumSc"]
        ViewData["BizCnslt"]["Mr"]["WorkerIncrSc"] = edc002_ViewData["Mr"]["MrRtoSc"]
        ViewData["BizCnslt"]["Mr"]["WorkerIncrSc"] = edc002_ViewData["Mr"]["MarkNetSc"]

    ### 데이터 저장 ##################################################3
    EDC003_data = {
        "BusinessNum": bizNo,
        "ViewID": ViewID,
        "SearchDate": str(datetime.datetime.now()).replace("T", ""),
        "CreateID": "autoSystem",
        "SurveyYn": "y",
        "ViewData": ViewData
    }
    import json, unicodedata
    EDC003_data = unicodedata.normalize("NFKD", json.dumps(EDC003_data, ensure_ascii=False))  # 유니코드 normalize
    EDC003_data = unicodedata.normalize("NFC", EDC003_data)  # 한글 자음모음 합치기
    EDC003_data = json.loads(EDC003_data)

    # 데이터형 모두 None 변환 (""->None)
    EDC003_data = change_str_to_none(EDC003_data)

    # 데이터 확인용
    # if False:
    #     print(json.dumps(EDC003_data, indent=2, ensure_ascii=False))
    #     flag = "success"

    # 엘라스틱서치에 데이터 저장
    if True:
        es_ = get_es_conn()
        # 올해 연도
        default_yyyymm = datetime.datetime.today().strftime("%Y%m")
        _id = ViewID + "_" + str(bizNo) + "_" + default_yyyymm
        # 데이터 저장
        res = es_.index(index=_view_data, body=EDC003_data, id=_id)
        if res["result"] in ["created", "updated"]:
            flag = "success"
        es_.close()

    return flag
# 설문조사데이터 저장/업데이트
# {"flag": "success": "created"/"updated",  "fail"}
# 가장최근년도-2, 가장최근년도-1, 가장최근년도
# EmpNo: '[3,3,2,3,3,3,  3,3,2,3,3,3,  3,3,2,3,3,3]'
def save_srv_to_es(bizNo, EmpNo, yyyy=default_yyyy):
    ViewID, flag = "SRV", "fail"
    srv_dict = dict()

    import copy
    EmpNo_ = copy.deepcopy(EmpNo)
    yyyy_ = int(copy.deepcopy(yyyy)) - 3

    es = get_es_conn()
    # 6개 항목씩 3개 년도 설문조사 데이터 저장
    try:
        for idx in range(0, len(EmpNo_), 6):
            yyyy_ = yyyy_ + 1
            _id = ViewID + "_" + str(bizNo) + "_" + str(yyyy_)
            empno = [
                int(i)
                if i is not None
                else None
                for i in EmpNo_[idx:idx + 6]
            ]
            srv_dict.update({str(yyyy_): empno})
            now = str(datetime.datetime.now()).replace("T", "")
            # 새로 저장하는 데이터라면
            SearchDate = now
            UpdateDate = None
            try:
                res = es.get(index=_survey_data, id=_id)
                # 이미 저장된 데이터가 있다면
                if res["found"]:
                    SearchDate = res["_source"]["SearchDate"]
                    if not SearchDate:
                        SearchDate = now
                    UpdateDate = now
            except:
                pass
            # 설문조사 데이터 저장
            Data = {
                "BusinessNum": bizNo,
                "ViewID": ViewID,
                "SearchDate": SearchDate,
                "UpdateDate": UpdateDate,
                "CriteriaDate": str(yyyy_),
                "CreateID": "autoSystem",
                "ViewData": {
                    "EmpNo": {
                        "TotEmpNo": empno[0],
                        "RndEmpNo": empno[1],
                        "AdmEmpNo": empno[2],
                        "StrEmpNo": empno[3],
                        "AssEmpNo": empno[4],
                        "MkEmpNo": empno[5]
                    }
                }
            }
            # 데이터형 모두 None 변환 (""->None)
            Data = change_str_to_none(Data)
            res = es.index(index=_survey_data, body=Data, id=_id)
            if res["result"] in ["created", "updated"]:
                flag = "success"
    except Exception as e:
        # flag = e
        # print(e)
        flag = "fail"
        pass

    # 설문조사 데이터 -> 재계산 -> 뷰 업데이트
    try:
        # R&D 역량 진단(RND)
        flag = update_srv_rnd(bizNo, srv_dict)

        # 유형1.기업진단컨설팅(EDC) > 경영·인적 역량 진단(EDC001)
        # 유형3.경영·재무 전략수립(EMF) > 경영·재무 역량 기초진단(EMF001)
        flag = update_srv_edc001(bizNo, srv_dict)

        # 유형1.기업진단컨설팅(EDC) > 사업화 역량 진단(EDC002)
        flag = update_srv_edc002(bizNo, srv_dict)

        # 유형1.기업진단컨설팅(EDC) > 기업진단 컨설팅 종합(EDC003)
        flag = update_srv_edc003(bizNo)
    except Exception as e:
        # print(e)
        # flag = e
        flag = "fail"
        pass
    es.close()
    return str({"flag": flag})


############################### 컨설팅보고서 #######################################################
# 컨설팅보고서
def get_view_from_es(bizNo, viewID, yyyymm=default_yyyymm):
    flag, Data = "fail", None
    if not yyyymm:
        yyyymm = default_yyyymm
    end = str(datetime.date(year=int(yyyymm[:4]), month=int(yyyymm[4:]), day=1) + relativedelta(months=1))
    es = get_es_conn(host=es_host1)
    try:
        query = {
            "size": 1,
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {"bool": {"must": [
                {"match": {"BusinessNum": bizNo}},
                {"match": {"ViewID": viewID}}
            ]
                # ,
                # "filter": {"range": {
                #     "SearchDate": {
                #         "lte": end,
                #         "format": "yyyy-MM-dd"
                #     }}}
            }}
        }
        refresh_es(index=_view_data)
        res = es.search(index=_view_data, body=query)
        if res["hits"]["total"]["value"] > 0:
            Data = res["hits"]["hits"][0]["_source"]
            
            # 5번 컨설팅 보고서 > 시장 및 수익구조 분석 (단위 천원>백만원)
            try:
                if viewID == "ETV":
                    Data["ViewData"]["DomSize"] = [
                        {"Year": d["Year"], "MkSize": int(float(d["MkSize"])/(10**3))}
                        for d in Data["ViewData"]["DomSize"]
                    ]
            except:
                pass
            
            # 데이터형 모두 str 변환 (None -> "")
            Data = change_none_to_str(Data)
            flag = "success"
        else:
            flag = "noData"
    except Exception as e:
        # print(e)
        flag = "fail"
    es.close()
    return str({"flag": flag, "Data": Data}),
# 컨설팅보고서 유형4.기술경쟁력분석(ETC003) > 사업화 포트폴리오 분석
def update_etc003(MkCmptePh, bizNo, yyyymm=default_yyyymm):
    flag = "fail"

    if not yyyymm:
        yyyymm = default_yyyymm
    end = str(datetime.date(year=int(yyyymm[:4]), month=int(yyyymm[4:]), day=1) + relativedelta(months=1))

    es = get_es_conn(host=es_host1)
    try:
        query = {
            "size": 1,
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {"bool": {"must": [
                {"match": {"BusinessNum": bizNo}},
                {"match": {"ViewID": "ETC003"}}
            ],
                "filter": {"range": {
                    "SearchDate": {
                        # "gte": start,
                        "lte": end,
                        "format": "yyyy-MM-dd"
                    }}
                }}}
        }
        refresh_es(index=_view_data)
        res = es.search(index=_view_data, body=query)
        if res["hits"]["total"]["value"] > 0:
            Data = res["hits"]["hits"][0]
            id = Data["_id"]
            result = es.update(
                index=_view_data,
                id=id,
                body={
                    "doc": {
                        "SearchDate": str(datetime.datetime.now()).replace("T", ""),
                        "ViewData": {"Cnslt": {"MkCmptePh": MkCmptePh}}}
                }
            )
            if result["result"] in ["created", "updated"]:
                flag = "success"
    except Exception as e:
        # print(e)
        pass
    es.close()
    return str({"flag": flag})
# 컨설팅보고서 유형5.기술가치평가(ETV) > 기술사업화 위험 프리미엄 산출 설문조사
# type = "MkShare"/"TechRisk"
# data = MkShare[5개], TechRisk[10개]
def update_etv(bizNo, type, data, yyyymm=default_yyyymm):
    flag, viewID, body = "fail", "ETV", None
    index = _view_data
    
    # 최근 데이터 호출
    query = {
        "size": 1,
        "sort": [{"SearchDate": {"order": "desc"}}],
        "query": {"bool": {"must": [
            {"match": {"BusinessNum": bizNo}},
            {"match": {"ViewID": viewID}}
        ]
        }}
    }
    es = get_es_conn(host=es_host1)
    res = es.search(index=index, body=query)
    if res["hits"]["total"]["value"] > 0:
        _id = res["hits"]["hits"][0]["_id"]
        try:
            SearchDate = str(datetime.datetime.now()).replace("T", "")
            # 시장점유율 업데이트
            if type == "MkShare":
                body = {
                    "doc": {
                        "SearchDate": SearchDate,
                        "ViewData": {"MkShare": data}}
                }
            elif type == "TechRisk":
                body = {
                    "doc": {
                        "SearchDate": SearchDate,
                        "ViewData": {"TechRisk": data}}
                }
            if body and _id:
                es = get_es_conn(host=es_host1)
                result = es.update(
                    index=index,
                    id=_id,
                    body=body
                )
                es.close()
                if result["result"] in ["created", "updated"]:
                    flag = "success"
        except Exception as e:
            # print(e)
            pass

    return str({"flag": flag})


############################### 코멘트 #######################################################
# 컨설턴트 코멘트 호출
def get_comment_from_es(bizNo, yyyymm=default_yyyymm, empNo=None):
    flag, Data = "fail", None
    if not yyyymm:
        yyyymm = default_yyyymm
    end = str(datetime.date(year=int(yyyymm[:4]), month=int(yyyymm[4:]), day=1) + relativedelta(months=1))
    try:
        es = get_es_conn()
        query = {
            "size": 1,
            "sort": [{"CreateDate": {"order": "desc"}}],
            "query": {"bool": {"must": [
                {"match": {"BusinessNum": bizNo}},
                {"match": {"ViewID": "CMT"}},
                {"match": {"EmpNo": str(empNo)}}
            ],
                "filter": {"range": {
                    "CriteriaDate": {
                        "lte": end,
                        "format": "yyyy-MM-dd"
                    }}
                }}}
        }
        # print(query)
        refresh_es(index=_view_comment)
        res = es.search(index=_view_comment, body=query)
        es.close()
        if res["hits"]["total"]["value"] > 0:
            Data = res["hits"]["hits"][0]["_source"]
            # 데이터형 모두 str 변환 (None -> "")
            Data = change_none_to_str(Data)
            flag = "success"
        else:
            flag = "noData"
    except Exception as e:
        # print(e)
        pass
    return str({"flag": flag, "Data": Data}),


# {"flag": "created"/"updated"/"fail"}
def save_comment_to_es(bizNo, ViewData, yyyymm=default_yyyymm, empNo=None):
    flag, viewID = "fail", "CMT"
    if not yyyymm:
        yyyymm = default_yyyymm
    _id = viewID + str(empNo) + str(bizNo) + str(yyyymm)
    try:
        es = get_es_conn()
        data = {
            "EmpNo": str(empNo),
            "BusinessNum": str(bizNo),
            "ViewID": viewID,
            "CreateDate": str(datetime.datetime.now()).replace("T", ""),
            "CriteriaDate": str(yyyymm),
            "CreateID": "autoSystem",
            "ViewData": ViewData
        }
        res = es.index(index=_view_comment, body=data, id=_id)
        es.close()
        if res["result"] in ["created", "updated"]:
            flag = "success"
    except Exception as e:
        # print(e)
        pass

    return str({"flag": flag})


############################### 기업성장지수 #######################################################
# 리스트형 중에서 데이터가 부재한 경우 빈 리스트 [] 로 대체
def check_empty_data_egi001(ViewData):
    for field in ["DetailInformationDissemination", "DetailCustomerInterest", "DetailMediaExposureLevel", "GenRpDist"]:
        tmp = [
            i for i in
            [list(d.values())[1:] for d in ViewData[field]]
        ]
        tmp = [i for i in sum(tmp, []) if i]
        if not tmp:
            ViewData[field] = list()

    # 일반 평판 비교(GenRpCmp)
    field = "WordCloud"
    tmp = [
        i for i in
        [list(d.values())[1:] for d in ViewData["GenRpCmp"][field]]
    ]
    tmp = [i for i in sum(tmp, []) if i]
    if not tmp:
        ViewData["GenRpCmp"][field] = list()
    # print(ViewData)
    return ViewData
def check_empty_data_egi002(ViewData):
    for field in ["DetailIMarketPotential", "MarketShareData", "DetailAdCampaign", "CampaignEffectData"]:
        tmp = [
            i for i in
            [list(d.values())[1:] for d in ViewData[field]]
        ]
        tmp = [i for i in sum(tmp, []) if i]
        if not tmp:
            ViewData[field] = list()
    # print(ViewData)
    return ViewData
def check_empty_data_egi003(ViewData):
    for field in ["DetailHRMember", "DetailHRWelfare", "WordCloud", "DetailHRDevelpoment"]:
        tmp = [
            i for i in
            [list(d.values())[1:] for d in ViewData[field]]
        ]
        tmp = [i for i in sum(tmp, []) if i]
        if not tmp:
            ViewData[field] = list()
    # print(ViewData)
    return ViewData
def check_empty_data_egi004(ViewData):
    for field in ["RndInvest", "RndTrData", "RndArData", "DetailRnDTV"]:
        tmp = [
            i for i in
            [list(d.values())[1:] for d in ViewData[field]]
        ]
        tmp = [i for i in sum(tmp, []) if i]
        if not tmp:
            ViewData[field] = list()
    # print(ViewData)
    return ViewData
def check_empty_data_egi005(ViewData):
    for field in ["DetailProfitability", "DetailStability", "DetailGrowthPotential"]:
        tmp = [
            i for i in
            [list(d.values())[1:] for d in ViewData[field]]
        ]
        tmp = [i for i in sum(tmp, []) if i]
        if not tmp:
            ViewData[field] = list()
    # print(ViewData)
    return ViewData
def check_empty_data_egi006(ViewData):
    for field in ["DetailTechScore", "DetailRightScore", "DetailMarketScore"]:
        tmp = [
            i for i in
            [list(d.values())[1:] for d in ViewData[field]]
        ]
        tmp = [i for i in sum(tmp, []) if i]
        if not tmp:
            ViewData[field] = list()
    return ViewData

def get_egi_from_es(bizNo, yyyy=int(default_yyyy)):
    _flag = "fail"
    if not yyyy:
        yyyy = int(default_yyyy)
    ViewData = get_egi_template()

    conn = get_mysql_conn()
    cur = conn.cursor()

    ### 개요 ###########################################################
    try:
        tot_sc = None
        # 기업성장지수 점수
        try:
            sql = "SELECT " \
                  "BUSINESS_NUM, STD_YEAR, " \
                  "TOT_SC, " \
                  "RI_SC, MR_SC, HR_SC, RD_SC, CR_SC, IP_SC " \
                  "FROM va_idx " \
                  "WHERE BUSINESS_NUM=%s AND STD_YEAR=%s"
            val = (bizNo, yyyy)
            cur.execute(sql, val)
            flag, res = True, cur.fetchall()
            if flag:
                tmp_data = [round(float(i), 2) if i else "" for i in list(res[0])[2:]]
                column_name = ["TotSc", "RpSc", "MkSc", "HrSc", "RdSc", "CrSc", "IpSc"]
                for idx, name in enumerate(column_name):
                    ViewData[name] = tmp_data[idx]
                tot_sc = tmp_data[0]
        except Exception as e:
            # print(e)
            pass

        # 기업성장지수 업계평균
        try:
            sql = "select " \
                  "avg(TOT_SC), std(TOT_SC), " \
                  "avg(RI_SC), avg(MR_SC), avg(HR_SC), avg(RD_SC), avg(CR_SC), avg(IP_SC) " \
                  "from va_idx " \
                  "where BUSINESS_NUM in (" \
                  "select biz_no from cmp_list " \
                  "where ksic_code_lvl2=(select ksic_code_lvl2 from cmp_list where biz_no=%s)" \
                  ") and  STD_YEAR=%s"
            val = (bizNo, yyyy)
            cur.execute(sql, val)
            flag, res = True, cur.fetchall()
            if flag:
                res = list(res[0])
                tmp_data = [round(float(i), 2) if i else "" for i in res[2:]]
                column_name = ["RpScAvg", "MkScAvg", "HrScAvg", "RdScAvg", "CrScAvg", "IpScAvg"]
                for idx, name in enumerate(column_name):
                    ViewData[name] = tmp_data[idx]
                # 기업성장지수 종합등급
                try:
                    import scipy.stats as stats
                    import bisect
                    avg, std = float(res[0]), float(res[1])
                    # print(tot_sc, avg, std)
                    rv = stats.norm(avg, std)
                    score = min(int(rv.cdf(tot_sc) * 100), 100)
                    ViewData["Rank"] = 100-score
                    i = bisect.bisect([20, 40, 60, 80], score)
                    TotIdx = "EDCBA"[i]
                except Exception as e:
                    TotIdx = ""
                ViewData["TotIdx"] = TotIdx
        except Exception as e:
            # print(e)
            pass
        _flag = "success"
    except Exception as e:
        # print(e)
        _flag = "fail"

    ### 지수 ###########################################################
    body = []
    for i in range(1, 7):
        body.append({"index": _view_data_egi})
        body.append({
            "size": 1,
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {"bool": {"must": [
                {"match": {"BusinessNum": bizNo}},
                {"match": {"ViewID": "EGI00" + str(i)}},
                {"match": {"StdYear": yyyy}}
            ]}}
        })
    es = get_es_conn()
    response = es.msearch(index=_view_data_egi, body=body)
    es.close()
    if response:
        for res in response["responses"]:
            try:
                v_id = res["hits"]["hits"][0]["_source"]["ViewID"]
                vd = res["hits"]["hits"][0]["_source"]["ViewData"]
                if v_id == "EGI001":
                    vd = check_empty_data_egi001(vd)
                elif v_id == "EGI002":
                    # 마케팅역량지수 > 광고캠페인효과(CampaignEffectData)
                    try:
                        CampaignEffectData = list()
                        for effect in vd["CampaignEffectData"]:
                            dt = "-".join(effect["Date"].split("-")[:-1])
                            ef = effect["Effect"]
                            CampaignEffectData.append({
                                "Date": dt,
                                "Effect":ef
                            })
                        CampaignEffectData = sorted(CampaignEffectData, key=itemgetter('Date'))
                        vd["CampaignEffectData"] = CampaignEffectData
                    except:
                        pass
                    vd = check_empty_data_egi002(vd)
                elif v_id == "EGI003":
                    vd = check_empty_data_egi003(vd)
                elif v_id == "EGI004":
                    vd = check_empty_data_egi004(vd)
                elif v_id == "EGI005":
                    vd = check_empty_data_egi005(vd)
                elif v_id == "EGI006":
                    vd = check_empty_data_egi006(vd)
                ViewData.update(vd)
                _flag = "success"
            except Exception as e:
                # print(e)
                continue

    #####################################################################################

    cur.close()
    conn.close()

    Data = {
        "BusinessNum": bizNo,
        "ViewID": "IDX",
        "Year": yyyy,
        "ViewData": ViewData

    }
    # import json
    # print(json.dumps(Data, indent=2, ensure_ascii=False))

    # 데이터형 모두 str 변환 (None -> "")
    Data = change_none_to_str(Data)
    return str({"flag": _flag, "Data": Data}),
def get_egi_template():
    ViewData = {
        "Rank": "",
        "TotIdx": "",
        "TotSc": "",
        "RpSc": "",
        "RpScAvg": "",
        "MkSc": "",
        "MkScAvg": "",
        "HrSc": "",
        "HrScAvg": "",
        "RdSc": "",
        "RdScAvg": "",
        "CrSc": "",
        "CrScAvg": "",
        "IpSc": "",
        "IpScAvg": ""
        ,
        "TotalReputationScore": "",
        "TotalReputationIndex": "",
        "DetailReputationScore": {
            "InformationDissemination": "",
            "CustomerInterest": "",
            "MediaExposureLevel": ""
        },
        "DetailInformationDissemination": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailCustomerInterest": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailMediaExposureLevel": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],

        "GenRpDist": [{
            "Type": "",
            "Percent": ""
        }],
        "GenRpCmp": {
            "WordCloud": [{
                "Type": "",
                "Keyword": "",
                "Mention": ""
            }],
            "PosKeyword": [],
            "NegKeyword": [],
            "NeuKeyword": [],
        }
        ,
        "TotalMarketingScore": "",
        "TotalMarketingIndex": "",
        "DetailMarketingScore": {
            "MarketPotential": "",
            "AdCampaign": ""
        },
        "DetailIMarketPotential": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "MarketShareData": [{
            "Year": "",
            "MarketShare": ""
        }],
        "DetailAdCampaign": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "CampaignEffectData": [{
            "Date": "",
            "Effect": ""
        }]
        ,

        "TotalHRScore": "",
        "TotalHRIndex": "",
        "DetailHRScore": {
            "HRMemberScore": "",
            "HRWelfareScore": "",
            "HRDevelopmentScore": "",
            "HRCEOScore": ""
        },
        "DetailHRMember": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailHRWelfare": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "WordCloud": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailHRDevelpoment": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailHRCEO": {
            "TypeMng": "",
            "TypeMkt": "",
            "TypeTch": "",
            "TypeCns": ""
        }
        ,
        "TotalRnDScore": "",
        "TotalRnDIndex": "",
        "DetailRnDScore": {
            "RnDInvestmentScore": "",
            "RnDTRScore": "",
            "RnDTVScore": ""
        },
        "RndInvest": [{
            "Year": "",
            "SalesAmt": "",
            "CosTresearch": "",
            "RndIntensity": ""
        }],
        "RndTrData": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "RndArData": [{
            "Type": "",
            "ArticleCnt": "",
            "TotCitationCnt": "",
            "CitationsComntptArticle": ""
        }],
        "DetailRnDTV": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }]
        ,
        "TotalCRScore": "",
        "TotalCRIndex": "",
        "DetailCRScore": {
            "Profitability": "",
            "Stability": "",
            "GrowthPotential": ""
        },
        "DetailProfitability": [{
            "Type": "",
            "OperatingProfitMargin": ""
        }],
        "DetailStability": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailGrowthPotential": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }]
        ,
        "TotalIPScore": "",
        "TotalIPIndex": "",
        "DetailIPScore": {
            "TechScore": "",
            "RightScore": "",
            "MarketScore": ""
        },
        "DetailTechScore": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailRightScore": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }],
        "DetailMarketScore": [{
            "Type": "",
            "CmpSc": "",
            "AvgSc": ""
        }]
    }
    return ViewData
# 인적자원지수 설문조사 데이터
# data = [36개]
def update_egi003(bizNo, data, yyyy=int(default_yyyy)):
    if not yyyy:
        yyyy = int(default_yyyy)
    flag, ViewID, body = "fail", "EGI003", None
    try:
        query = {
            "size": 1,
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {"bool": {"must": [
                {"match": {"BusinessNum": bizNo}},
                {"match": {"ViewID": ViewID}},
                {"match": {"StdYear": yyyy}}
            ]
        }}}
        es = get_es_conn(host=es_host1)
        res = es.search(index=_view_data_egi, body=query)
        if res["hits"]["total"]["value"] > 0:
            _id = res["hits"]["hits"][0]["_id"]
            # 시장점유율 업데이트
            SearchDate = str(datetime.datetime.now()).replace("T", "")
            body = {
                "doc": {
                    "SearchDate": SearchDate,
                    "ViewData": {"DetailHRCEO": data.strip('][').split(',')}
                }
            }
            result = es.update(
                index=_view_data_egi,
                id=_id,
                body=body
            )
            if result["result"] in ["created", "updated"]:
                flag = "success"
        es.close()
    except Exception as e:
        # print(e)
        pass
    return str({"flag": flag})


############################### 마이페이지 #########################################################
# 마이페이지 화면
def get_mypage_data(bizNo, empGb, empNo, yyyymm=default_yyyymm):
    _flag, ViewData, empGb = "fail", get_mypage_template(empGb=empGb), str(empGb)
    if not yyyymm:
        yyyymm = default_yyyymm

    conn = get_mysql_conn()
    cur = conn.cursor()

    # 일반회원인 경우
    if empGb == '0':
        # 기업의 산업분류코드
        code = get_indust_code(bizNo=bizNo)

        # 동종업계 기업목록 mysql에서 select
        cmp_list = tuple()
        sql = "SELECT biz_no FROM cmp_list WHERE ksic_code_lvl2=%s"
        val = (code,)
        cur.execute(sql, val)
        flag, res = True, cur.fetchall()
        if flag:
            cmp_list = tuple(i[0] for i in res)

        try:
            # 0. 산업통계정보 FLAG = 1 (항상)
            ViewData["IndFlag"] = 1

            # 1. 기술정보 Brief FLAG ~ 기업컨설팅보고서1 FLAG
            try:
                sql = "SELECT " \
                      "BUSINESS_NUM, " \
                      "BRIEF_ST_DATE, BRIEF_END_DATE, " \
                      "RPT1_ST_DATE, RPT1_END_DATE, RPT2_ST_DATE, RPT2_END_DATE, " \
                      "RPT3_ST_DATE, RPT3_END_DATE, RPT4_ST_DATE, RPT4_END_DATE, " \
                      "RPT5_ST_DATE, RPT5_END_DATE, RPT6_ST_DATE, RPT6_END_DATE " \
                      "FROM CMT_RPT " \
                      "WHERE BUSINESS_NUM=%s"
                val = (bizNo,)
                cur.execute(sql, val)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = list(res[0])[1:]
                    column_name = ["BriefFlag", "Rep1Flag", "Rep2Flag", "Rep3Flag", "Rep4Flag", "Rep5Flag", "Rep6Flag"]
                    for i in range(0, len(tmp_data), 2):
                        start, end = tmp_data[i], tmp_data[i + 1]
                        # 시작하지 않았다면 미진행 = 0
                        flag_ = 0
                        if start:
                            # 시작과 종료 모두 존재하면, 완료 = 1
                            if end:
                                flag_ = 1
                            # 시작했지만 종료는 안했다면, 진행중 = 2
                            else:
                                flag_ = 2
                        ViewData[column_name[int(i / 2)]] = flag_
            except Exception as e:
                # print(e)
                pass

            # 2. 기업컨설팅보고서 동종업계 상위기업
            try:
                RepList = list()
                sql = "SELECT c.BIZ_NO, c.CMP_NM, v.RPT1_TOT_SC " \
                      "FROM cmp_list AS c " \
                      "INNER JOIN VA_RPT AS v " \
                      "ON c.BIZ_NO = v.BUSINESS_NUM " \
                      "where c.biz_no in " + str(tuple(cmp_list)) + " order by v.RPT1_TOT_SC desc"
                cur.execute(sql)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = list(dict.fromkeys(res))[:5]
                    for item in tmp_data:
                        RepList.append({
                            "BizNo": item[0],
                            "CmpNm": item[1]
                        })
                for i in range(len(RepList), 5):
                    RepList.append(({
                        "BizNo": "",
                        "CmpNm": ""
                    }))
                ViewData["RepList"] = RepList
            except Exception as e:
                # print(e)
                pass

            # 3. 기업성장지수 점수
            try:
                sql = "SELECT " \
                      "BUSINESS_NUM, STD_YEAR, " \
                      "RI_SC, MR_SC, HR_SC, RD_SC, CR_SC, IP_SC " \
                      "FROM va_idx " \
                      "WHERE BUSINESS_NUM=%s " \
                      "ORDER BY STD_YEAR desc"
                val = (bizNo,)
                cur.execute(sql, val)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = [round(float(i), 2) if i else "" for i in list(res[0])[2:]]
                    column_name = ["RpSc", "MkSc", "HrSc", "RdSc", "CrSc", "IpSc"]
                    for idx, name in enumerate(column_name):
                        ViewData[name] = tmp_data[idx]
            except Exception as e:
                # print(e)
                pass

            # 4. 기업성장지수 업계평균
            try:
                sql = "select " \
                      "avg(RI_SC), avg(MR_SC), avg(HR_SC), avg(RD_SC), avg(CR_SC), avg(IP_SC) " \
                      "from va_idx " \
                      "where BUSINESS_NUM in (" \
                      "select biz_no from cmp_list " \
                      "where ksic_code_lvl2=%s" \
                      ")"
                val = (code,)
                cur.execute(sql, val)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = [round(float(i), 2) if i else "" for i in list(res[0])]
                    column_name = ["RpScAvg", "MkScAvg", "HrScAvg", "RdScAvg", "CrScAvg", "IpScAvg"]
                    for idx, name in enumerate(column_name):
                        ViewData[name] = tmp_data[idx]
            except Exception as e:
                # print(e)
                pass

            # 5. 기업성장지수 동종업계 상위기업
            try:
                IndList = list()
                sql = "SELECT c.BIZ_NO, c.CMP_NM, v.TOT_SC " \
                      "FROM cmp_list AS c " \
                      "INNER JOIN va_idx AS v " \
                      "ON c.BIZ_NO = v.BUSINESS_NUM " \
                      "where c.biz_no in " + str(tuple(cmp_list)) \
                      + " and v.TOT_SC is not null order by v.TOT_SC desc"
                cur.execute(sql)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = list(dict.fromkeys(res))[:5]
                    # BIZ_NO, CMP_NM, KSIC_CODE_LVL2, TOT_SC
                    for item in tmp_data:
                        TotSc = item[-1]
                        if TotSc:
                            TotSc = round(float(TotSc), 2)
                        IndList.append({
                            "BizNo": item[0],
                            "CmpNm": item[1],
                            "TotSc": TotSc,
                        })
                    # 내림차순 정렬
                    IndList = sorted(IndList, key=lambda d: d['TotSc'], reverse=True)
                for i in range(len(IndList), 5):
                    IndList.append(({
                        "BizNo": "",
                        "CmpNm": "",
                        "TotSc": ""
                    }))
                ViewData["IndList"] = IndList
            except Exception as e:
                # print(e)
                pass

            _flag = "success"
        except Exception as e:
            # print(e)
            _flag = "fail"
    # 컨설턴트회원인 경우
    elif empGb == '1':
        try:
            # 1. 기술정보 Brief 최근 조회 기업
            BriefList = list()
            sql = "SELECT c.BIZ_NO, c.CMP_NM " \
                  "FROM cmp_list AS c " \
                  "INNER JOIN cmt_history AS h " \
                  "ON c.BIZ_NO = h.BUSINESS_NUM " \
                  "where h.EMP_NO=%s and SCH_TYPE=0 " \
                  "order by h.SCH_DATE desc"
            val = (empNo,)
            cur.execute(sql, val)
            flag, res = True, cur.fetchall()
            if flag and res:
                tmp_data = list(dict.fromkeys(res))[:5]
                for item in tmp_data:
                    BriefList.append({
                        "BizNo": item[0],
                        "CmpNm": item[1]
                    })
            for i in range(len(BriefList), 5):
                BriefList.append(({
                    "BizNo": "",
                    "CmpNm": ""
                }))
            ViewData["BriefList"] = BriefList

            # 2. 기업컨설팅보고서 최근 조회 기업
            RepList = list()
            sql = "SELECT c.BIZ_NO, c.CMP_NM " \
                  "FROM cmp_list AS c " \
                  "INNER JOIN cmt_history AS h " \
                  "ON c.BIZ_NO = h.BUSINESS_NUM " \
                  "where h.EMP_NO=%s and h.SCH_TYPE<>0 and h.SCH_TYPE<>7 " \
                  "order by h.SCH_DATE desc"
            val = (empNo,)
            cur.execute(sql, val)
            flag, res = True, cur.fetchall()
            if flag:
                tmp_data = list(dict.fromkeys(res))[:5]
                for item in tmp_data:
                    RepList.append({
                        "BizNo": item[0],
                        "CmpNm": item[1]
                    })
            for i in range(len(RepList), 5):
                RepList.append(({
                    "BizNo": "",
                    "CmpNm": ""
                }))
            ViewData["RepList"] = RepList

            # 3. 기업성장지수 최근 조회 기업
            IndList = list()
            sql = "SELECT DISTINCT(BUSINESS_NUM), SCH_DATE " \
                  "FROM cmt_history " \
                  "where EMP_NO=%s and SCH_TYPE=7 " \
                  "order by SCH_DATE desc"
            val = (empNo,)
            cur.execute(sql, val)
            flag, res = True, cur.fetchall()
            if flag:
                biz_list_ = list(dict.fromkeys([i[0] for i in res]))[:5]
                sql = "SELECT BIZ_NO, CMP_NM " \
                      "FROM cmp_list " \
                      "WHERE BIZ_NO IN " + str(biz_list_).replace("[", "(").replace("]", ")")
                cur.execute(sql)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_dict = dict()
                    for item in res:
                        tmp_dict.update({item[0]: [item[1], None]})
                    sql = "SELECT BUSINESS_NUM, TOT_SC " \
                          "FROM va_idx " \
                          "WHERE BUSINESS_NUM IN " + str(biz_list_).replace("[", "(").replace("]", ")")
                    cur.execute(sql)
                    flag, res = True, cur.fetchall()
                    if flag:
                        for item in res:
                            if item[0] in tmp_dict.keys():
                                try:
                                    TotSc = round(float(item[1]), 2)
                                except:
                                    TotSc = ""
                                tmp_dict[item[0]][1] = TotSc
                    for k, v in tmp_dict.items():
                        IndList.append({
                            "BizNo": k,
                            "CmpNm": v[0],
                            "TotSc": v[1]
                        })
            for i in range(len(IndList), 5):
                IndList.append(({
                    "BizNo": "",
                    "CmpNm": "",
                    "TotSc": ""
                }))
            ViewData["IndList"] = IndList

            _flag = "success"
        except Exception as e:
            # print(e)
            _flag = "fail"

    cur.close()
    conn.close()

    Data = {
        "BusinessNum": bizNo,
        "EmpNo": empNo,
        "EmpGb": empGb,
        "ViewID": "MYPAGE",
        "ViewData": ViewData
    }
    # import json
    # print(json.dumps(Data, indent=2, ensure_ascii=False))

    # 데이터형 모두 str 변환 (None -> "")
    Data = change_none_to_str(Data)

    return str({"flag": _flag, "Data": Data}),
def get_mypage_template(empGb):
    # 일반회원인 경우
    empGb = str(empGb)
    BriefList, RepList, IndList = list(), list(), list()
    for i in range(5):
        BriefList.append({
            "BizNo": "",
            "CmpNm": ""
        })
        RepList.append({
            "BizNo": "",
            "CmpNm": ""
        })
        IndList.append({
            "BizNo": "",
            "CmpNm": "",
            "TotSc": ""
        })

    if empGb == '0':
        ViewData = {
            "IndFlag": 1,
            "BriefFlag": "",

            "Rep1Flag": "",
            "Rep2Flag": "",
            "Rep3Flag": "",
            "Rep4Flag": "",
            "Rep5Flag": "",
            "Rep6Flag": "",

            "RepList": RepList,

            "RpSc": "",
            "RpScAvg": "",
            "MkSc": "",
            "MkScAvg": "",
            "HrSc": "",
            "HrScAvg": "",
            "RdSc": "",
            "RdScAvg": "",
            "CrSc": "",
            "CrScAvg": "",
            "IpSc": "",
            "IpScAvg": "",

            "IndList": IndList
        }
    # 컨설턴트회원인 경우
    elif empGb == '1':
        ViewData = {
            "BriefList": BriefList,
            "RepList": RepList,
            "IndList": IndList
        }
    else:
        ViewData = ""

    return ViewData


############################### 컨설팅보고서 #########################################################
# 기업컨설팅보고서 기본화면
def get_report_data(bizNo, empGb, empNo, yyyymm=default_yyyymm):
    _flag, ViewData, empGb = "fail", get_report_template(), str(empGb)
    if not yyyymm:
        yyyymm = default_yyyymm
    first_day_year = (today - relativedelta(months=6)).strftime("%Y-%m-01")

    conn = get_mysql_conn()
    cur = conn.cursor()

    # 기업의 산업분류코드
    code = get_indust_code(bizNo=bizNo)

    # 기업컨설팅보고서1 FLAG
    try:
        sql = "SELECT " \
              "BUSINESS_NUM, " \
              "RPT1_ST_DATE, RPT1_END_DATE, RPT2_ST_DATE, RPT2_END_DATE, " \
              "RPT3_ST_DATE, RPT3_END_DATE, RPT4_ST_DATE, RPT4_END_DATE, " \
              "RPT5_ST_DATE, RPT5_END_DATE, RPT6_ST_DATE, RPT6_END_DATE " \
              "FROM CMT_RPT " \
              "WHERE BUSINESS_NUM=%s"
        val = (bizNo,)
        cur.execute(sql, val)
        flag, res = True, cur.fetchall()
        if flag:
            tmp_data = list(res[0])[1:]
            column_name = ["Rep1Flag", "Rep2Flag", "Rep3Flag", "Rep4Flag", "Rep5Flag", "Rep6Flag"]
            for i in range(0, len(tmp_data), 2):
                start, end = tmp_data[i], tmp_data[i + 1]
                # 시작하지 않았다면 미진행 = 0
                flag_ = 0
                if start:
                    # 시작과 종료 모두 존재하면, 완료 = 1
                    if end:
                        flag_ = 1
                    # 시작했지만 종료는 안했다면, 진행중 = 2
                    else:
                        flag_ = 2
                ViewData[column_name[int(i / 2)]] = flag_
    except Exception as e:
        _flag = "fail"

    # 일반회원인 경우
    if empGb == '0':
        # 동종업계 기업목록 mysql에서 select
        cmp_list = tuple()
        sql = "SELECT biz_no FROM cmp_list WHERE ksic_code_lvl2=%s"
        val = (code,)
        cur.execute(sql, val)
        flag, res = True, cur.fetchall()
        if flag:
            cmp_list = tuple(i[0] for i in res)

        try:

            # 1. 기업컨설팅보고서 동종업계 상위기업
            try:
                RepList = list()
                sql = "SELECT c.BIZ_NO, c.CMP_NM, v.RPT1_TOT_SC " \
                      "FROM cmp_list AS c " \
                      "INNER JOIN VA_RPT AS v " \
                      "ON c.BIZ_NO = v.BUSINESS_NUM " \
                      "where c.biz_no in " + str(tuple(cmp_list)) + " order by v.RPT1_TOT_SC desc"
                cur.execute(sql)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = list(dict.fromkeys(res))[:5]
                    for item in tmp_data:
                        RepList.append({
                            "BizNo": item[0],
                            "CmpNm": item[1]
                        })
                for i in range(len(RepList), 5):
                    RepList.append(({
                        "BizNo": "",
                        "CmpNm": ""
                    }))
                ViewData["RepList"] = RepList
            except:
                pass

            # 2. 기업컨설팅보고서 동종업계 최근 등록기업
            try:
                RecList = list()
                # 유형1 컨설팅보고서 완료시각 내림차순
                sql = "SELECT c.BIZ_NO, c.CMP_NM, r.rpt1_end_date " \
                      "FROM cmp_list AS c " \
                      "INNER JOIN cmt_rpt AS r " \
                      "ON c.BIZ_NO = r.BUSINESS_NUM " \
                      "where c.biz_no in " + str(tuple(cmp_list)) \
                      + " and r.rpt1_end_date >= '" + first_day_year + "' order by r.rpt1_end_date desc"
                cur.execute(sql)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = list(dict.fromkeys(res))[:5]
                    for item in tmp_data:
                        RecList.append({
                            "BizNo": item[0],
                            "CmpNm": item[1]
                        })
                for i in range(len(RecList), 5):
                    RecList.append(({
                        "BizNo": "",
                        "CmpNm": ""
                    }))
                ViewData["RecList"] = RecList
            except:
                pass

            _flag = "success"
        except Exception as e:
            # print(e)
            _flag = "fail"

    # 컨설턴트회원인 경우
    elif empGb == '1':
        try:

            # 1. 기업컨설팅보고서 최근 조회 기업
            try:
                RepList = list()
                sql = "SELECT c.BIZ_NO, c.CMP_NM " \
                      "FROM cmp_list AS c " \
                      "INNER JOIN cmt_history AS h " \
                      "ON c.BIZ_NO = h.BUSINESS_NUM " \
                      "where h.EMP_NO=%s and h.SCH_TYPE<>0 and h.SCH_TYPE<>7 " \
                      "and h.SCH_DATE >= '" + first_day_year + "' order by h.SCH_DATE desc"
                val = (empNo,)
                cur.execute(sql, val)
                flag, res = True, cur.fetchall()
                if flag:
                    tmp_data = list(dict.fromkeys(res))[:5]
                    for item in tmp_data:
                        RepList.append({
                            "BizNo": item[0],
                            "CmpNm": item[1]
                        })
                for i in range(len(RepList), 5):
                    RepList.append(({
                        "BizNo": "",
                        "CmpNm": ""
                    }))
                ViewData["RepList"] = RepList
            except:
                pass

            # 2. 기업컨설팅보고서 최근 등록 기업
            try:
                RecList = list()
                # 유형1 컨설팅보고서 완료시각 내림차순
                sql = "SELECT c.BIZ_NO, c.CMP_NM " \
                "FROM cmp_list AS c " \
                "INNER JOIN cmt_rpt AS r " \
                "ON c.BIZ_NO = r.BUSINESS_NUM " \
                "WHERE rpt1_end_date >= '" + first_day_year + "' order by rpt1_end_date desc"
                cur.execute(sql)
                flag, res = True, cur.fetchall()
                if flag:
                    for t in res[:5]:
                        RecList.append({
                            "BizNo": t[0],
                            "CmpNm": t[1]
                        })
                for i in range(len(RecList), 5):
                    RecList.append(({
                        "BizNo": "",
                        "CmpNm": ""
                    }))
                ViewData["RecList"] = RecList
            except:
                pass

            _flag = "success"
        except Exception as e:
            # print(e)
            _flag = "fail"

    cur.close()
    conn.close()

    Data = {
        "BusinessNum": bizNo,
        "EmpNo": empNo,
        "EmpGb": empGb,
        "ViewID": "REPORT",
        "ViewData": ViewData
    }

    # 데이터형 모두 str 변환 (None -> "")
    Data = change_none_to_str(Data)

    return str({"flag": _flag, "Data": Data}),
def get_report_template():
    ViewData = {
        "Rep1Flag": "",
        "Rep2Flag": "",
        "Rep3Flag": "",
        "Rep4Flag": "",
        "Rep5Flag": "",
        "Rep6Flag": "",
        "RepList": [{"BizNo": "", "CmpNm": ""}] * 5,
        "RecList": [{"BizNo": "", "CmpNm": ""}] * 5
    }
    return ViewData


############################### 기술정보 Brief #########################################################
def get_brief_data(bizNo, yyyymm=default_yyyymm):
    _flag, ViewData = "fail", get_brief_template(bizNo)
    if not yyyymm:
        yyyymm = default_yyyymm

    ################### Codes #####################
    Data = {"BusinessNum": bizNo, "ViewID": "BRIEF", "ViewData": ViewData}
    # 데이터형 모두 str 변환 (None -> "")
    Data = change_none_to_str(Data)

    # 추후 제거
    _flag = "success"

    return (str({"flag": _flag, "Data": Data}),)
def get_brief_template(bizNo):
    try:
        query = {
            "size": 1,
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"BusinessNum": bizNo}},
                        {"match": {"ViewID": "BRIEF"}},
                    ]
                }
            },
        }
        es = get_es_conn(host=es_host1)
        res = es.search(index=_brief_data, body=query)
        if res["hits"]["total"]["value"] > 0:
            Data = res["hits"]["hits"][0]["_source"]["ViewData"]
            Data["SummaryResult"] = Data["SummaryResult"].replace("'", '"')
            
            # 미래성장지수 데이터의 격자가 너무 커 200~1000으로 수정 #############
            FtSc = copy.deepcopy(Data["TotalCg"]["FtSc"])
            FtSc_tmp = list()
            for ft in FtSc:
                Category, UserCompany, CompanyAvg = ft["Category"], ft["UserCompany"], ft["CompanyAvg"]
                if UserCompany is not None:
                    UserCompany = float(UserCompany)
                    if UserCompany >= 1000: UserCompany = 1000
                    if UserCompany < 200: UserCompany = 200

                if CompanyAvg is not None:
                    CompanyAvg = float(CompanyAvg)
                    if CompanyAvg >= 1000: CompanyAvg = 1000
                    if CompanyAvg < 200: CompanyAvg = 200
                    
                FtSc_tmp.append({
                    "Category": Category,
                    "UserCompany": UserCompany,
                    "CompanyAvg": CompanyAvg
                })
            Data["TotalCg"]["FtSc"] = FtSc_tmp
            
            # 평판역량 격차가 커서 조정 #############
            RpSc = copy.deepcopy(Data["TotalCg"]["RpSc"])
            RpSc_tmp = list()
            for rp in RpSc:
                Category, UserCompany, CompanyAvg = rp["Category"], rp["UserCompany"], rp["CompanyAvg"]
                if UserCompany is not None:
                    UserCompany = float(UserCompany)
                    if UserCompany < 20: UserCompany = 20
                    
                if CompanyAvg is not None:
                    CompanyAvg = float(CompanyAvg)
                    if CompanyAvg < 20: CompanyAvg = 20
                    
                RpSc_tmp.append({
                    "Category": Category,
                    "UserCompany": UserCompany,
                    "CompanyAvg": CompanyAvg
                })
            Data["TotalCg"]["RpSc"] = RpSc_tmp           
            #####################################################
            
            Data = change_none_to_str(Data)
            flag = "success"
            return Data
        else:
            Data = {"BusinessNum": bizNo, "ViewID": "BRIEF", "ViewData": ""}
            flag = "noData"
            return str(Data)

    except Exception as e:
        # print(e)
        Data = {"BusinessNum": bizNo, "ViewID": "BRIEF", "ViewData": ""}
        flag = "fail"
        return str(Data)


############################### 산업통계정보 #########################################################
def get_stat_data(ViewID, KsicMain, KsicMid, yyyymm=default_yyyymm):
    _flag, ViewData = "fail", get_stat_template(KsicMain, KsicMid)
    if not yyyymm:
        yyyymm = default_yyyymm

    ################### Codes #####################

    Data = {
        "KsicMain": KsicMain,
        "KsicMid": KsicMid,
        "ViewID": "STAT",
        "ViewData": ViewData
    }
    # 데이터형 모두 str 변환 (None -> "")
    Data = change_none_to_str(Data)

    # 추후 제거
    _flag = "success"

    return str({"flag": _flag, "Data": Data}),
def get_stat_template(KsicMain, KsicMid):
    try:
        query = {
            "size": 1,
            "sort": [{"SearchDate": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"KsicMain": KsicMain}},
                        {"match": {"KsicMid": KsicMid}},
                        {"match": {"ViewID": "STAT"}},
                    ]
                }
            },
        }
        es = get_es_conn(host=es_host1)
        res = es.search(index=_brief_data, body=query)
        if res["hits"]["total"]["value"] > 0:
            Data = res["hits"]["hits"][0]["_source"]["ViewData"]
            
            # 미래성장지수 데이터의 격자가 너무 커 200~1000으로 수정 #############
            FtSc = copy.deepcopy(Data["TotalCg"]["FtSc"])
            FtSc_tmp = list()
            for ft in FtSc:
                Category, UserCompany, CompanyAvg = ft["Category"], ft["UserCompany"], ft["CompanyAvg"]
                if UserCompany is not None:
                    UserCompany = float(UserCompany)
                    if UserCompany >= 1000: UserCompany = 1000
                    if UserCompany < 200: UserCompany = 200

                if CompanyAvg is not None:
                    CompanyAvg = float(CompanyAvg)
                    if CompanyAvg >= 1000: CompanyAvg = 1000
                    if CompanyAvg < 200: CompanyAvg = 200
                    
                FtSc_tmp.append({
                    "Category": Category,
                    "UserCompany": UserCompany,
                    "CompanyAvg": CompanyAvg
                })
            Data["TotalCg"]["FtSc"] = FtSc_tmp
            
            # 평판역량 격차가 커서 조정 #############
            RpSc = copy.deepcopy(Data["TotalCg"]["RpSc"])
            RpSc_tmp = list()
            for rp in RpSc:
                Category, UserCompany, CompanyAvg = rp["Category"], rp["UserCompany"], rp["CompanyAvg"]
                if UserCompany is not None:
                    UserCompany = float(UserCompany)
                    if UserCompany < 20: UserCompany = 20
                    
                if CompanyAvg is not None:
                    CompanyAvg = float(CompanyAvg)
                    if CompanyAvg < 20: CompanyAvg = 20
                    
                RpSc_tmp.append({
                    "Category": Category,
                    "UserCompany": UserCompany,
                    "CompanyAvg": CompanyAvg
                })
            Data["TotalCg"]["RpSc"] = RpSc_tmp           
            #####################################################            
            
            Data["SummaryResult"] = Data["SummaryResult"].replace("'", '"')
            Data = change_none_to_str(Data)
            flag = "success"
            return Data
        else:
            Data = {
                "KsicMain": KsicMain,
                "KsicMid": KsicMid,
                "ViewID": "STAT",
                "ViewData": ""
            }
            flag = "noData"
            return str(Data)

    except Exception as e:
        # print(e)
        Data = {
            "KsicMain": KsicMain,
            "KsicMid": KsicMid,
            "ViewID": "STAT",
            "ViewData": ""
        }
        flag = "fail"
        return str(Data)
