# 목적: 업종별 과거 속성을 학습 시킨 후 연도별 미래 속성예측

if __name__ != "__main__":
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

from cmath import nan
from genericpath import exists
from Funcs import funcs, check_data_pattern
from elasticsearch_dsl import Search, A, Q
import numpy as np
import pandas as pd
import time, datetime, statistics
import re, itertools, unicodedata, copy, json, joblib
import pandas as pd
from copy import deepcopy
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

# 연도 설정
this_year = int(datetime.datetime.today().year)
past_year = this_year - 7
future_year = this_year + 8

# 기본 dict
AcctNm_mapping = {"매출액": "Sales", "매출원가": "Cost_of_Sales", "매출총손익": "Gross_Profit_or_Loss",
                  "판매비와관리비": "Sales_and_Admin_Expenses",
                  "세금과공과": "Tax_Department", "법인세비용": "Corporate_Tax_Expense", "영업손익": "Operating_Profit",
                  "감가상각비": "Dep", "경상연구개발비": "CapEx", "유동자산": "CurrAsst", "유동부채": "CurrLiab"
                  }


def get_istans_data(biz_no):
    tmp_istans, istansDataDict = None, None
    try:

        ### 산업분류코드 ############################################################################################
        # [기업의 산업분류코드, 10차산업분류코드(2자리), ecos분류(알파벳), istans(4자리) 리스트, 코드세부설명]
        # ex) ['47312', '47', 'G', ['2101', '2100'], '도매 및 소매업']
        # ex) [None, 'etc', 'etc', ['etc'], '']

        IndustCode = funcs.get_indust_code(biz_no=biz_no)
        # print("IndustCode: {}".format(IndustCode))

        tmp_istans = dict()

        ### 국내시장규모 예측하기 -> ISTANS
        # 기업의 산업분류코드==etc => 기타 서비스로 분류
        istansDataDict = {
            "MS": {}
        }
        if IndustCode[3] == ["etc"]:
            query = {
                "size": 1000,
                "sort": [
                    {"Data.IstansYear": {"order": "asc"}}
                ],
                "query": {"bool": {"must": [
                    {"terms": {"Data.IstansGb": ["MS"]}}
                    , {"terms": {"Data.IndustName": ["기타 서비스"]}}
                ]}}
            }
        # 동종업계 ISTANS 데이터 호출
        else:
            query = {
                "size": 1000,
                "sort": [
                    {"Data.IstansYear": {"order": "asc"}}
                ],
                "query": {"bool": {"must": [
                    {"terms": {"Data.IstansGb": ["MS"]}}
                    , {"terms": {"Data.IndustCode": IndustCode[3]}}
                ]}}
            }
        data = funcs.get_data_from_es(index="source_data", query=query)

        if data:
            start = data[0]["_source"]["Data"]["IstansYear"]
            end = data[-1]["_source"]["Data"]["IstansYear"]
            for Year in range(start, end + 1):
                tmp_istans.update({Year: list()})

            for i in istansDataDict.keys():
                tmp_copy = copy.deepcopy(tmp_istans)
                istansDataDict[i] = tmp_copy

            for item in data:
                IstansGb = item["_source"]["Data"]["IstansGb"]
                Year = item["_source"]["Data"]["IstansYear"]
                Price = item["_source"]["Data"]["IstansPrice"]
                istansDataDict[IstansGb][Year].append(Price)

            for IstansGb, v1 in deepcopy(istansDataDict).items():
                for Year, v2 in v1.items():
                    v2 = [i for i in v2 if i is not None]
                    if v2:
                        size = sum(v2) / len(v2)
                    else:
                        size = None
                    istansDataDict[IstansGb][Year] = size

    except Exception as e:
        print(e)
        pass

    return tmp_istans, istansDataDict


def get_ecos_data(biz_no):
    tmp_ecos, ecosDataDict = None, None

    try:
        ### 매출액, 매출원가, 매출총손익, 판매비와관리비, 세금과공과, 법인세비용, 영업손익 예측하기 -> ECOS
        # 기업의 산업분류코드==etc => 기타개인서비스업으로 분류

        # 매출액: Sales 매출원가: Cost_of_Sales 매출총손익:Gross_Profit_or_Loss 판매비와관리비: Sales_and_Admin_Expenses
        # 세금과공과: Tax_Department 법인세비용: Corporate_Tax_Expense 영업손익 : Operating_Profit

        IndustCode = funcs.get_indust_code(biz_no=biz_no)
        # print("IndustCode: {}".format(IndustCode))

        tmp_ecos = dict()

        ecosDataDict = {
            "Sales": {}, "Cost_of_Sales": {}, "Gross_Profit_or_Loss": {}, "Sales_and_Admin_Expenses": {},
            "Tax_Department": {}, "Corporate_Tax_Expense": {}, "Operating_Profit": {}, "Dep": {}, "CapEx": {},
            "CurrAsst": {}, "CurrLiab": {}
        }
        if IndustCode[3] == ["etc"]:
            query = {
                "size": 1000,
                "sort": [
                    {"Data.IstansYear": {"order": "asc"}}
                ],
                "query": {"bool": {"must": [
                    {"match": {"DataType": "ecos"}},
                    {"terms": {
                        "Data.AcctNm": ["매출액", "매출원가", "매출총손익", "판매비와관리비", "세금과공과", "법인세비용", "영업손익", "감가상각비", "경상연구개발비",
                                        "유동자산", "유동부채"]}}
                    , {"terms": {"Data.IndustName": ["기타개인서비스업"]}}
                ]}}
            }

        # 동종업계 ECOS 데이터 호출
        else:
            query = {
                "size": 1000,
                "sort": [
                    {"Data.IstansYear": {"order": "asc"}}
                ],
                "query": {"bool": {"must": [
                    {"match": {"DataType": "ecos"}},
                    {"terms": {
                        "Data.AcctNm": ["매출액", "매출원가", "매출총손익", "판매비와관리비", "세금과공과", "법인세비용", "영업손익", "감가상각비", "경상연구개발비",
                                        "유동자산", "유동부채"]}},
                    {"terms": {"Data.IndustCode": [IndustCode[2] + IndustCode[1]]}}
                ]}}
            }
            query_else = {
                "size": 1000,
                "sort": [
                    {"Data.IstansYear": {"order": "asc"}}
                ],
                "query": {"bool": {"must": [
                    {"match": {"DataType": "ecos"}},
                    {"terms": {
                        "Data.AcctNm": ["매출액", "매출원가", "매출총손익", "판매비와관리비", "세금과공과", "법인세비용", "영업손익", "감가상각비", "경상연구개발비",
                                        "유동자산", "유동부채"]}},
                    {"terms": {"Data.IndustCode": [IndustCode[2]]}}
                ]}}
            }

        print()
        # print(query)
        data = funcs.get_data_from_es(index="source_data", query=query)
        # print(data)

        if data:
            start = int(data[0]["_source"]["Data"]["EcosYear"])
            end = int(data[-1]["_source"]["Data"]["EcosYear"])
            for Year in range(start, end + 1):
                tmp_ecos.update({Year: list()})

            for i in ecosDataDict.keys():
                tmp_copy = copy.deepcopy(tmp_ecos)
                ecosDataDict[i] = tmp_copy

            for item in data:
                AcctNm = item["_source"]["Data"]["AcctNm"]
                Year = item["_source"]["Data"]["EcosYear"]
                AcctAmt = item["_source"]["Data"]["AcctAmt"]
                ecosDataDict[AcctNm_mapping[AcctNm]][int(Year)] = AcctAmt

        else:  # data없는경우 알파벳으로 검색
            data_onlyalphabet = funcs.get_data_from_es(index="source_data", query=query_else)

            if data_onlyalphabet:  # 있는경우 똑같이위에 코드 가져오기
                start = int(data_onlyalphabet[0]["_source"]["Data"]["EcosYear"])
                end = int(data_onlyalphabet[-1]["_source"]["Data"]["EcosYear"])
                for Year in range(start, end + 1):
                    tmp_ecos.update({Year: list()})

                for i in ecosDataDict.keys():
                    tmp_copy = copy.deepcopy(tmp_ecos)
                    ecosDataDict[i] = tmp_copy

                for item in data_onlyalphabet:
                    AcctNm = item["_source"]["Data"]["AcctNm"]
                    Year = item["_source"]["Data"]["EcosYear"]
                    AcctAmt = item["_source"]["Data"]["AcctAmt"]
                    ecosDataDict[AcctNm_mapping[AcctNm]][int(Year)] = AcctAmt

            else:  # 없는경우 그냥 0넣기
                zero_tmp_ecos = {}
                start = int("2015")
                end = int("2021")
                for Year in range(start, end + 1):
                    tmp_ecos.update({Year: list()})
                    zero_tmp_ecos.update({Year: 0})

                for i in ecosDataDict.keys():
                    tmp_copy = copy.deepcopy(zero_tmp_ecos)
                    for Year in range(start, end + 1):
                        ecosDataDict[i] = tmp_copy

    except Exception as e:
        print(e)
        pass

    return tmp_ecos, ecosDataDict


def Prediction(biz_no):
    result_df = None

    try:
        ### 데이터 불러오기 ####################################################
        tmp_istans, istansDataDict = get_istans_data(biz_no)
        # print("------------------")
        # print(tmp_istans,istansDataDict)
        tmp_ecos, ecosDataDict = get_ecos_data(biz_no)
        # print("-----------------")
        # print(tmp_ecos,ecosDataDict)

        ### 미래값 예측 #######################################################
        Year = [y for y in range(past_year, future_year + 1)]

        pred_X = pd.DataFrame({'Year': Year})
        result_df = pd.DataFrame({'Year': Year})

        ### istans - MS ######################################################
        if istansDataDict.values():
            df_istans = pd.DataFrame({
                'Year': list(tmp_istans.keys()),
                'MS': list(istansDataDict["MS"].values())
            })

            ### 결측치 제거
            df_istans = df_istans.dropna()

            X_istans = df_istans[["Year"]]
            # y_istans = df_istans[["MS"]]

            df_istans_tmp = df_istans.loc[df_istans["Year"] >= 2015]
            df_istans_tmp = df_istans_tmp.reset_index()
            df_istans_tmp = df_istans_tmp.drop(['index'], axis=1)

            for i in df_istans.columns[1:]:
                result_df[i] = np.nan

            for year in df_istans_tmp['Year']:
                result_df.loc[result_df.index[result_df['Year'] == year], 'MS'] = df_istans_tmp.iloc[
                    result_df.index[result_df['Year'] == year]]

            # result_df.update(df_istans_tmp,overwrite=True)

            for istans_column in df_istans.columns:

                if not istans_column == 'Year':

                    model = LinearRegression()
                    model.fit(X_istans, df_istans[[istans_column]])
                    pred_y = model.predict(pred_X)  # 예측값

                    for idx in result_df.index:
                        if np.isnan(result_df.iloc[idx, :][istans_column]):
                            result_df.at[idx, istans_column] = pred_y[idx]

        ### ecos- 이하 7개 columns #############################################
        if ecosDataDict.values():

            df_ecos = pd.DataFrame({
                'Year': list(tmp_ecos.keys()),
                'Sales': list(ecosDataDict["Sales"].values()),
                'Cost_of_Sales': list(ecosDataDict["Cost_of_Sales"].values()),
                'Gross_Profit_or_Loss': list(ecosDataDict["Gross_Profit_or_Loss"].values()),
                'Sales_and_Admin_Expenses': list(ecosDataDict["Sales_and_Admin_Expenses"].values()),
                'Tax_Department': list(ecosDataDict["Tax_Department"].values()),
                'Corporate_Tax_Expense': list(ecosDataDict["Corporate_Tax_Expense"].values()),
                'Operating_Profit': list(ecosDataDict["Operating_Profit"].values()),
                'Dep': list(ecosDataDict["Dep"].values()),
                'CapEx': list(ecosDataDict["CapEx"].values()),
                'CurrAsst': list(ecosDataDict["CurrAsst"].values()),
                'CurrLiab': list(ecosDataDict["CurrLiab"].values())
            })

            ### 결측치 제거
            df_ecos = df_ecos.dropna()

            X_ecos = df_ecos[["Year"]]
            # y_ecos = df_ecos[["Sales","Cost_of_Sales","Gross_Profit_or_Loss","Sales_and_Admin_Expenses","Tax_Department","Corporate_Tax_Expense","Operating_Profit"]]

            df_ecos_tmp = df_ecos.loc[df_ecos["Year"] >= 2015]
            df_ecos_tmp = df_ecos_tmp.reset_index()
            df_ecos_tmp = df_ecos_tmp.drop(['index'], axis=1)

            for i in df_ecos.columns[1:]:
                result_df[i] = np.nan

            # result_df.update(df_ecos_tmp)

            for year in df_ecos_tmp['Year']:
                for col in result_df.columns:
                    if col not in ['Year', 'MS']:
                        result_df.loc[result_df.index[result_df['Year'] == year], col] = df_ecos_tmp.iloc[
                            result_df.index[result_df['Year'] == year]]

            # print(result_df)

            for ecos_column in df_ecos.columns:
                if not ecos_column == 'Year':

                    model = LinearRegression()
                    model.fit(X_ecos, df_ecos[[ecos_column]])
                    pred_y = model.predict(pred_X)  # 예측값

                    for idx in result_df.index:
                        if np.isnan(result_df.iloc[idx, :][ecos_column]):
                            result_df.at[idx, ecos_column] = pred_y[idx]

        result_df = result_df.astype(int)
        result_df.set_index('Year', inplace=True)

        # ### 데이터 확인용
        # print("="*100)
        # print(result_df)
        # print()

    except Exception as e:
        print(e)
        # print("Exception")

        # ### 데이터 확인용
        # print("="*100)
        # print(result_df)
        # print()
    return result_df


if __name__ == "__main__":
    # 잘나옴
    res = Prediction(
        biz_no=1018157784
    )
    print(res)

    # 음...!
    res = Prediction(
        biz_no=1018220101
    )
    print(res)




#     # index_name = "view_data_renewal"
#
#     l = [
#         "1008109981",
#         "8078100337",
#         "1138185148",
#         "1138196801",
#         "1138611016",
#         "1138629821",
#         "1138639275",
#         "2098154271",
#         "2108178540",
#         "2118642832",
#         "2118710133",
#         "2118745307",
#         "2118767975",
#         "2118780100",
#
#         "6200795203"
#
#     ]
#
#     # for biz_no in l:
#     #     Prediction(
#     #         biz_no=biz_no
#     #     )
#
#     for idx, biz_no in enumerate(l):
#         # if idx < 3:
#         #     continue
#         print("{}/{}".format(idx + 1, len(l)))
#         biz_no = str(int(biz_no))
#         res = Prediction(
#             biz_no=biz_no
#         )
#         print(res)
#         print("-" * 100)
#         # break
