from func import funcs
import time
import math
import datetime as dt
import random
import os

from bs4 import BeautifulSoup as bs
import urllib.request
import urllib.parse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

WEBDRIVER_PATH = r".\chromedriver.exe"
WEBDRIVER_OPTIONS = webdriver.ChromeOptions()
WEBDRIVER_OPTIONS.add_argument('headless')
WEBDRIVER_OPTIONS.add_argument('--disable-dev-shm-usage')
WEBDRIVER_OPTIONS.add_argument('--no-sandbox')
WEBDRIVER_OPTIONS.add_argument('--ignore-certificate-errors')

def get_id(biz_no, CompanyName):

    # 크롤링 시작
    print(f'{CompanyName} start....')

    ############################################ 기업 검색해서 출원번호 뽑아내는 코드... ################################################
    webDriver = webdriver.Chrome(options=WEBDRIVER_OPTIONS, executable_path=WEBDRIVER_PATH)
    url = 'http://kpat.kipris.or.kr/kpat/searchLogina.do?next=MainSearch#page1'
    webDriver.get(url)

    search = 'AP=[' + CompanyName + ']'
    search_xpath = '//*[@id="queryText"]'
    input_search = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.XPATH, search_xpath)))
    input_search.send_keys(search)

    css_selector = '#divSelSort_1'
    search_ = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
    search_.click()

    css_selector = '#btnSelSortLayer_1_1'
    search_ = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
    time.sleep(0.5)
    search_.click()

    css_selector = '#btnSortOrderDesc_1'
    search_ = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
    search_.click()

    css_selector = '#leftside > div > span > a > img'
    search_ = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
    search_.click()

    loading_bar = webDriver.find_element_by_xpath('//*[@id="loadingBarBack"]')

    while True: # 로딩 끝나면 탈출
        if 'display: none' in loading_bar.get_attribute('style'):
            break

    # id 추출
    try: # 기업 특허 하나도 없으면 탈출
        webDriver.find_element_by_class_name('search_nodata')

        index = "kipris_test_211206"
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"DataType": "kipris"}},
                        {"match": {"BusinessNum" : str(biz_no)}}
                        ]
                    }
                }
            }
        Data = funcs.get_data_from_es(index, query)

        if Data :
            return
        else:
            Kipris_data = None
            kipris = {
            "BusinessNum": biz_no,
            "DataType" : "kipris",
            "SearchDate": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%f")[:-3],
            "SearchID": "autoSystem",
            "Data": Kipris_data
            }

            id_ = "Nodata_" + str(biz_no)

            funcs.save_data_to_es(
            index="kipris_test_211206",
            id=id_,
            data=kipris)

            return

    except:
        total = int(webDriver.find_element_by_xpath('//*[@id="divMainArticle"]/form/section/div[1]/p/span[1]').text.replace(',','')) # 총 결과수
        total_page = math.ceil(total / 30)

        if total > 30: # 30개 초과
            id = []
            cpl = len(webDriver.find_elements_by_xpath('//*[@id="divMainArticle"]/form/section/article')) # 현재 페이지 결과수
            [id.append(webDriver.find_element_by_xpath(f'//article[{idx}]/div[2]/ul/div[1]/li[2]/span[2]/a').text.split()[0]) for idx in range(1,cpl+1)]
            page_num = 1
            selector_num = 2

            try:
                while True:
                    page_num += 1
                    selector_num += 1
                    css_selector = f'#divMainArticle > form > section > div.float_right > span > a:nth-child({selector_num})'
                    btn_login = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
                    btn_login.click()
                    loading_bar = webDriver.find_element_by_xpath('//*[@id="loadingBarBack"]')

                    while True: # 로딩 끝나면 탈출
                        if 'display: none' in loading_bar.get_attribute('style'):
                            break

                    cpl = len(webDriver.find_elements_by_xpath('//*[@id="divMainArticle"]/form/section/article'))
                    [id.append(webDriver.find_element_by_xpath(f'//article[{idx}]/div[2]/ul/div[1]/li[2]/span[2]/a').text.split()[0]) for idx in range(1,cpl+1)]

                    if page_num > total_page:
                        break

                    if selector_num == 11:
                        selector_num = 2
                        css_selector = f'#divMainArticle > form > section > div.float_right > span > a.next'
                        btn_login = WebDriverWait(webDriver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
                        btn_login.click()
                        loading_bar = webDriver.find_element_by_xpath('//*[@id="loadingBarBack"]')
                        
                        while True: # 로딩 끝나면 탈출
                            if 'display: none' in loading_bar.get_attribute('style'):
                                break

                        cpl = len(webDriver.find_elements_by_xpath('//*[@id="divMainArticle"]/form/section/article'))
                        [id.append(webDriver.find_element_by_xpath(f'//article[{idx}]/div[2]/ul/div[1]/li[2]/span[2]/a').text.split()[0]) for idx in range(1,cpl+1)]
                        
            except:
                pass

        else: # 30개 이하
            id = [webDriver.find_element_by_xpath(f'//article[{idx}]/div[2]/ul/div[1]/li[2]/span[2]/a').text.split()[0] for idx in range(1,total+1)]

        print(id)

        return id

def crawl_main(CompanyName, biz_no, id):
    
    time.sleep(random.randint(1,7))

    Kipris_data = Kipris_templete()
    tab = {"Sub01":"BASE", "Sub02":"Sub02", "Sub07":"Sub07", "Sub08":"Sub08", "Sub11":"Sub11"}

    for k,v in tab.items():
        url = f'http://kpat.kipris.or.kr/kpat/biblioa.do?method=biblioMain_biblio&next=biblioView{k}&applno={id}&getType={v}&link=N'

        try:
            time.sleep(1) # sleep 안걸면 밑에서 가끔 TimeoutError 발생
            with urllib.request.urlopen(url) as response:
                soup = bs(response, 'lxml')
                if k == 'Sub01': # 서지정보

                    indx = [indx.find('strong').text.replace('(국제)','').split(') ')[-1] for indx in soup.find_all("li")]

                    Title = soup.select_one('#apttl').get_text() # 특허 명칭

                    IPC = [ipc.text for ipc in soup.select('#divBiblioContent > div.detial_plan_info > ul > li:nth-child(1) > span > a')]
                    IPC = [ipc.split('(')[0].replace(' ','') for ipc in IPC] # IPC 번호
                    CPC = [ipc.text for ipc in soup.select('#divBiblioContent > div.detial_plan_info > ul > li:nth-child(2) > span > a')]
                    CPC = [cpc.split('(')[0].replace(' ','') for cpc in CPC] # CPC 번호

                    Applicationdata = Ex_tag(soup.find_all("li")[indx.index('출원번호/일자')], 'strong') # 출원번호 / 일자
                    if Applicationdata:
                        AppNum = Applicationdata.split()[0] # 출원번호
                        AppDate = Applicationdata.split()[1].replace('(','').replace(')','').replace('.','') 
                        AppDate = string_to_date(AppDate) # 출원일자
                    else:
                        AppNum = None
                        AppDate = None

                    AppName = soup.find_all("li")[indx.index('출원인')].get_text(separator='|', strip=True).split('|')[1:] # 출원인

                    Registerdata = Ex_tag(soup.find_all("li")[indx.index('등록번호/일자')], 'strong') # 등록번호 / 일자
                    if Registerdata:
                        RegiNum = Registerdata.split()[0] # 등록번호
                        RegiDate = Registerdata.split()[1].replace('(','').replace(')','').replace('.','') 
                        RegiDate = string_to_date(RegiDate) # 등록일자
                    else:
                        RegiNum = None
                        RegiDate = None

                    Opendata = Ex_tag(soup.find_all("li")[indx.index('공개번호/일자')], 'strong') # 공개번호 / 일자
                    if Opendata:
                        OpenNum = Opendata.split()[0] # 공개번호
                        OpenDate = Opendata.split()[1].replace('(','').replace(')','').replace('.','') 
                        OpenDate = string_to_date(OpenDate) # 공개일자
                    else:
                        OpenNum = None
                        OpenDate = None

                    RegiStatus = soup.find_all("li")[indx.index('법적상태')].select('b')[0].get_text().lstrip().rstrip() # 법적상태

                    ExaminationCount = Ex_tag(soup.find_all("li")[indx.index('심사청구항수')], 'strong') # 심사청구항수

                    AstrtCont = soup.find_all("summary")[0].get_text().replace('\n','') # 요약

                    Kipris_data['InventionTitle'] = Title # 특허 명칭
                    Kipris_data['IPCNumber'] = IPC # IPC
                    Kipris_data['CPCNumber'] = CPC # CPC
                    Kipris_data['ApplicationNumber'] = AppNum # 출원번호
                    Kipris_data['ApplicationDate'] = AppDate # 출원일자
                    Kipris_data['ApplicantName'] = AppName # 출원인
                    Kipris_data['RegisterNumber'] = RegiNum # 등록번호
                    Kipris_data['RegisterDate'] = RegiDate # 등록일자
                    Kipris_data['OpenNumber'] = OpenNum # 공개번호
                    Kipris_data['OpenDate'] = OpenDate # 공개일자
                    Kipris_data['RegisterStatus'] = RegiStatus # 법적상태
                    Kipris_data['ExaminationCount'] = ExaminationCount # 심사청구항수
                    Kipris_data['AstrtCont'] = AstrtCont # 요약

                    continue 

                if k == 'Sub02': # 인명정보
                    InventorCount = len(soup.select('#divBiblioContent > table:nth-child(6) > tbody > tr')) # 발명자수
                    Kipris_data['InventorCount'] = str(InventorCount) # 발명자수

                    continue

                if k == 'Sub07': # 인용/피인용
                    FC_data = [] # 인용
                    for FC in soup.select('#divBiblioContent > table:nth-child(4) > tbody > tr'):
                        if '데이터가 존재하지 않습니다.' in FC.text:
                            break
                        else:
                            FC_data.append([fc.text for fc in FC.find_all('td')])

                    BC_data = [] # 피인용
                    for BC in soup.select('#divBiblioContent > table:nth-child(7) > tbody > tr'):
                        if '데이터가 존재하지 않습니다.' in BC.text:
                            break
                        else:
                            BC_data.append([bc.text for bc in BC.find_all('td')])
                    
                    if FC_data:
                        Kipris_data['ForwardCitation'] = []
                        for FC in FC_data:
                            Kipris_data['ForwardCitation'].append({
                            'FCCountry' : FC[0], # 국가
                            'FCNumber' : FC[1].replace('\n','').replace(' ',''), # 공보번호
                            'FCDate' : string_to_date(FC[2].replace('.','')), # 공보일자
                            'FCTitle' : FC[3].replace('\n',''), # 발명의 명칭
                            'FCIPC' : FC[4].rstrip().lstrip().replace(' ','') # IPC
                            })
                    else:
                        pass

                    if BC_data:
                        Kipris_data['BackwardCitation']=[]
                        for BC in BC_data:
                            Kipris_data['BackwardCitation'].append({
                            'BCNumber' : BC[0], # 출원번호
                            'BCDate' : string_to_date(BC[1].replace('.','')), # 출원일자
                            'BCTitle' : BC[2], # 발명의 명칭
                            'BCIPC' : BC[3].rstrip().lstrip().replace(' ','') # IPC
                            })
                    else:
                        pass

                    continue

                if k == 'Sub08': # 패밀리 정보

                    FAMILY_data = [] # 패밀리 정보
                    for FM in soup.select('#divBiblioContent > table:nth-child(1) > tbody > tr'):
                        if '데이터가 존재하지 않습니다.' in FM.text:
                            break
                        else:
                            FAMILY_data.append([fm.text for fm in FM.find_all('td')])

                    DOCDB_data = [] # DOCDB 패밀리
                    for DOC in soup.select('#divBiblioContent > table:nth-child(4) > tbody > tr'):
                        if '데이터가 존재하지 않습니다.' in DOC.text:
                            break
                        else:
                            DOCDB_data.append([doc.text for doc in DOC.find_all('td')])
                    
                    if FAMILY_data:
                        Kipris_data['Family']=[]
                        for FM in FAMILY_data:
                            Kipris_data['Family'].append({
                            'FamilyNumber' : FM[1].rstrip().lstrip(), # 패밀리번호
                            'FamilyCountrycode' : FM[2], # 국가코드
                            'FamilyCountryname' : FM[3], # 국가명
                            'FamilyType' : FM[4], # 종류
                            })
                    else:
                        pass

                    if DOCDB_data:
                        Kipris_data['DOCDBfamily']=[]
                        for DOC in DOCDB_data:
                            Kipris_data['DOCDBfamily'].append({
                            'DOCDBnumber' : DOC[1].rstrip().lstrip(), # 패밀리번호
                            'DOCDBcountrycode' : DOC[2], # 국가코드
                            'DOCDBcountryname' : DOC[3], # 국가명
                            'DOCDBtype' : DOC[4], # IPC
                            })
                    else:
                        pass                

                    continue

                if k == 'Sub11': # 국가연구개발사업

                    RS_data = []
                    for RS in soup.select('#divBiblioContent > table > tbody > tr'):
                        if '데이터가 존재하지 않습니다.' in RS.text:
                            break
                        else:
                            RS_data.append([rs.text for rs in RS.find_all('td')])
                    
                    if RS_data:
                        Kipris_data['ResearchData']=[]
                        for RS in RS_data:
                            Kipris_data['ResearchData'].append({
                            'ResearchDepartment' : RS[1], # 연구부처
                            'ResearchInstitution' : RS[2], # 주관기관
                            'ResearchBusiness' : RS[3], # 연구사업
                            'ResearchProject' : RS[4], # 연구과제
                            })
                    else:
                        pass

            Success=True

        except TimeoutError as e:
            print(f'{id}의 {k} 에서 실패... {e}')
            with open('./Errorlog/kipris_errorlog/' + CompanyName + '_' + biz_no + '_' + str(id) + '.txt', 'w') as f:
                f.write(f'{id}의 {k} 에서 실패...{e}')
                
            Success=False
            break

    if Success:
        kipris = {
        "BusinessNum": biz_no,
        "DataType" : "kipris",
        "SearchDate": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%f")[:-3],
        "SearchID": "autoSystem",
        "Data": Kipris_data
        }

        id_ = "kipris_" + str(id)

        if funcs.re_type(CompanyName) in list(map(funcs.re_type, AppName)):
            funcs.save_data_to_es(
            index="kipris_test_211206",
            id=id_,
            data=kipris)

        else:
            print(f'{id}, 기업명과 출원인이 일치하지 않습니다.')
    else:
        pass

    return Success

def error_re_save(): # error 발생한 목록 재적재
    ERROR_PATH = './Errorlog/kipris_errorlog'
    file_list = os.listdir(ERROR_PATH)
    if file_list:
        for i in file_list:

            i = i.replace('.txt','')
            CompanyName = i.split('_')[0]
            biz_no = i.split('_')[1]
            id = i.split('_')[2]

            Success = crawl_main(CompanyName, biz_no, id)
            if Success:
                os.remove(ERROR_PATH + '/' + i + '.txt')
    else:
        return

def Ex_tag(p, tag): # 특정 태그 "제외"한 문자열 추출하는 함수
    return ''.join(text for text in p.find_all(text=True) if text.parent.name != tag).lstrip().rstrip()

def string_to_date(YYYYMMDD):
    return str(dt.datetime.strptime(YYYYMMDD, '%Y%m%d').date())

# def delete_empty_data(biz_no):

#     index = "kipris_test_211206"
#     query1 = {
#         "query":{
#             "bool":{
#                 "must":[
#                     {"match":{"BusinessNum":str(biz_no)}}
#                     ]
#                 }
#             }
#         }

#     Data1 = funcs.get_data_from_es(index, query1)

#     query2 = {
#         "query": {
#             "bool": {
#                 "must": {"match":{"BusinessNum":str(biz_no)}},
#                 "must_not": {"exists": {"field": "Data"}}
#                 }
#             }
#         }
#     Data2 = funcs.get_data_from_es(index, query2)

#     if bool(Data1) and bool(Data2):
#         # delete by query2
#         pass

def kipris_main(biz_no, CompanyName): # 외부 호출용 메인함수

    id_list = get_id(biz_no, CompanyName)

    if id_list:
        for i in id_list:
            crawl_main(CompanyName, biz_no, id=i)
    else:
        pass

def Kipris_templete():
    Kipris_data = {
    'InventionTitle' : None,
    'IPCNumber' : None,
    'CPCNumber' : None,
    'ApplicationNumber' : None,
    'ApplicationDate' : None,
    'ApplicantName' : None,
    'RegisterNumber' : None,
    'RegisterDate' : None,
    'OpenNumber' : None,
    'OpenDate' : None,
    'RegisterStatus' : None,
    'ExaminationCount' : None,
    'AstrtCont' : None,
    'InventorCount' : None,
    'ForwardCitation' : None,
    'BackwardCitation' : None,
    'Family' : None,
    'DOCDBfamily' : None,
    'ResearchData' : None
    }

    return Kipris_data

if __name__ == "__main__":

    biz_no = '6478100375'
    CompanyName = '아이콘루프'

    id_list = get_id(biz_no, CompanyName)
    if id_list:
        for i in id_list:
            crawl_main(CompanyName, biz_no, id=i)
    else:
        pass

    # error_re_save()
