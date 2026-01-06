import time
from tqdm import tqdm_notebook
import warnings
warnings.filterwarnings('ignore')

import pandas as pd

from selenium import webdriver 
from selenium.webdriver.common.by import By 

url = "https://www.oliveyoung.co.kr/store/display/getCategoryShop.do?dispCatNo=10000010001&gateCd=Drawer&t_page=%EB%93%9C%EB%A1%9C%EC%9A%B0_%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_click=%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC%ED%83%AD_%EB%8C%80%EC%B9%B4%ED%85%8C%EA%B3%A0%EB%A6%AC&t_1st_category_type=%EB%8C%80_%EC%8A%A4%ED%82%A8%EC%BC%80%EC%96%B4"
driver = webdriver.Chrome("../driver/chromedriver")
driver.get(url)

# 데이터 프레임 생성 
df_review_cos1 = pd.DataFrame(columns=['category', 'ranking', 'name', 'brand', 'price', 'sale_price', 'date', 'rate', 'id', 'skin_type', 'select_1_title', 'select_1_content', 'select_2_title', 'select_2_content', 'select_3_title', 'select_3_content', 'txt'])
df_review_cos1

# 상품 리뷰 크롤링 함수
def review_crawling(df, page_num):    
    for current_page in range(1, page_num):      
        # 리뷰의 인덱스는 한 페이지 내에서 1~10까지 존재
        for i in range(1, 10):  # 한 페이지 내 10개 리뷰 크롤링
            try:
                date = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.score_area > span.date').text
                rate = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.score_area > span.review_point > span').text
                id = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.info > div > p.info_user > a.id').text
                skin_type = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(1) > dd > span').text
                select_1_title = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(1) > dt > span').text
                select_1_content = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(1) > dd > span').text
                select_2_title = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(2) > dt > span').text
                select_2_content = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(2) > dd > span').text
                select_3_title = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(3) > dt > span').text
                select_3_content = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.poll_sample > dl:nth-child(3) > dd > span').text
                txt = driver.find_element(By.CSS_SELECTOR, f'#gdasList > li:nth-child({i}) > div.review_cont > div.txt_inner').text
                df.loc[len(df)] = [category, ranking, name, brand, price, sale_price, date, rate, id, skin_type, select_1_title, select_1_content, select_2_title, select_2_content, select_3_title, select_3_content, txt]
            except:
                    pass
        try:
            if current_page % 10 != 0: # 현재 페이지가 10의 배수가 아닐 때
                if current_page // 10 < 1: # 페이지 수가 한 자리수 일 때  
                    # 리뷰 10개 긁으면 next 버튼 클릭
                    page_button = driver.find_element(By.CSS_SELECTOR, f'#gdasContentsArea > div > div.pageing > a:nth-child({current_page%10+1})')
                    page_button.click() 
                    time.sleep(2)
                else: # 페이지 수가 두자리 수 이상일 때 
                    # 리뷰 10개 긁으면 next 버튼 클릭
                    page_button = driver.find_element(By.CSS_SELECTOR, f'#gdasContentsArea > div > div.pageing > a:nth-child({current_page%10+2})')
                    page_button.click() 
                    time.sleep(2)
            else:
                next_button = driver.find_element(By.CSS_SELECTOR, '#gdasContentsArea > div > div.pageing > a.next')
                next_button.click() # 현재 페이지가 10의 배수일 때 페이지 넘김 버튼 클릭
                time.sleep(2)
        except:
            pass
        print(f'{current_page}페이지 크롤링 완료')