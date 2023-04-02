#from airflow import DAG
#from airflow.operators.python import PythonOperator, BranchPythonOperator
#from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
from random import randint
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium import webdriver
import time
import configparser
import pandas as pd


def configureWebDriver():
    #driver = webdriver.Chrome('C:\chromedriver_win32\chromedriver.exe')
    driver = webdriver.Chrome('/home/paresh/chromedriver_win32/chromedriver.exe')
    return driver

def scrapper():
    driver = configureWebDriver()
    driver.get('https://www.qlik.com/us/partners/find-a-partner')
    time.sleep(3)
    driver.maximize_window()
    time.sleep(1)

    time.sleep(3)
    driver.execute_script("window.scrollTo(0, 2500)")
    time.sleep(10)
    WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((By.XPATH, "//button[contains(@class,'zl_show-more-btn zl_btn-outline')]")))
    time.sleep(3)
    button = driver.find_element(
        By.XPATH, "//button[contains(@class,'zl_show-more-btn zl_btn-outline')]")
    # button.click()
    time.sleep(10)
    driver.execute_script("arguments[0].scrollIntoView();", button)
    driver.execute_script("arguments[0].click();", button)

    soup = BeautifulSoup(driver.page_source,'html.parser')
    elements = soup.find('div',class_='zl_partner-tiles')
    data = elements.find_all('div',class_='zl_partner-tile zl_partner-tile-hover')
    # button = soup.find('button', class_='zl_show-more-btn zl_btn-outline')
    # button


    datalist=[]
    for i in range(0,len(data)):
        partner_name = data[i].find('div',class_="zl_partner-name zl_partner-name-hover").text
        partner_tier = data[i].find('div',class_="zl_partner-tier").text
        partner_address = data[i].find('div',class_="zl_partner-address").get_text(separator=" ").strip()
        # tier = data[i].find('li',class_="zl_more-details-custom-field")
        
        # partner_tier = tier.find('span',class_='zl_value').text
        # if partner_tier != None:
        #     partner_tier= tier.text
        # else:
        #     partner_tier = ''
        
        
        image = data[i].find('div',class_='zl_partner-logo zl_partner-logo-hover')
        img = image.find('img')
        website = data[i].find('div',class_='zl_partner-website')
        if website != None:
            website_url = website.find('a')
            website_url_href = website_url['href']
        else:
            website_url_href=''
        data_dict = {
            'PartnerName':partner_name,
        #    'PartnerTier':partner_tier,
            'PartnerAddress':partner_address,
            'ImageURL': img['src'],
            'Website':website_url_href
        }
        datalist.append(data_dict)

        df = pd.DataFrame(datalist)
        #d
        # f.to_excel(r'E:\Python\Qlik Partner Scrapper\US.xlsx', index = False, header=True)
        df.to_excel(r'/home/paresh/US.xlsx', index = False, header=True)
        #df.to_excel(r'\home\paresh\Data.xlsx', index = False, header=True)



default_args = {
    'owner': 'admin1',
    'start_date': days_ago(0),
    'email': ['paresh.rahool131@tmcltd.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id = 'Web-Scrapping-task',
    default_args=default_args,
    description='This is my Web Scrapping Dag',
    schedule_interval=timedelta(days=1),
)


task1 = PythonOperator(
    task_id = 'Scrapping',
    python_callable = scrapper,
    dag=dag
)

task1
