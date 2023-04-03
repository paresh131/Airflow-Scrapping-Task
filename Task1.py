from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


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

default_args = {
    'owner': 'admin1',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'web_scraper',
    default_args=default_args,
    description='web_scraping_job',
    schedule_interval=timedelta(days=1)
)

def run_scraper():
    # Import necessary modules
    from selenium import webdriver
    import time
    
    # Set up the webdriver (in this case, using Chrome)
    #driver = webdriver.ChromeOptions()
    #driver=webdriver.Chrome('/mnt/c/Program Files/chromedriver_win32/chromedriver.exe')
    #driver=webdriver.Chrome('chromedriver.exe')
    #driver.add_argument('--headless')
    driver = webdriver.Chrome(executable_path="chromedriver.exe")
    
    #driver = webdriver.Chrome(options=driver)
    
    # Set the URL to scrape and perform the scraping
    #driver.get('https://www.example.com')
    #time.sleep(10)
    # Add your scraping code here
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
        #df.to_excel(r'/home/paresh/US.xlsx', index = False, header=True)
        df.to_excel(r'\home\paresh\Data.xlsx', index = False, header=True)
    
    # Close the webdriver
    driver.quit()

run_web_scraper = PythonOperator(
    task_id='run_web_scraper',
    python_callable=run_scraper,
    dag=dag
)

run_web_scraper