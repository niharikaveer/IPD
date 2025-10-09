from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import re

driver = webdriver.Chrome()
pattern = re.compile(r".*/(\d+)/.*")
id_queue = []
year = 1950
for i in range(1):
    driver.get(f"https://indiankanoon.org/search/?formInput=doctypes%3A%20supremecourt%20year%3A%20{year}&pagenum={i}")
    elems = driver.find_elements(By.CLASS_NAME, "result_title")
    for elem in elems:
        id = elem.find_element(By.TAG_NAME,"a").get_attribute("href")
        match = pattern.match(id)
        id_queue.append(match.group(1))
    
    while(len(id_queue) > 0):
        id = id_queue.pop(0)
        driver.get(f"https://indiankanoon.org/doc/{id}")
        data = driver.find_element(By.CLASS_NAME,"judgments")
        with open(f"data/{year}_{i}_{id}.html","a") as f:
            f.write(data.get_attribute("outerHTML"))
    time.sleep(2)
driver.close()