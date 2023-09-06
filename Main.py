# Databricks notebook source
# MAGIC %md
# MAGIC #Class Token

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/Metadata/Govenance_bodies/1.api_datagoverning_bodeis/Token"

# COMMAND ----------

# MAGIC %md
# MAGIC #Class Employee_Data

# COMMAND ----------

# MAGIC %run "/Users/chanwitt@ais.co.th/Metadata/Govenance_bodies/1.api_datagoverning_bodeis/Employee_data"

# COMMAND ----------

# MAGIC %md
# MAGIC #Main

# COMMAND ----------

# MAGIC %md
# MAGIC ##Request token

# COMMAND ----------

url = "https://as-api.ais.th/auth/v3.2/oauth/token"
grant_type = dbutils.secrets.get(scope = 'dataGovern-scope', key = 'dataGovern-app-grant_type')
client_id = dbutils.secrets.get(scope = 'dataGovern-scope', key = 'dataGovern-app-client_id')
client_secret = dbutils.secrets.get(scope = 'dataGovern-scope', key = 'dataGovern-app-client_secret')
content_type = dbutils.secrets.get(scope = 'dataGovern-scope', key = 'dataGovern-app-content_type')

ADMD_token = Token(url=url, grant_type=grant_type, client_id=client_id, client_secret=client_secret, content_type=content_type)
print(ADMD_token)
response, headers = ADMD_token.send_request()
if response:
    print("Response:", response)
    print("Headers:", headers)
else:
    print("Request failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##request data

# COMMAND ----------

url = "https://as-api.ais.th/api/v3/om/GetEmpDetailByRange"
emp_data = Employee_Data(url)
emp_data.header_info(ADMD_token)
print(emp_data)
response, headers = emp_data.send_request(start='1', end='2')
if response:
    print("Response:", response)
    print("Headers:", headers)
else:
    print("Request failed.")

total_emp = emp_data.response_body['dataResult']['totalRow']
print(total_emp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Processed emp data

# COMMAND ----------

intervals = 60
interval_size = total_emp // intervals  # Determine the size of each interval

split_numbers = []
current_number = 0

for i in range(intervals):
    split_numbers.append(current_number + interval_size)
    current_number += interval_size

if  total_emp%intervals != 0:
    split_numbers.append(total_emp)  # Add the last number in the split

print(split_numbers)

# COMMAND ----------

import pandas as pd
from time import sleep

result = pd.DataFrame()
start = 1

for i in range(len(split_numbers)):
    print("==================")
    str_start = str(start)
    print(str_start)
    end = split_numbers[i]
    str_end = str(end)
    print(str_end)
    
    emp_data.send_request(start=str_start, end=str_end)

    data = emp_data.response_body['dataResult']['listEmployee']
    df = pd.DataFrame(data)
    result = result.append(df)

    start = end + 1
    sleep(5)

result.display()
len(result)

# COMMAND ----------

result.display()
len(result)
res = result.copy()

res['enfirstname'] = res['enfirstname'] + " " +res['enlastname']
res['thfirstname'] = res['thfirstname'] + " " +res['thlastname']
res['boss_enfirstname'] = res['boss_enfirstname'] + " " +res['boss_enlastname']
res['boss_thfirstname'] = res['boss_thfirstname'] + " " +res['boss_thlastname']

res.drop(['enlastname','thlastname','boss_enlastname','boss_enlastname'], axis=1,inplace = True)
res.rename(columns={'enfirstname':'enname',
                    'thfirstname':'thname',
                    'boss_enfirstname':'boss_enname',
                    'boss_thfirstname':'boss_thname',},inplace=True)

res.display()

try:
  path_output = "/dbfs/mnt/dmbd-dg/data_governing_bodies/employee_data.csv"
  res.to_csv(path_output, index = False)
except:
  dbutils.fs.mkdirs("dbfs:/mnt/dmbd-dg/data_governing_bodies/")
  path_output = "/dbfs/mnt/data-exploration-blob/dg-storage/data_governing_bodies/employee_data.csv"
  res.to_csv(path_output, index = False)

path_output2 = "dbfs:/mnt/dmbd-dg/data_governing_bodies/employee_data.csv"
# Read the Excel file into a PySpark DataFrame
employee_data = spark.read.format("csv") \
                          .option("header", "true") \
                          .load(path_output2) 
                
employee_data.display()

# COMMAND ----------


