import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as f 
import requests
import json
from pyspark.sql import SparkSession

def read_csv(file_name,schema):
    df=(spark.read.option("header",True).csv(file_name,schema=schema))
    return(df)

def convert_timestamp(df,col_ts):
    df=(df.withColumn(col_ts,f.to_timestamp(f.trim(f.col(col_ts)),"dd.MM.yyyy HH:mm")))
    return(df)
def convert_date(df,col_date):
    df=df.withColumn(col_date,f.to_date(f.trim(f.col(col_date)),"dd.MM.yyyy"))
    return(df)

def get_nse_id(claim_id):
    try:
        response=requests.get(f"https://api.hashify.net/hash/md4/hex?value={claim_id}")
        data = json.loads(response.text)
        return(data['Digest'])
    except Exception as e:
        return e

spark=SparkSession.\
    builder.\
    appName("Transactions").\
    getOrCreate()
    
print("Modules imported")