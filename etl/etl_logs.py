#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row, IntegerType, StringType
import pyspark.sql.functions as F
import sys, os, re

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.project_config import project_config
from etl.base.etl import base_etl

spark = SparkSession.builder\
                    .appName('etl_log_analysis')\
                    .master('local[*]')\
                    .getOrCreate()

sc = spark.sparkContext

config = project_config.get_config()
input_path = config['DATA']['input_path']
output_path = config['DATA']['output_path']

def read_data(input_path):
    data = sc.textFile('{0}/*'.format(input_path))
    
    return data

def row_can_be_structured(row):
    contains_host_separator = '- -' in row
    contains_space_separators = len(re.findall(' ', row)) > 1
    match_timestamp = bool(re.search('\[.*\]', row))
    match_request = bool(re.search('".*"', row))
    
    result = contains_host_separator and contains_space_separators and match_timestamp and match_request
    
    return result

def structure_log(row):
    if not row_can_be_structured(row):
        return None
    
    match_timestamp = re.search('\[.*\]', row)
    tmstmp = match_timestamp.group(0)
    tmstmp = re.sub('\[|\]', '', tmstmp) # remove brackets from timestamp
    
    match_request = re.search('".*"', row)
    request = match_request.group(0)
    request = request.replace('"', '') # remove quotes from request
    
    result = {
        'host': row.split('- -')[0].strip(),
        'timestamp': tmstmp.strip(),
        'request': request.strip(),
        'status_code': row.split(' ')[-2].strip(),
        'total_bytes': row.split(' ')[-1].strip()
    }
    
    return result

def convert_dataframe(df):
    df = df.select(
        df.host,
        df.request,
        df.status_code.cast(IntegerType()).alias('status_code'),
        F.to_timestamp(df.timestamp, 'dd/MMM/yyyy:HH:mm:ss Z').alias('timestamp'),
        df.total_bytes.cast(IntegerType()).alias('total_bytes')
        )
    
    return df

def transform(rdd):
    structured_rdd = rdd.map(lambda x: structure_log(x))
    filtered_rdd = structured_rdd.filter(lambda x: x != None)
    df = filtered_rdd.toDF().cache()
    
    df = convert_dataframe(df)
    
    return df

def export_data(df, output_path):
    path = '{0}/server_logs'.format(output_path)
    df.write.mode('overwrite').parquet(path)

def run():
    # extract
    logs = read_data(input_path)
    
    # transform
    df = transform(logs)

    # load
    export_data(df, output_path)

if __name__ == '__main__':
    run()
