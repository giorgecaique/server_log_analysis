#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import re

spark = SparkSession.builder.appName('teste_semantix').getOrCreate()
sc = spark.sparkContext

data_path = '<input_data_dir>'
answer_filename = '<output_data_dir>/answer.txt'

def read_data():
    data = sc.textFile('{0}/*'.format(data_path)).cache()
    
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
        'host' : row.split('- -')[0].strip(),
        'timestamp' : tmstmp.strip(),
        'request' : request.strip(),
        'status_code' : row.split(' ')[-2].strip(),
        'total_bytes' : row.split(' ')[-1].strip()
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

def create_dataframe(rdd):
    structured_rdd = rdd.map(lambda x: structure_log(x))
    filtered_rdd = structured_rdd.filter(lambda x: x != None)
    df = filtered_rdd.toDF().cache()
    
    df = convert_dataframe(df)
    
    return df

def export_answer(answer):
    with open(answer_filename, 'w') as f:
        f.write(answer)

def main():
    logs = read_data()
    
    df = create_dataframe(logs)
    
    answer = ''
    
    # 1. Número total de hosts únicos

    unique_hosts = df.select('host').drop_duplicates().count()
    
    answer += '1. Número total de hosts únicos\n'
    answer += 'Answer: {0} hosts únicos\n'.format(unique_hosts)
    
    # 2. O​ ​total​ ​de​ ​erros​ ​404
    
    total_404_errors = df.where(df.status_code == 404).count()
    
    answer += '2. O total de erros 404\n'
    answer += 'Answer: {0} erros\n'.format(total_404_errors)
    
    
    # 3. As​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​ ​404
    
    urls_with_most_404_errors = df.where(df.status_code == 404)\
        .groupBy('host')\
        .agg(F.count('status_code').alias('count_errors_404'))\
        .orderBy(F.desc('count_errors_404'))\
        .limit(5)\
        .select('host')\
        .collect()
        
    urls_with_most_404_errors = [row['host'] for row in urls_with_most_404_errors]
        
    answer += '3. As 5 URLs que mais causaram erro 404\n'
    answer += 'Answer: {0}\n'.format(', '.join(urls_with_most_404_errors))
    
    # 4. Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia
    
    errors_per_day = df.where(df.status_code == 404)\
        .groupBy(F.dayofmonth('timestamp').alias('dia'))\
        .agg(F.count('status_code').alias('count_errors_404'))\
        .orderBy(F.asc('dia'))\
        .collect()
        
    errors_per_day = ['dia: {0}: {1} erros 404'.format(row['dia'], row['count_errors_404']) for row in errors_per_day]
    
    answer += '4. Quantidade de erros 404 por dia\n'
    answer += 'Answer: {0}\n'.format('\n'.join(errors_per_day))
    
    # 5. O​ ​total​ ​de​ ​bytes​ ​retornados
    
    total_bytes = df.select(F.sum(df.total_bytes).alias('total_bytes')).collect()
    total_bytes = total_bytes[0]['total_bytes']
    
    answer += '5. O total de bytes retornados\n'
    answer += 'Answer: {0} bytes'.format(total_bytes)
    
    export_answer(answer)
    
if __name__ == '__main__':
    main()