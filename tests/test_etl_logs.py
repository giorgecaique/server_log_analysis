import pytest
from unittest import mock
from etl.etl_logs import etl_server_logs
from pyspark.sql.readwriter import DataFrameWriter
from pyspark import SparkContext

class test_etl_logs():
    def test_commons():
        assert callable(getattr(etl_server_logs,'read_data')) 
        assert callable(getattr(etl_server_logs,'transform')) 
        assert callable(getattr(etl_server_logs,'export_data')) 

    @mock.path.object(SparkContext, 'textFile')
    def test_read_data(text_file_patch):
        test_input_path = 'abc'

        etl_server_logs.read_data(test_input_path)

        assert text_file_patch.called

    def test_row_can_be_structured():
        test_row = 'in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839'
        expected_value = True

        assert etl_server_logs.row_can_be_structured(test_row) == expected_value

        test_row = 'abc dfnf 33232'
        expected_value = False

        assert etl_server_logs.row_can_be_structured(test_row) == expected_value

    @mock.patch.object(etl_server_logs, 'row_can_be_structured')
    def test_structure_log(row_can_be_structured_patch):
        test_row = 'in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839'

        expected_value = Row(
            host='in24.inetnebr.com',
            timestamp='01/Aug/1995:00:00:01 -0400',
            request='GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0',
            status_code='200',
            total_bytes='1839'
        )

        assert etl_server_logs.structure_log(test_row) == expected_value

    def test_convert_dataframe():
        test_data = Row(
            host='abc',
            request='abc',
            status_code='200',
            timestamp='01/Aug/1995:00:00:01 -0400',
            total_bytes='1000'
        )

        test_df = spark.createDataFrame(test_data)

        expected_data = Row(
            host='abc',
            request='abc',
            status_code=200,
            timestamp='01/Aug/1995:00:00:01 -0400',
            total_bytes=1000
        )

        expected_df = spark.createDataFrame(expected_data)
        expected_df = expected_df.withColumn('timestamp', F.to_timestamp(df.timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'))

        test_df = etl_server_logs.convert_dataframe(test_df) 

        assert expected_df.collect() == test_df.collect()

    @mock.pathc.object(DataFrameWriter, 'parquet')
    def test_export_data(parquet_patch):
        test_data = Row(col1='abc')
        test_df = spark.createDataFrame(test_data)

        test_output_path = 'abc'

        etl_server_logs.export_data(test_df, test_output_path)

        assert parquet_patch.called

    def test_transform():
        test_data = ['in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839']
        test_rdd = sc.parallelize(test_data)

        expected_data = Row(
            host='abc',
            request='abc',
            status_code=200,
            timestamp='01/Aug/1995:00:00:01 -0400',
            total_bytes=1000
        )

        expected_df = spark.createDataFrame(expected_data)
        expected_df = expected_df.withColumn('timestamp', F.to_timestamp(df.timestamp, 'dd/MMM/yyyy:HH:mm:ss Z'))

        assert transform(test_rdd).collect() == expected_df.collect()

    @mock.patch.object(etl_server_logs, 'read_data')
    @mock.patch.object(etl_server_logs, 'export_data')
    def test_run(read_data_patch, export_data_patch):
        mock_data = ['in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839']
        mock_rdd = sc.parallelize(mock_data)

        read_data_patch.return_value = mock_rdd

        etl_server_logs.run()

        assert read_data_patch.called
        assert export_data_patch.called
