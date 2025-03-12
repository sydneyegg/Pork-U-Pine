import sys
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(os.path.abspath('/Workspace/Repos/EDP/edp-invent-databricks/notebooks/EDP/JDA/DC_SPLIT_INVENT/UTILS'))

from transformations import fn_parse_kafka_df

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit tests").getOrCreate()

@pytest.fixture
def setup_data(spark):
    # Sample data for df_delta and dc_split_schema
    t = "2024-07-05 08:46:36.02"
    data = [
        {'key': 'dc_split_str_file', 'value': '{"LOCATION_ID":"7188","WEEK":"202452","AVAIL_FOR_ALLOC":"Y","FORMAT":"LARGE","INSERT_DATE":"3-Jul-24","INSERT_ID":"Woods, Jaclyn","filename":"dc_store_mapping_without_headers.csv"}', 'topic': 'invent_files_nrt', 'partition': 0, 'offset': '10101', 'timestamp': t, 'processed_timestamp': t}, 
        {'key': 'dc_split_str_file', 'value': '{"LOCATION_ID":"7175","WEEK":"202452","AVAIL_FOR_ALLOC":"Y","FORMAT":"LARGE","INSERT_DATE":"3-Jul-24","INSERT_ID":"Woods, Jaclyn","filename":"dc_store_mapping_without_headers.csv"}','topic': 'invent_files_nrt', 'partition': 0, 'offset': '10102', 'timestamp': t, 'processed_timestamp': t} 
    ]
    schema = StructType([
            StructField("LOCATION_ID", StringType(), True),
            StructField("LIKE_LOCATION_ID", StringType(), True),
            StructField("SUPPLYING_DC", StringType(), True),
            StructField("WEEK", StringType(), True),
            StructField("AVAIL_FOR_ALLOC", StringType(), True),
            StructField("FORMAT", StringType(), True),
            StructField("INSERT_DATE", StringType(), True),
            StructField("INSERT_ID", StringType(), True)
    ])
    df_delta = spark.createDataFrame(data)
    dc_split_schema = schema  # Assuming schema is the same for simplicity
    return df_delta, dc_split_schema

def test_fn_parse_kafka_df(spark, setup_data):
    df_delta, dc_split_schema = setup_data
    df_stage = fn_parse_kafka_df(df_delta, dc_split_schema, "view_dc_split_stage")
    assert df_stage is not None
    assert df_stage.count() == len(df_delta.collect())

def test_df_pre_final(spark, setup_data):
    df_delta, dc_split_schema = setup_data
    fn_parse_kafka_df(df_delta, dc_split_schema, "view_dc_split_stage")
    df_pre_final = spark.sql("""
        SELECT
            LOCATION_ID AS LOCATION_NO,
            LIKE_LOCATION_ID AS LIKE_LOCATION_ID,
            SUPPLYING_DC AS DC_NO,
            WEEK AS FISCAL_YEAR_WEEK_NO,
            AVAIL_FOR_ALLOC AS AVAIL_FOR_ALLOCATION_IND,
            FORMAT AS FORMAT_NAME,
            coalesce(TO_TIMESTAMP(CAST(UNIX_TIMESTAMP(INSERT_DATE, 'dd-MMM-yy') AS TIMESTAMP)), now()) AS SRC_INSERT_DTTM,
            INSERT_ID AS SRC_INSERT_ID,
            'DC_STORE_ALLOCATION_FACT' AS INSERT_BATCH_ID,
            now() AS INSERT_LOAD_DT,
            'DC_STORE_ALLOCATION_FACT' AS UPDT_BATCH_ID,
            now() AS UPDT_LOAD_DT
        FROM
            (
                SELECT
                    LOCATION_ID,
                    LIKE_LOCATION_ID,
                    SUPPLYING_DC,
                    WEEK,
                    AVAIL_FOR_ALLOC,
                    FORMAT,
                    INSERT_DATE,
                    INSERT_ID,
                    row_number() over ( PARTITION BY LOCATION_ID, WEEK order by int(offset) desc ) as rnk
                FROM
                    view_dc_split_stage
                where
                    LOCATION_ID is not null
                    and WEEK is not null
            )
        WHERE
            rnk = 1
    """)
    assert df_pre_final is not None
    assert df_pre_final.count() == 2  # Based on the sample data