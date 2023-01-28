import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3

import pyspark.sql.functions as F
from pyspark.ml import Transformer

class DDBDelete:
    def __init__(self, table: str, keyGen):
        self.table = table
        self.keyGen = keyGen
        
    def process(self, df: DynamicFrame):
        df.toDF().foreachPartition(self.delete)
        
    def delete(self, rows):
        ddb_underlying_table = boto3.resource("dynamodb").Table(self.table)
        with ddb_underlying_table.batch_writer() as batch:
            for row in rows:
                batch.delete_item(Key=self.keyGen(row))


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Underlying DynamoDB
UnderlyingDynamoDB_node1673216732174 = glueContext.create_dynamic_frame.from_catalog(
    database="quotesdb",
    table_name="option_underlying_quote_history",
    transformation_ctx="UnderlyingDynamoDB_node1673216732174",
)

# Script generated for node Options DynamoDB
OptionsDynamoDB_node1673216729643 = glueContext.create_dynamic_frame.from_catalog(
    database="quotesdb",
    table_name="option_quote_history",
    transformation_ctx="OptionsDynamoDB_node1673216729643",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1673216792458 = ApplyMapping.apply(
    frame=UnderlyingDynamoDB_node1673216732174,
    mappings=[
        ("date", "date", "underlying_date", "date"),
        ("totalvolume", "long", "underlying_totalvolume", "long"),
        ("symbol", "string", "underlying_symbol", "string"),
        ("openprice", "double", "underlying_openprice", "double"),
        ("description", "string", "underlying_description", "string"),
        ("delayed", "boolean", "underlying_delayed", "boolean"),
        ("lowprice", "double", "underlying_low", "double"),
        ("highprice", "double", "underlying_high", "double"),
        ("id", "string", "underlying_id", "string"),
        ("close", "double", "underlying_close", "double"),
        ("fiftytwoweekhigh", "double", "underlying_fiftytwoweekhigh", "double"),
        ("asksize", "long", "underlying_asksize", "long"),
        ("markpercentchange", "double", "underlying_markpercentchange", "double"),
        ("timestamp", "string", "underlying_timestamp", "string"),
        ("percentchange", "double", "underlying_percentchange", "double"),
        ("last", "double", "underlying_last", "double"),
        ("bidsize", "long", "underlying_bidsize", "long"),
        ("change", "double", "underlying_change", "double"),
        ("fiftytwoweeklow", "double", "underlying_fiftytwoweeklow", "double"),
        ("tradetime", "long", "underlying_tradetime", "long"),
        ("markchange", "double", "underlying_markchange", "double"),
        ("quotetime", "long", "underlying_quotetime", "long"),
        ("ask", "double", "underlying_ask", "double"),
        ("exchangename", "string", "underlying_exchangename", "string"),
        ("bid", "double", "underlying_bid", "double"),
        ("mark", "double", "underlying_mark", "double"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1673216792458",
)

# Script generated for node Join
Join_node1673216776940 = Join.apply(
    frame1=OptionsDynamoDB_node1673216729643,
    frame2=RenamedkeysforJoin_node1673216792458,
    keys1=["underlying_id"],
    keys2=["underlying_id"],
    transformation_ctx="Join_node1673216776940",
)

Join_node1673216776940_resolved = Join_node1673216776940.resolveChoice(specs=[
    ("strikePrice", "cast:long"), 
    ("vega", "cast:double"),
    ("lowPrice", "cast:double"),
    ("theoreticalOptionValue", "cast:double"),
    ("percentChange", "cast:double"),
    ("volatility", "cast:double"),
    ("markPercentChange", "cast:double"),
    ("markChange", "cast:double"),
    ("netChange", "cast:double"),
    ("closePrice", "cast:double"),
    ("theta", "cast:double"),
    ("highPrice", "cast:double"),
    ("delta", "cast:double"),
    ("rho", "cast:double"),
    ("timeValue", "cast:double"),
    ("gamma", "cast:double"),
    ("ask", "cast:double"),
    ("bid", "cast:double"),
    ('underlying_markchange', "cast:double"),
    ('underlying_change', "cast:double"),
    ('underlying_ask', "cast:double"),
    ('underlying_bid', "cast:double"),
    ('underlying_markpercentchange', "cast:double"),
    ('underlying_percentchange', "cast:double"),
    ('underlying_markchange', "cast:double"),
    ('underlying_fiftytwoweekhigh', "cast:double"),
    ('underlying_fiftytwoweeklow', "cast:double"),
    ('underlying_totalvolume', "cast:long"),
    ('underlying_openprice', "cast:double"),
    ('underlying_low', "cast:double"),
    ('underlying_high', "cast:double"),
    ('underlying_close', "cast:double"),
    ('underlying_asksize', "cast:long"),
    ('underlying_last', "cast:double"),
    ('underlying_bidsize', "cast:long"),
    ('underlying_mark', "cast:double")
])

partitioned_dataframe = Join_node1673216776940_resolved.toDF().repartition(1)
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://tonberry-option-quotes-history-staging",
        "partitionKeys": ["underlying_symbol", "underlying_date"],
    },
    transformation_ctx="S3bucket_node3",
)

DDBDelete("option_underlying_quote_history", lambda x: {"id": x.id, "timestamp": x.timestamp}).process(UnderlyingDynamoDB_node1673216732174)
DDBDelete("option_quote_history", lambda x: {"symbol": x.symbol, "timestamp": x.timestamp }).process(OptionsDynamoDB_node1673216729643)


job.commit()
