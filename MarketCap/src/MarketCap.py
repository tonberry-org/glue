import sys
from typing import Callable, Dict, Iterator, Tuple
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from pyspark.sql.types import Row
from pyspark.sql.functions import concat_ws, col, coalesce, to_date, split, year


def init() -> Tuple[GlueContext, Job]:
    params = []
    if "--JOB_NAME" in sys.argv:
        params.append("JOB_NAME")
    args = getResolvedOptions(sys.argv, params)

    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)

    if "JOB_NAME" in args:
        jobname = args["JOB_NAME"]
    else:
        jobname = "test"
    job.init(jobname, args)
    return (context, job)


context, job = init()
market_cap_dynamodb_node: DynamicFrame = context.create_dynamic_frame.from_catalog(
    database="market_cap",
    table_name="market_cap",
    transformation_ctx="market_cap_dynamodb_node",
)
resolved_frame = market_cap_dynamodb_node.resolveChoice(
    specs=[
        ("symbol", "cast:string"),
        ("value", "cast:long"),
    ]
)
df = resolved_frame.toDF().withColumn("date", to_date(col("date")))
df = df.withColumn("year", year(col("date")))
transform_node = DynamicFrame.fromDF(df, context, "transformed")

partitioned_dataframe: DynamicFrame = transform_node.toDF().repartition(1)
partitioned_dynamicframe: DynamicFrame = DynamicFrame.fromDF(
    partitioned_dataframe, context, "partitioned_df"
)

context.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://tonberry-market-cap-staging",
        "partitionKeys": ["year"],
        "compression": "gzip",
    },
    transformation_ctx="S3bucket_node3",
)
job.commit()
