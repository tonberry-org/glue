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
from pyspark.sql.functions import concat_ws, col, coalesce, to_date, split


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

earnings_dynamodb_node: DynamicFrame = context.create_dynamic_frame.from_catalog(
    database="earnings",
    table_name="earnings",
    transformation_ctx="earnings_dynamodb_node",
)

resolved_frame = earnings_dynamodb_node.resolveChoice(
    specs=[
        ("estimate", "cast:double"),
        ("percent", "cast:double"),
        ("actual", "cast:double"),
        ("difference", "cast:double"),
        ("high", "cast:double"),
    ]
)

partitioned_dataframe: DynamicFrame = resolved_frame.toDF().repartition(1)

partitioned_dynamicframe: DynamicFrame = DynamicFrame.fromDF(
    partitioned_dataframe, context, "partitioned_df"
)

context.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://tonberry-earnings-staging",
        "partitionKeys": ["symbol"],
        "compression": "gzip",
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
