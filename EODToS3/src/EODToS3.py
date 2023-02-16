import sys
from typing import Tuple
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, to_date, year


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


eod_prices_s3_frame = context.create_dynamic_frame.from_catalog(
    database="eod_prices",
    table_name="tonberry_eod_prices_raw",
    transformation_ctx="eod_prices_s3_frame",
)

resolved_frame = eod_prices_s3_frame.resolveChoice(specs=[
    ("open", "cast:double"),
    ("high", "cast:double"),
    ("low", "cast:double"),
    ("close", "cast:double"),
    ("adjusted_close", "cast:double"),
    ("volume", "cast:int"),
])

df = resolved_frame.toDF().withColumn('date', to_date(col('date'))).withColumn('year', year(col('date')))
to_date_frame =  DynamicFrame.fromDF(df, context, 'transformed')


partitioned_dataframe: DynamicFrame = to_date_frame.toDF().repartition(10)
partitioned_dynamicframe: DynamicFrame = DynamicFrame.fromDF(
    partitioned_dataframe, context, "partitioned_df"
)
context.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://tonberry-eod-prices",
        "partitionKeys": ["year"],
        "compression": "gzip",
    },
    transformation_ctx="S3bucket_node3",
)
job.commit()
