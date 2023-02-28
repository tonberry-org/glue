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
from pyspark.sql.functions import (
    concat_ws,
    col,
    coalesce,
    to_date,
    split,
    to_timestamp,
    year,
)


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

news_dynamodb_node: DynamicFrame = context.create_dynamic_frame.from_catalog(
    database="news",
    table_name="tonberry_news_raw",
    transformation_ctx="news_dynamodb_node",
)
transform_df = (
    news_dynamodb_node.toDF()
    .withColumn("tags", concat_ws(",", col("tags")))
    .na.fill("", "tags")
    .withColumn("symbols", concat_ws(",", col("symbols")))
    .na.fill("", "symbols")
    .withColumn(
        "neg_sentiment", coalesce(col("sentiment.neg.double"), col("sentiment.neg.int"))
    )
    .withColumn(
        "pos_sentiment", coalesce(col("sentiment.pos.double"), col("sentiment.pos.int"))
    )
    .withColumn(
        "neu_sentiment", coalesce(col("sentiment.neu.double"), col("sentiment.neu.int"))
    )
    .withColumn(
        "polarity_sentiment",
        coalesce(col("sentiment.polarity.double"), col("sentiment.polarity.int")),
    )
    .withColumn("date", to_timestamp(col("date")))
    .withColumn("year", year(col("date")))
    .drop("sentiment")
    .dropDuplicates(["link"])
    .repartition(10)
)

transform_frame = DynamicFrame.fromDF(transform_df, context, "transform_df")

resolved_frame = transform_frame.resolveChoice(
    specs=[
        ("neg_sentiment", "cast:double"),
        ("pos_sentiment", "cast:double"),
        ("neu_sentiment", "cast:double"),
        ("polarity_sentiment", "cast:double"),
    ],
    transformation_ctx="resolver_news_transform",
)


context.write_dynamic_frame.from_options(
    frame=transform_frame,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://tonberry-news",
        "compression": "gzip",
        "partitionKeys": ["year"],
    },
    transformation_ctx="S3bucket_news_datasink",
)

job.commit()
