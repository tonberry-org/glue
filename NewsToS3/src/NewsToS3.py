import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import concat_ws, col, coalesce, to_timestamp, split

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df_transformed = (
        df.withColumn("tags", concat_ws(",", col("tags")))
        .na.fill("", "tags")
        .withColumn("symbols", concat_ws(",", col("symbols")))
        .na.fill("", "symbols")
        .withColumn(
            "neg_sentiment",
            coalesce(col("sentiment.neg.long"), col("sentiment.neg.double")),
        )
        .withColumn(
            "pos_sentiment",
            coalesce(col("sentiment.pos.long"), col("sentiment.pos.double")),
        )
        .withColumn(
            "new_sentiment",
            coalesce(col("sentiment.neu.long"), col("sentiment.neu.double")),
        )
        .withColumn("publish_timestamp", col("date"))
        .withColumn("date", to_timestamp(col("date")))
        .drop("sentiment")
    )
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "transform_news")
    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1676070376555 = glueContext.create_dynamic_frame.from_catalog(
    database="news",
    table_name="tonberry_news_raw",
    transformation_ctx="AmazonS3_node1676070376555",
)

# Script generated for node Custom Transform
CustomTransform_node1676011945331 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"AmazonS3_node1676070376555": AmazonS3_node1676070376555}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1676012328913 = SelectFromCollection.apply(
    dfc=CustomTransform_node1676011945331,
    key=list(CustomTransform_node1676011945331.keys())[0],
    transformation_ctx="SelectFromCollection_node1676012328913",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1676012328913,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://tonberry-news/test/", "partitionKeys": []},
    format_options={"compression": "gzip"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
