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

class DDBDelete:
    def __init__(self, table: str, keyGen: Callable[[Row], Dict[str, str]]) -> None:
        self.table = table
        self.keyGen = keyGen
        
    def process(self, df: DynamicFrame) -> None:
        df.toDF().foreachPartition(self.delete)
        
    def delete(self, rows: Iterator[Row]) -> None:
        ddb_underlying_table = boto3.resource("dynamodb").Table(self.table)
        with ddb_underlying_table.batch_writer() as batch:
            for row in rows:
                batch.delete_item(Key=self.keyGen(row))

def init() -> Tuple[GlueContext, Job]:
    params = []
    if '--JOB_NAME' in sys.argv:
        params.append('JOB_NAME')
    args = getResolvedOptions(sys.argv, params)

    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)

    if 'JOB_NAME' in args:
        jobname = args['JOB_NAME']
    else:
        jobname = "test"
    job.init(jobname, args)
    return (context, job)

context, job = init()

bonds_dynamodb_node: DynamicFrame = context.create_dynamic_frame.from_catalog(
    database="bonds",
    table_name="bonds",
    transformation_ctx="bonds_dynamodb_node",
)


transform_node = bonds_dynamodb_node.toDF().withColumn('tags', concat_ws(',', col('tags')))
transform_node = transform_node.withColumn('symbols', concat_ws(',', col('symbols')))
transform_node = transform_node.withColumn('neg_sentiment', coalesce(col('sentiment.neg.long'), col('sentiment.neg.double')))
transform_node = transform_node.withColumn('pos_sentiment', coalesce(col('sentiment.pos.long'), col('sentiment.pos.double')))
transform_node = transform_node.withColumn('new_sentiment', coalesce(col('sentiment.neu.long'), col('sentiment.neu.double')))
transform_node = transform_node.withColumn('publish_timestamp',transform_node['date'])
transform_node = transform_node.withColumn('date', to_date(transform_node['date']))
transform_node = transform_node.withColumn('symbol', split(transform_node['symbol:link'], '#').getItem(0))

drop_node = DynamicFrame.fromDF(transform_node.drop('sentiment'), context, "drop_node")

partitioned_dataframe: DynamicFrame = drop_node.toDF().repartition(1)
partitioned_dynamicframe: DynamicFrame = DynamicFrame.fromDF(partitioned_dataframe, context, "partitioned_df")

context.write_dynamic_frame.from_options(
    frame=drop_node,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://tonberry-bonds-staging",
        "partitionKeys": ["symbol", "date"],
    },
    transformation_ctx="S3bucket_node3",
)

DDBDelete("bonds", lambda x: {"date": x['date'], "symbol:link": x['symbol:link'] }).process(bonds_dynamodb_node)

job.commit()

