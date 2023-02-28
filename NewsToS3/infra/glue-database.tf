resource "aws_glue_catalog_database" "news" {
  name = "news"
}

resource "aws_glue_catalog_table" "tonberry_news_raw" {
  name          = "tonberry_news_raw"
  database_name = aws_glue_catalog_database.news.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"


  storage_descriptor {
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    location          = "s3://${data.aws_s3_bucket.raw.id}/"
    number_of_buckets = -1
    ser_de_info {
      parameters = {
        "paths" = "content,date,link,sentiment,symbols,tags,title"

      }
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }

  lifecycle {
    ignore_changes = [
      parameters,
      storage_descriptor[0].parameters,
      storage_descriptor[0].columns
    ]
  }
}


resource "aws_glue_registry" "news" {
  registry_name = "news"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "news-raw"
  registry_arn      = aws_glue_registry.news.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/news-raw.json")
}
