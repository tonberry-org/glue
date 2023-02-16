resource "aws_glue_catalog_database" "eod_prices" {
  name = "eod_prices"
}


resource "aws_glue_catalog_table" "tonberry_eod_prices_raw" {
  name          = "tonberry_eod_prices_raw"
  database_name = aws_glue_catalog_database.eod_prices.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"


  storage_descriptor {
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    location          = "s3://${data.aws_s3_bucket.raw.id}/"
    number_of_buckets = -1
    ser_de_info {
      parameters = {
        "paths" = "adjusted_close,close,date,high,low,open,volume"

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


# resource "aws_glue_registry" "eod_prices" {
#   registry_name = "eod_prices"
# }

# resource "aws_glue_schema" "schema" {
#   schema_name       = "eod_prices"
#   registry_arn      = aws_glue_registry.eod_prices.arn
#   data_format       = "JSON"
#   compatibility     = "FULL_ALL"
#   schema_definition = file("./schemas/eod_prices.json")
# }
