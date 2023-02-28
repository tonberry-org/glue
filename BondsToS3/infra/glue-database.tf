resource "aws_glue_catalog_database" "bonds" {
  name = "bonds"
}

resource "aws_glue_catalog_table" "tonberry_bonds_raw" {
  name          = "tonberry_bonds_raw"
  database_name = aws_glue_catalog_database.bonds.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"


  storage_descriptor {
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    location          = "s3://${data.aws_s3_bucket.raw.id}/"
    number_of_buckets = -1
    ser_de_info {
      parameters = {
        "paths" = "adjusted_close,bond,close,date,high,low,open,volume"

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
