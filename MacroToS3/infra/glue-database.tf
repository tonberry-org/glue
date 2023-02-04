resource "aws_glue_catalog_database" "macro_indicators" {
  name = "macro_indicators"
}

resource "aws_glue_catalog_table" "macro_indicators" {
  name          = "macro_indicators"
  database_name = aws_glue_catalog_database.macro_indicators.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.macro_indicators.arn
    number_of_buckets = -1

    ser_de_info {
      parameters = {}
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

resource "aws_glue_registry" "macro_indicators" {
  registry_name = "macro_indicators"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "macro_indicators"
  registry_arn      = aws_glue_registry.macro_indicators.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/macro_indicators.json")
}
