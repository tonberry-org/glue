resource "aws_glue_catalog_database" "eod_prices" {
  name = "eod_prices"
}

resource "aws_glue_catalog_table" "eod_prices" {
  name          = "eod_prices"
  database_name = aws_glue_catalog_database.eod_prices.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.eod_prices.arn
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

resource "aws_glue_registry" "eod_prices" {
  registry_name = "eod_prices"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "eod_prices"
  registry_arn      = aws_glue_registry.eod_prices.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/eod_prices.json")
}
