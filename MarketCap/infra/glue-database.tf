resource "aws_glue_catalog_database" "market-cap" {
  name = "market_cap"
}

resource "aws_glue_catalog_table" "market-cap" {
  name          = "market_cap"
  database_name = aws_glue_catalog_database.market-cap.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.market-cap.arn
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

resource "aws_glue_registry" "market-cap" {
  registry_name = "market_cap"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "market_cap"
  registry_arn      = aws_glue_registry.market-cap.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/market-cap.json")
}
