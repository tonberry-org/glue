resource "aws_glue_catalog_database" "earnings" {
  name = "earnings"
}

resource "aws_glue_catalog_table" "earnings" {
  name          = "earnings"
  database_name = aws_glue_catalog_database.earnings.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.earnings.arn
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

resource "aws_glue_registry" "earnings" {
  registry_name = "earnings"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "earnings"
  registry_arn      = aws_glue_registry.earnings.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/earnings.json")
}
