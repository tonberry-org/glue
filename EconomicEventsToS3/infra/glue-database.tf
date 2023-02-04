resource "aws_glue_catalog_database" "economic_events" {
  name = "economic_events"
}

resource "aws_glue_catalog_table" "economic_events" {
  name          = "economic_events"
  database_name = aws_glue_catalog_database.economic_events.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.economic_events.arn
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

resource "aws_glue_registry" "economic_events" {
  registry_name = "economic_events"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "economic_events"
  registry_arn      = aws_glue_registry.economic_events.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/economic_events.json")
}
