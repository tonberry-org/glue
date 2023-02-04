resource "aws_glue_catalog_database" "bonds" {
  name = "bonds"
}

resource "aws_glue_catalog_table" "bonds" {
  name          = "bonds"
  database_name = aws_glue_catalog_database.bonds.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.bonds.arn
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

resource "aws_glue_registry" "bonds" {
  registry_name = "bonds"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "bonds"
  registry_arn      = aws_glue_registry.bonds.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/bonds.json")
}
