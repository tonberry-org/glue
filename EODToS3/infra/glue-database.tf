resource "aws_glue_catalog_database" "news" {
  name = "news"
}

resource "aws_glue_catalog_table" "news" {
  name          = "news"
  database_name = aws_glue_catalog_database.news.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  storage_descriptor {
    location          = data.aws_dynamodb_table.news.arn
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

resource "aws_glue_registry" "news" {
  registry_name = "news"
}

resource "aws_glue_schema" "schema" {
  schema_name       = "news"
  registry_arn      = aws_glue_registry.news.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/news.json")
}
