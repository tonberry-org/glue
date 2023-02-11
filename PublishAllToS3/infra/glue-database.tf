resource "aws_glue_catalog_database" "publish_all" {
  name = "publish_all"
}

# resource "aws_glue_catalog_table" "publish_all" {
#   name          = "publish_all"
#   database_name = aws_glue_catalog_database.publish_all.name
#   table_type    = "EXTERNAL_TABLE"
#   owner         = "owner"
#   storage_descriptor {
#     location          = data.aws_dynamodb_table.publish_all.arn
#     number_of_buckets = -1

#     ser_de_info {
#       parameters = {}
#     }
#   }

#   lifecycle {
#     ignore_changes = [
#       parameters,
#       storage_descriptor[0].parameters,
#       storage_descriptor[0].columns
#     ]
#   }
# }

# resource "aws_glue_registry" "publish_all" {
#   registry_name = "publish_all"
# }

# resource "aws_glue_schema" "schema" {
#   schema_name       = "publish_all"
#   registry_arn      = aws_glue_registry.publish_all.arn
#   data_format       = "JSON"
#   compatibility     = "FULL_ALL"
#   schema_definition = file("./schemas/publish_all.json")
# }
