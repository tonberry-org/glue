
resource "aws_glue_crawler" "bonds" {
  database_name = aws_glue_catalog_database.bonds.name
  name          = "BondsCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 09 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.bonds.name
  }
}

