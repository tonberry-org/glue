
resource "aws_glue_crawler" "earnings" {
  database_name = aws_glue_catalog_database.earnings.name
  name          = "EarningsCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 09 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.earnings.name
  }
}

