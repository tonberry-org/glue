
resource "aws_glue_crawler" "economic_events" {
  database_name = aws_glue_catalog_database.economic_events.name
  name          = "NewsCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 09 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.economic_events.name
  }
}

