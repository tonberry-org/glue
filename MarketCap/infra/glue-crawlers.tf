
resource "aws_glue_crawler" "market-cap" {
  database_name = aws_glue_catalog_database.market-cap.name
  name          = "MarketCapCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 09 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.market-cap.name
  }
}

