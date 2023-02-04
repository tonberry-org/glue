
resource "aws_glue_crawler" "eod_prices" {
  database_name = aws_glue_catalog_database.eod_prices.name
  name          = "NewsCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 09 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.eod_prices.name
  }
}

