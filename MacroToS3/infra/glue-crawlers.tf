
resource "aws_glue_crawler" "macro_indicators" {
  database_name = aws_glue_catalog_database.macro_indicators.name
  name          = "MacroIndicatorsCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 09 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.macro_indicators.name
  }
}

