
resource "aws_glue_crawler" "option_quote_history" {
  database_name = aws_glue_catalog_database.quotesdb.name
  name          = "OptionHistoryQuotesCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 23 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.option_quote_history.name
  }
}

resource "aws_glue_crawler" "option_underlying_quote_history" {
  database_name = aws_glue_catalog_database.quotesdb.name
  name          = "OptionHistoryUnderlyingQuotes"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 23 * * ? *)"

  dynamodb_target {
    path = data.aws_dynamodb_table.option_underlying_quote_history.name
  }
}
