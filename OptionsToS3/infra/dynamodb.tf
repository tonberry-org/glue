data "aws_dynamodb_table" "option_quote_history" {
  name = "option_quote_history"
}

data "aws_dynamodb_table" "option_underlying_quote_history" {
  name = "option_underlying_quote_history"
}

