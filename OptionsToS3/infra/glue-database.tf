resource "aws_glue_catalog_database" "quotesdb" {
  name = "quotesdb"
}

resource "aws_glue_catalog_table" "option_quote_history" {
  name          = "option_quote_history"
  database_name = aws_glue_catalog_database.quotesdb.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"

  #   parameters = {
  #     "CrawlerSchemaDeserializerVersion" = "1.0"
  #     "CrawlerSchemaSerializerVersion"   = "1.0"
  #     "UPDATED_BY_CRAWLER"               = "OptionHistoryQuotesCrawler"
  #     "classification"                   = "dynamodb"
  #     "compressionType"                  = "none"
  #     "hashKey"                          = "symbol"
  #     "rangeKey"                         = "timestamp"
  #     "typeOfData"                       = "table"
  #   }
  storage_descriptor {
    location          = data.aws_dynamodb_table.option_quote_history.arn
    number_of_buckets = -1
    # parameters = {
    #   "CrawlerSchemaDeserializerVersion" = "1.0"
    #   "CrawlerSchemaSerializerVersion"   = "1.0"
    #   "UPDATED_BY_CRAWLER"               = "OptionHistoryQuotesCrawler"
    #   "classification"                   = "dynamodb"
    #   "compressionType"                  = "none"
    #   "hashKey"                          = "symbol"
    #   "rangeKey"                         = "timestamp"
    #   "typeOfData"                       = "table"
    # }

    ser_de_info {
      parameters = {}
    }
    # columns {
    #   name = "totalvolume"
    #   type = "bigint"
    # }
    # columns {
    #   name = "underlying_id"
    #   type = "string"
    # }
    # columns {
    #   name = "symbol"
    #   type = "string"
    # }
    # columns {
    #   name = "openinterest"
    #   type = "bigint"
    # }
    # columns {
    #   name = "optiondeliverableslist"
    #   type = "string"
    # }
    # columns {
    #   name = "ismini"
    #   type = "string"
    # }
    # columns {
    #   name = "delta"
    #   type = "double"
    # }
    # columns {
    #   name = "openprice"
    #   type = "bigint"
    # }
    # columns {
    #   name = "description"
    #   type = "string"
    # }
    # columns {
    #   name = "volatility"
    #   type = "double"
    # }
    # columns {
    #   name = "timevalue"
    #   type = "double"
    # }
    # columns {
    #   name = "theta"
    #   type = "double"
    # }
    # columns {
    #   name = "markprice"
    #   type = "string"
    # }
    # columns {
    #   name = "lowprice"
    #   type = "double"
    # }
    # columns {
    #   name = "highprice"
    #   type = "double"
    # }
    # columns {
    #   name = "asksize"
    #   type = "bigint"
    # }
    # columns {
    #   name = "theoreticalvolatility"
    #   type = "bigint"
    # }
    # columns {
    #   name = "expirationdate"
    #   type = "string"
    # }
    # columns {
    #   name = "markpercentchange"
    #   type = "double"
    # }
    # columns {
    #   name = "timestamp"
    #   type = "string"
    # }
    # columns {
    #   name = "netchange"
    #   type = "double"
    # }
    # columns {
    #   name = "isindexoption"
    #   type = "string"
    # }
    # columns {
    #   name = "settlementtype"
    #   type = "string"
    # }
    # columns {
    #   name = "percentchange"
    #   type = "double"
    # }
    # columns {
    #   name = "expirationtype"
    #   type = "string"
    # }
    # columns {
    #   name = "multiplier"
    #   type = "bigint"
    # }
    # columns {
    #   name = "bidsize"
    #   type = "bigint"
    # }
    # columns {
    #   name = "strike"
    #   type = "string"
    # }
    # columns {
    #   name = "tradetimeinlong"
    #   type = "bigint"
    # }
    # columns {
    #   name = "quotetimeinlong"
    #   type = "bigint"
    # }
    # columns {
    #   name = "putcall"
    #   type = "string"
    # }
    # columns {
    #   name = "isnonstandard"
    #   type = "string"
    # }
    # columns {
    #   name = "markchange"
    #   type = "double"
    # }
    # columns {
    #   name = "lastsize"
    #   type = "bigint"
    # }
    # columns {
    #   name = "ask"
    #   type = "double"
    # }
    # columns {
    #   name = "rho"
    #   type = "double"
    # }
    # columns {
    #   name = "isinthemoney"
    #   type = "string"
    # }
    # columns {
    #   name = "expiration"
    #   type = "string"
    # }
    # columns {
    #   name = "deliverablenote"
    #   type = "string"
    # }
    # columns {
    #   name = "exchangename"
    #   type = "string"
    # }
    # columns {
    #   name = "closeprice"
    #   type = "double"
    # }
    # columns {
    #   name = "bid"
    #   type = "double"
    # }
    # columns {
    #   name = "theoreticaloptionvalue"
    #   type = "double"
    # }
    # columns {
    #   name = "gamma"
    #   type = "double"
    # }
    # columns {
    #   name = "vega"
    #   type = "double"
    # }
    # columns {
    #   name = "strikeprice"
    #   type = "double"
    # }
    # columns {
    #   name = "lastprice"
    #   type = "string"
    # }
  }

  lifecycle {
    ignore_changes = [
      parameters,
      storage_descriptor[0].parameters,
      storage_descriptor[0].columns
    ]
  }
}

resource "aws_glue_registry" "stocks" {
  registry_name = "stocks"
}

resource "aws_glue_schema" "option_underlying_quote_history" {
  schema_name       = "option_underlying_quote_history"
  registry_arn      = aws_glue_registry.stocks.arn
  data_format       = "JSON"
  compatibility     = "FULL_ALL"
  schema_definition = file("./schemas/option_underlying_quote_history.json")
}

resource "aws_glue_catalog_table" "option_underlying_quote_history" {
  name          = "option_underlying_quote_history"
  database_name = aws_glue_catalog_database.quotesdb.name
  table_type    = "EXTERNAL_TABLE"
  owner         = "owner"
  #   parameters = {
  #     "CrawlerSchemaDeserializerVersion" = "1.0"
  #     "CrawlerSchemaSerializerVersion"   = "1.0"
  #     "UPDATED_BY_CRAWLER"               = "OptionHistoryUnderlyingQuotes"
  #     "classification"                   = "dynamodb"
  #     "compressionType"                  = "none"
  #     "hashKey"                          = "id"
  #     "rangeKey"                         = "timestamp"
  #     "typeOfData"                       = "table"
  #   }
  storage_descriptor {
    location          = data.aws_dynamodb_table.option_underlying_quote_history.arn
    number_of_buckets = -1
    schema_reference {
      schema_id {
        registry_name = aws_glue_registry.stocks.registry_name
        schema_name   = aws_glue_schema.option_underlying_quote_history.schema_name

      }
      schema_version_number = "1.0"
    }
    # parameters = {
    #   "CrawlerSchemaDeserializerVersion" = "1.0"
    #   "CrawlerSchemaSerializerVersion"   = "1.0"
    #   "UPDATED_BY_CRAWLER"               = "OptionHistoryUnderlyingQuotes"
    #   "classification"                   = "dynamodb"
    #   "compressionType"                  = "none"
    #   "hashKey"                          = "id"
    #   "rangeKey"                         = "timestamp"
    #   "typeOfData"                       = "table"
    # }
    ser_de_info {
      parameters = {}
    }
    # columns {
    #   name = "date"
    #   type = "date"
    # }
    # columns {
    #   name = "totalvolume"
    #   type = "bigint"
    # }
    # columns {
    #   name = "symbol"
    #   type = "string"
    # }
    # columns {
    #   name = "openprice"
    #   type = "double"
    # }
    # columns {
    #   name = "description"
    #   type = "string"
    # }
    # columns {
    #   name = "delayed"
    #   type = "boolean"
    # }
    # columns {
    #   name = "lowprice"
    #   type = "double"
    # }
    # columns {
    #   name = "highprice"
    #   type = "double"
    # }
    # columns {
    #   name = "id"
    #   type = "string"
    # }
    # columns {
    #   name = "close"
    #   type = "double"
    # }
    # columns {
    #   name = "fiftytwoweekhigh"
    #   type = "double"
    # }
    # columns {
    #   name = "asksize"
    #   type = "bigint"
    # }
    # columns {
    #   name = "markpercentchange"
    #   type = "double"
    # }
    # columns {
    #   name = "timestamp"
    #   type = "string"
    # }
    # columns {
    #   name = "percentchange"
    #   type = "double"
    # }
    # columns {
    #   name = "last"
    #   type = "double"
    # }
    # columns {
    #   name = "bidsize"
    #   type = "bigint"
    # }
    # columns {
    #   name = "change"
    #   type = "double"
    # }
    # columns {
    #   name = "fiftytwoweeklow"
    #   type = "double"
    # }
    # columns {
    #   name = "tradetime"
    #   type = "bigint"
    # }
    # columns {
    #   name = "markchange"
    #   type = "double"
    # }
    # columns {
    #   name = "quotetime"
    #   type = "bigint"
    # }
    # columns {
    #   name = "ask"
    #   type = "double"
    # }
    # columns {
    #   name = "exchangename"
    #   type = "string"
    # }
    # columns {
    #   name = "bid"
    #   type = "double"
    # }
    # columns {
    #   name = "mark"
    #   type = "double"
    # }
  }

  lifecycle {
    ignore_changes = [
      parameters,
      storage_descriptor[0].parameters,
      storage_descriptor[0].columns
    ]
  }
}
