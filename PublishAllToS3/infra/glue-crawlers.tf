
resource "aws_glue_crawler" "publish_all_s3" {
  for_each = data.aws_s3_bucket.all

  database_name = aws_glue_catalog_database.publish_all.name
  name          = "${each.value.bucket}-crawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 10 * * ? *)"
  s3_target {
    path = "s3://${each.value.id}"
  }

  configuration = jsonencode(
    {
      Grouping = {
        TableGroupingPolicy     = "CombineCompatibleSchemas",
        TableLevelConfiguration = 1
      }
      CrawlerOutput = {
        Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
        Tables = {
          AddOrUpdateBehavior = "MergeNewColumns",
          TableThreshold      = 5
        }
      }
      Version = 1
    }
  )
}


resource "aws_glue_crawler" "publish_all" {
  database_name = aws_glue_catalog_database.publish_all.name
  name          = "MarketCapIndicatorsCrawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 10 * * ? *)"
  dynamodb_target {
    path = data.aws_dynamodb_table.market_cap.name
  }

  configuration = jsonencode(
    {
      Grouping = {
        TableGroupingPolicy = "CombineCompatibleSchemas"
      }
      CrawlerOutput = {
        Tables = {
          AddOrUpdateBehavior = "MergeNewColumns",
          TableThreshold      = 5
        }
        Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      }
      Version = 1
    }
  )
}

