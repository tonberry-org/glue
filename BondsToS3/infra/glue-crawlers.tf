resource "aws_glue_crawler" "eod-prices-raw" {
  database_name = aws_glue_catalog_database.bonds.name
  name          = "${data.aws_s3_bucket.raw.bucket}-crawler"
  role          = data.aws_iam_role.glue_general_purpose.arn
  schedule      = "cron(00 10 * * ? *)"
  s3_target {
    path = "s3://${data.aws_s3_bucket.raw.id}"
  }

  classifiers = [aws_glue_classifier.bonds.name]

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

resource "aws_glue_classifier" "bonds" {
  name = "bonds"

  json_classifier {
    json_path = "$[*]"
  }
}
