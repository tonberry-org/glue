


resource "aws_glue_job" "job" {
  name     = "MarketCap"
  role_arn = data.aws_iam_role.glue_general_purpose.arn

  execution_class   = "FLEX"
  number_of_workers = 4
  worker_type       = "G.1X"
  max_retries       = 0
  glue_version      = "3.0"

  timeout = 30

  default_arguments = {
    "--TempDir"                          = "s3://${data.aws_s3_bucket.assets.bucket}/temporary/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--job-language"                     = "python"
    "--spark-event-logs-path"            = "s3://${data.aws_s3_bucket.assets.bucket}/sparkHistoryLogs/"
  }
  command {
    script_location = "s3://${data.aws_s3_bucket.assets.bucket}/${aws_s3_object.script.key}"
  }
}

resource "aws_glue_trigger" "options_to_s3" {
  name     = "FivePastMidnightUTC"
  schedule = "cron(05 0 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.job.name
  }
}
