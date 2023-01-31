data "aws_s3_bucket" "assets" {
  bucket = "aws-glue-assets-536213556125-us-west-2"
}

resource "aws_s3_object" "script" {
  bucket = data.aws_s3_bucket.assets.bucket
  key    = "scripts/${local.project_name}.py"
  source = "../src/${local.project_name}.py"
  etag   = filemd5("../src/${local.project_name}.py")
}


resource "aws_s3_bucket" "news_staging" {
  bucket = "tonberry-news-staging"
}
