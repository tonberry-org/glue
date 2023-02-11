data "aws_s3_bucket" "assets" {
  bucket = "aws-glue-assets-536213556125-us-west-2"
}


resource "aws_s3_object" "script" {
  bucket = data.aws_s3_bucket.assets.bucket
  key    = "scripts/${local.project_name}.py"
  source = "../src/${local.project_name}.py"
  etag   = filemd5("../src/${local.project_name}.py")
}

data "aws_s3_bucket" "all" {
  for_each = toset(local.sources)
  bucket   = each.value
}

data "aws_s3_bucket" "news-yearly" {
  bucket = "tonberry-news-yearly"
}
