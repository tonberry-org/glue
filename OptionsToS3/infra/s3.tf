data "aws_s3_bucket" "assets" {
  bucket = "aws-glue-assets-536213556125-us-west-2"
}

resource "aws_s3_object" "script" {
  bucket = data.aws_s3_bucket.assets.bucket
  key    = "scripts/${local.project_name}.py"
  source = "../lib/${local.project_name}.py"
  etag   = filemd5("../${local.project_name}.py")
}
