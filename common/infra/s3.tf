resource "aws_s3_bucket" "glue" {
  bucket = "tonberry-glue-scripts"
}

resource "aws_s3_bucket_acl" "glue" {
  bucket = aws_s3_bucket.glue.id
  acl    = "private"
}
