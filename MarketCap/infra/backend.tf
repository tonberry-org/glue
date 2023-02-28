terraform {
  backend "s3" {
    bucket         = "tonberry-terraform-backend"
    key            = "glue-market-cap-to-s3.tfstate"
    region         = "us-west-2"
    dynamodb_table = "terraform-state-lock"
  }
}
