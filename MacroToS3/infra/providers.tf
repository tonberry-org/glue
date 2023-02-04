
provider "aws" {
  region              = "us-west-2"
  allowed_account_ids = ["536213556125"]

}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.52.0"
    }
  }
}
