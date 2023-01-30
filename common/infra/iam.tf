data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}


data "aws_iam_policy" "ddb" {
  arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

data "aws_iam_policy" "s3" {
  arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

data "aws_iam_policy" "glue" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue" {
  statement {
    actions = [
      "glue:*",
      "redshift:DescribeClusters",
      "redshift:DescribeClusterSubnetGroups",
      "iam:ListRoles",
      "iam:ListUsers",
      "iam:ListGroups",
      "iam:ListRolePolicies",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListAttachedRolePolicies",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcs",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeRouteTables",
      "ec2:DescribeVpcAttribute",
      "ec2:DescribeKeyPairs",
      "ec2:DescribeInstances",
      "rds:DescribeDBInstances",
      "rds:DescribeDBClusters",
      "rds:DescribeDBSubnetGroups",
      "s3:ListAllMyBuckets",
      "s3:ListBucket",
      "s3:GetBucketAcl",
      "s3:GetBucketLocation",
      "cloudformation:DescribeStacks",
      "cloudformation:GetTemplateSummary",
      "dynamodb:ListTables",
      "kms:ListAliases",
      "kms:DescribeKey",
      "cloudwatch:GetMetricData",
      "cloudwatch:ListDashboards",

    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      "arn:aws:s3:::*/*aws-glue-*/*",
      "arn:aws:s3:::aws-glue-*"
    ]
  }
  statement {
    actions   = ["tag:GetResources"]
    resources = ["*"]
  }
  statement {
    actions = [
      "s3:CreateBucket",
      "s3:PutBucketPublicAccessBlock"
    ]
    resources = ["arn:aws:s3:::aws-glue-*"]
  }
  statement {
    actions = [
      "logs:GetLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:/aws-glue/*"]
  }
  statement {
    actions = [

      "cloudformation:CreateStack",
      "cloudformation:DeleteStack"
    ]
    resources = ["arn:aws:cloudformation:*:*:stack/aws-glue*/*"]
  }
  statement {
    actions = [
      "ec2:RunInstances"
    ]
    resources = [
      "arn:aws:ec2:*:*:instance/*",
      "arn:aws:ec2:*:*:key-pair/*",
      "arn:aws:ec2:*:*:image/*",
      "arn:aws:ec2:*:*:security-group/*",
      "arn:aws:ec2:*:*:network-interface/*",
      "arn:aws:ec2:*:*:subnet/*",
      "arn:aws:ec2:*:*:volume/*"
    ]
  }

  statement {
    actions = [
      "ec2:TerminateInstances",
      "ec2:CreateTags",
      "ec2:DeleteTags"
    ]
    resources = ["arn:aws:ec2:*:*:instance/*"]
    condition {
      test     = "StringLike"
      variable = "ec2:ResourceTag/aws:cloudformation:stack-id"
      values   = ["arn:aws:cloudformation:*:*:stack/aws-glue-*/*"]
    }
    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/aws:cloudformation:logical-id"
      values   = ["ZeppelinInstance"]
    }
  }

  statement {
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::*:role/AWSGlueServiceRole*"]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }

  statement {
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::*:role/AWSGlueServiceNotebookRole*"]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com"]
    }
  }

  statement {
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::*:role/service-role/AWSGlueServiceRole*"]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "glue-general-purpose" {
  name   = "GlueGeneralPurpose"
  policy = data.aws_iam_policy_document.glue.json
}

resource "aws_iam_role" "glue-general-purpose" {
  name               = "AWSGlueServiceRole-GeneralPurpose"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  managed_policy_arns = [
    data.aws_iam_policy.ddb.arn,
    data.aws_iam_policy.s3.arn,
    data.aws_iam_policy.glue.arn,
    aws_iam_policy.glue-general-purpose.arn,
  ]

}
