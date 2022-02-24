# R on AWS Lambda

This repository creates a R runtime environment and function to work on AWS Lambda by deploying the container image on AWS ECR. The lambda function would take in the S3 "bucket name" and the "forecast_loc"(location code, e.g. 06 means location is California. Details about the forecast_loc could be found in covidhub) to pull the as of date hospitalization data and save it in the S3 bucket. 

This lambda function is trigerred using an AWS Eventbridge rule to pull data periodically.

For better understanding of how the runtime environment and deploying an R code on Lambda works check out this [blog post]((https://mdneuzerling.com/post/r-on-aws-lambda-with-containers/)) by MDNEUZERLING which helped me build this container image.