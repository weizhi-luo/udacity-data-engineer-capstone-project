# convert_ecmwf_forecast_to_json
This is the code base of AWS Lambda function converting ECMWF forecast data from grib2 to the json format that can be imported to Redshift data warehouse.

The requirements.ext file includes packages required to implement the conversion. After downloading the packages, the application will become too big to deploy to Lambda by zip file and S3 upload.

One solution is to download [AWS base image](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html) of python and follow the [instructions](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#images-create-from-base) to create a docker image. Then upload the image to AWS [ECR](https://aws.amazon.com/ecr/). Finally, create the Lambda function using the uploaded image.