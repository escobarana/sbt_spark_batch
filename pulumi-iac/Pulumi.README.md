# Pulumi Service README
Sign in to AWS to view stack resources

## Configuration

Application URL: ${outputs.appUrl}

Deploy Bucket: [${outputs.outputDeployBucket}](${outputs.deployS3Console})

Data Bucket: [${outputs.outputDataBucket}](${outputs.dataS3Console})

## Run Job

Name: SparkJob

Runtime role: ${outputs.role}

Script location / S3 URI: s3://${outputs.scriptBucket}/${outputs.scriptKey}

Main class: App

Script arguments: ["s3://${outputs.dataBucket}/${outputs.dataKey}", "s3://${outputs.dataBucket}/out/csv"]

Spark Properties
* spark.driver.cores = 1
* spark.driver.memory = 1G
* spark.executor.cores = 1
* spark.executor.memory = 1G

