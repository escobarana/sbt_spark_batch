import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { readFileSync } from "fs";

const stack = pulumi.getStack()

const s3DeployBucket = new aws.s3.Bucket(`deployment-${stack}`, {
    forceDestroy: true
})

const s3DataBucket = new aws.s3.Bucket(`data-${stack}`, {
    forceDestroy: true
})

const sparkJobDownload = "https://github.com/escobarana/sbt_spark_batch/releases/download/"
const sparkJobVersion = "1.0.0"

const jarUrl = `${sparkJobDownload}/${sparkJobVersion}/spark-job-assembly-1.0.jar`

const jarKey = "jobs/spark-job/spark-job-assembly-1.0.jar"
const jarFile = new aws.s3.BucketObject("spark-job-assembly-1.0.jar", {
    key: jarKey,
    bucket: s3DeployBucket.id,
    source: new pulumi.asset.RemoteAsset(jarUrl),
    //kmsKeyId: examplekms.arn,
});

const csv1Url = `${sparkJobDownload}/${sparkJobVersion}/CO2_emission.csv`
const csv2Url = `${sparkJobDownload}/${sparkJobVersion}/CO2_emission_report1.csv`
const csv3Url = `${sparkJobDownload}/${sparkJobVersion}/CO2_emission_report2.csv`

const csvKey = "in/CO2.csv"
const csv1File = new aws.s3.BucketObject("CO2_emission.csv", {
    key: csvKey,
    bucket: s3DataBucket.id,
    source: new pulumi.asset.RemoteAsset(csv1Url),
    //kmsKeyId: examplekms.arn,
});

const csv2File = new aws.s3.BucketObject("CO2_emission_report1.csv", {
    key: csvKey,
    bucket: s3DataBucket.id,
    source: new pulumi.asset.RemoteAsset(csv2Url),
    //kmsKeyId: examplekms.arn,
});

const csv3File = new aws.s3.BucketObject("CO2_emission_report2.csv", {
    key: csvKey,
    bucket: s3DataBucket.id,
    source: new pulumi.asset.RemoteAsset(csv3Url),
    //kmsKeyId: examplekms.arn,
});

/*const s3StudioBucket = new aws.s3.Bucket("studio", {
    forceDestroy: true
})*/

/*const example = new aws.emr.Studio("emr-studio", {
    authMode: "IAM",
    defaultS3Location: `s3://${s3StudioBucket.bucket}/test`,
    engineSecurityGroupId: null, //aws_security_group.test.id,
    //serviceRole: aws_iam_role.test.arn,
    //subnetIds: [aws_subnet.test.id],
    //userRole: aws_iam_role.test.arn,
    //vpcId: aws_vpc.test.id,
   // workspaceSecurityGroupId: aws_security_group.test.id,
});*/


const batchProcessingApp = new aws.emrserverless.Application(`batch-processing-${stack}`, {
    name: "batch-processing",
    initialCapacities: [{
        initialCapacityConfig: {
            workerConfiguration: {
                cpu: "2 vCPU",
                memory: "8 GB",
            },
            workerCount: 1,
        },
        initialCapacityType: "Driver",
    }],
    maximumCapacity: {
        cpu: "4 vCPU",
        memory: "16 GB",
        disk: "70 GB"
    },
    releaseLabel: "emr-6.9.0",
    type: "spark",
});

const emrRole = new aws.iam.Role(`emrRole-${stack}`, {
    assumeRolePolicy: JSON.stringify({
        Version: "2012-10-17",
        Statement: [{
            Action: "sts:AssumeRole",
            Effect: "Allow",
            Sid: "",
            Principal: {
                Service: "emr-serverless.amazonaws.com",
            },
        }],
    }),
    inlinePolicies: [
        {
            name: 'fullAccessToData',
            policy: s3DataBucket.id.apply(id => JSON.stringify({
                Version: "2012-10-17",
                Statement: [
                    {
                        Sid: "FullAccessToData",
                        Effect: "Allow",
                        Action: [
                            "s3:PutObject",
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:DeleteObject"
                        ],
                        Resource: [
                            `arn:aws:s3:::${id}`,
                            `arn:aws:s3:::${id}/*`,
                        ]
                    }]}))
        },
        {
            name: 'readDeployment',
            policy: s3DeployBucket.id.apply(id => JSON.stringify({
                Version: "2012-10-17",
                Statement: [
                    {
                        Sid: "ReadDeployment",
                        Effect: "Allow",
                        Action: [
                            "s3:GetObject",
                            "s3:ListBucket"
                        ],
                        Resource: [
                            `arn:aws:s3:::${id}`,
                            `arn:aws:s3:::${id}/*`
                        ]
                    }]}))
        }],
    tags: {
        "tag-key": "tag-value",
    },
});

export const aws_region = "eu-west-1"
export const readme = readFileSync("./Pulumi.README.md").toString();

export const outputDeployBucket = s3DeployBucket.id
export const outputDataBucket = s3DataBucket.id

export const deployS3Console = outputDeployBucket.apply(id =>`https://us-east-1.console.aws.amazon.com/s3/buckets/${id}?region=.${aws_region}&tab=objects`)
export const dataS3Console = outputDataBucket.apply(id => `https://us-east-1.console.aws.amazon.com/s3/buckets/${id}?region=.${aws_region}&tab=objects`)

const emrstudioAppId = "spark-job"
export const appUrl = batchProcessingApp.id.apply(id => `https://${emrstudioAppId}.emrstudio-prod.${aws_region}.amazonaws.com/#/serverless-applications/${id}`)

export const scriptKey = jarFile.key
export const scriptBucket = jarFile.bucket

export const dataKey = csvFile.key
export const dataBucket = csvFile.bucket

export const role = emrRole.id