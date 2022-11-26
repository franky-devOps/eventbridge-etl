// const { ECS } = require('aws-sdk');
// const AWS = require('aws-sdk');
import AWS, { ECS } from 'aws-sdk';
import { S3CreateEvent } from 'aws-lambda';

AWS.config.region = process.env.AWS_REGION || 'us-east-1';
const eventbridge = new AWS.EventBridge();

export const handler = async (event: S3CreateEvent): Promise<any> => {
  var ecs = new ECS({ apiVersion: '2014-11-13' });

  console.log('request:', JSON.stringify(event, undefined, 2));

  //Extract variables from environment
  const clusterName = process.env.CLUSTER_NAME;
  if (typeof clusterName == 'undefined') {
    throw new Error('Cluster Name is not defined');
  }

  const taskDefinition = process.env.TASK_DEFINITION;
  if (typeof taskDefinition == 'undefined') {
    throw new Error('Task Definition is not defined');
  }

  const subNets = process.env.SUBNETS;
  if (typeof subNets == 'undefined') {
    throw new Error('SubNets are not defined');
  }

  const containerName = process.env.CONTAINER_NAME;
  if (typeof containerName == 'undefined') {
    throw new Error('Container Name is not defined');
  }

  console.log('Cluster Name - ' + clusterName);
  console.log('Task Definition - ' + taskDefinition);
  console.log('SubNets - ' + subNets);

  var params: any = {
    cluster: clusterName,
    launchType: 'FARGATE',
    taskDefinition: taskDefinition,
    count: 1,
    platformVersion: 'LATEST',
    networkConfiguration: {
      awsvpcConfiguration: {
        subnets: JSON.parse(subNets),
        assignPublicIp: 'DISABLED',
      },
    },
  };

  /**
   * An event can contain multiple records to process. i.e. the user could have uploaded 2 files.
   */
  let idx = 0;
  for (let record of event.Records) {
    console.log('processing s3 event record ' + record);

    //Extract variables from event
    const objectKey = record?.s3?.object?.key;
    const bucketName = record?.s3?.bucket?.name;
    const bucketARN = record?.s3?.bucket?.arn;

    console.log('Object Key - ' + objectKey);
    console.log('Bucket Name - ' + bucketName);
    console.log('Bucket ARN - ' + bucketARN);

    if (
      typeof objectKey != 'undefined' &&
      typeof bucketName != 'undefined' &&
      typeof bucketARN != 'undefined'
    ) {
      params.overrides = {
        containerOverrides: [
          {
            environment: [
              {
                name: 'S3_BUCKET_NAME',
                value: bucketName,
              },
              {
                name: 'S3_OBJECT_KEY',
                value: objectKey,
              },
            ],
            name: containerName,
          },
        ],
      };

      let ecsResponse = await ecs
        .runTask(params)
        .promise()
        .catch((error: any) => {
          throw new Error(error);
        });

      console.log(ecsResponse);

      // Building our ecs started event for EventBridge
      var eventBridgeParams = {
        Entries: [
          {
            DetailType: 'ecs-started',
            EventBusName: 'default',
            Source: 'cdkpatterns.the-eventbridge-etl',
            Time: new Date(),
            // Main event body
            Detail: JSON.stringify({
              status: 'success',
              data: ecsResponse,
            }),
          },
        ],
      };

      console.log(eventBridgeParams);

      // const result = await eventbridge
      //   .putEvents(eventBridgeParams)
      //   .promise()
      //   .catch((error: any) => {
      //     throw new Error(error);
      //   });
      // console.log(result);
    } else {
      console.log('not an s3 event...');
    }
    idx++;
  }
};
