import { ECS, config, EventBridge } from 'aws-sdk';
import { SQSEvent } from 'aws-lambda';

config.region = process.env.AWS_REGION || 'us-east-1';
const eventbridge = new EventBridge();

const validateEnvVariables = (
  clusterName: string | undefined,
  taskDefinition: string | undefined,
  subNets: string | undefined,
  containerName: string | undefined
) => {
  if (typeof clusterName == 'undefined') {
    throw new Error('Cluster Name is not defined');
  }

  if (typeof taskDefinition == 'undefined') {
    throw new Error('Task Definition is not defined');
  }

  if (typeof subNets == 'undefined') {
    throw new Error('SubNets are not defined');
  }

  if (typeof containerName == 'undefined') {
    throw new Error('Container Name is not defined');
  }

  return { clusterName, taskDefinition, subNets, containerName };
};

export const handler = async (event: SQSEvent): Promise<any> => {
  var ecs = new ECS({ apiVersion: '2014-11-13' });

  console.log('request:', JSON.stringify(event, undefined, 2));

  //Extract variables from environment
  const { clusterName, taskDefinition, subNets, containerName } =
    validateEnvVariables(
      process.env.CLUSTER_NAME,
      process.env.TASK_DEFINITION,
      process.env.SUBNETS,
      process.env.CONTAINER_NAME
    );

  console.log('Cluster Name - ' + clusterName);
  console.log('Task Definition - ' + taskDefinition);
  console.log('SubNets - ' + subNets);

  var runeTaskRequestParams: ECS.Types.RunTaskRequest = {
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

  const records = event.Records;
  for (let index in records) {
    let payload = JSON.parse(records[index].body);
    console.log('processing s3 events ' + payload);

    let s3eventRecords = payload.Records;

    console.log('records ' + s3eventRecords);

    for (let idx in s3eventRecords) {
      let s3event = s3eventRecords[idx];
      console.log('s3 event ' + s3event);

      //Extract variables from event
      const objectKey = s3event?.s3?.object?.key;
      const bucketName = s3event?.s3?.bucket?.name;
      const bucketARN = s3event?.s3?.bucket?.arn;

      console.log('Object Key - ' + objectKey);
      console.log('Bucket Name - ' + bucketName);
      console.log('Bucket ARN - ' + bucketARN);

      // Overrwrite standalone ecs task with required environment variables
      if (
        typeof objectKey != 'undefined' &&
        typeof bucketName != 'undefined' &&
        typeof bucketARN != 'undefined'
      ) {
        runeTaskRequestParams.overrides = {
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

        // Run standalone task without ECS service schedule
        // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_run_task.html
        let ecsResponse = await ecs
          .runTask(runeTaskRequestParams)
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

        // Notify observer lambda
        const result = await eventbridge
          .putEvents(eventBridgeParams)
          .promise()
          .catch((error: any) => {
            throw new Error(error);
          });
        console.log(result);
      } else {
        console.log('not an s3 event...');
      }
    }
  }
};
