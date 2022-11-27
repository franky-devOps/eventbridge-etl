import {
  EventBridgeClient,
  PutEventsCommandInput,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';
import {
  ECSClient,
  RunTaskCommand,
  RunTaskCommandInput,
} from '@aws-sdk/client-ecs';
import { SQSEvent } from 'aws-lambda';

const eventbridgeClient = new EventBridgeClient({
  region: process.env.AWS_REGION,
});

const runStandaloneEcsTask = async (runTaskCommand: RunTaskCommand) => {
  try {
    const ecsClient = new ECSClient({ region: process.env.AWS_REGION });
    const result = await ecsClient.send(runTaskCommand);

    return result;
  } catch (error) {
    console.log(error);
    throw new Error('Failed to run standalone ecs task');
  }
};

const putEventAsEcsStarted = async (ecsResponse: any) => {
  try {
    // Building our ecs started event for EventBridge
    const putEventsCommandInput: PutEventsCommandInput = {
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
    // Although this handler does not care who subs to the event
    // It is actually subscribed by the observer lambda
    const putEventsCommand = new PutEventsCommand(putEventsCommandInput);

    const result = await eventbridgeClient.send(putEventsCommand);

    console.log(result);
  } catch (error) {
    console.log(error);
    throw new Error('Failed to put event to event bridge');
  }
};

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

  const runTaskCommandInput: RunTaskCommandInput = {
    cluster: clusterName,
    launchType: 'FARGATE',
    taskDefinition: taskDefinition,
    count: 1,
    platformVersion: 'LATEST',
    networkConfiguration: {
      awsvpcConfiguration: {
        subnets: JSON.parse(subNets),
        // assignPublicIp: 'DISABLED',
        assignPublicIp: 'ENABLED',
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

    console.log('records ' + JSON.stringify(s3eventRecords));

    for (let idx in s3eventRecords) {
      let s3event = s3eventRecords[idx];
      console.log('s3 event ' + JSON.stringify(s3event));

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
        runTaskCommandInput.overrides = {
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
        const runTaskCommand = new RunTaskCommand(runTaskCommandInput);
        const ecsResponse = await runStandaloneEcsTask(runTaskCommand);

        await putEventAsEcsStarted(ecsResponse);

        console.log(ecsResponse);
      } else {
        console.log('not an s3 event...');
      }
    }
  }
};
