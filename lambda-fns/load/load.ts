import {
  EventBridgeClient,
  PutEventsCommandInput,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';
import {
  DynamoDBClient,
  PutItemCommand,
  PutItemCommandInput,
} from '@aws-sdk/client-dynamodb';

const eventbridgeClient = new EventBridgeClient({
  region: process.env.AWS_REGION,
});

const dynamoDbClient = new DynamoDBClient({
  region: process.env.AWS_REGION,
});

const putItemToDynamoTable = async (putItemCommand: PutItemCommand) => {
  try {
    const result = await dynamoDbClient.send(putItemCommand);
    console.log(result);
  } catch (error) {
    console.log(error);
    throw new Error('Failed to put item to dynamoDB table');
  }
};

const putEventAsLoaded = async (putItemCommandInput: any) => {
  try {
    // Building our data loaded event for EventBridge
    const putEventsCommandInput: PutEventsCommandInput = {
      Entries: [
        {
          DetailType: 'data-loaded',
          EventBusName: 'default',
          Source: 'cdkpatterns.the-eventbridge-etl',
          Time: new Date(),
          // Main event body
          Detail: JSON.stringify({
            status: 'success',
            data: putItemCommandInput,
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

export const handler = async (event: any) => {
  console.log(JSON.stringify(event, null, 2));

  const putItemCommandInput: PutItemCommandInput = {
    TableName: process.env.TABLE_NAME as string,
    Item: {
      id: { S: event.detail.data.ID },
      house_number: { S: event.detail.data.HouseNum },
      street_address: { S: event.detail.data.Street },
      town: { S: event.detail.data.Town },
      zip: { S: event.detail.data.Zip },
    },
  };

  // Call DynamoDB to add the item to the table
  // let result = await dynamoDb.putItem(putItemParams).promise();
  const putItemCommand = new PutItemCommand(putItemCommandInput);
  await putItemToDynamoTable(putItemCommand);

  await putEventAsLoaded(putItemCommandInput);
};
