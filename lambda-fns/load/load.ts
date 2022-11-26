import { DynamoDB, EventBridge, config } from 'aws-sdk';

config.region = process.env.AWS_REGION || 'us-east-1';
const eventbridge = new EventBridge();

export const handler = async (event: any) => {
  console.log(JSON.stringify(event, null, 2));

  // Create the DynamoDB service object
  const dynamoDb = new DynamoDB({ apiVersion: '2012-08-10' });

  const putItemParams: DynamoDB.PutItemInput = {
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
  let result = await dynamoDb.putItem(putItemParams).promise();

  console.log(result);

  // Building our data loaded event for EventBridge
  var eventBridgeParams = {
    Entries: [
      {
        DetailType: 'data-loaded',
        EventBusName: 'default',
        Source: 'cdkpatterns.the-eventbridge-etl',
        Time: new Date(),
        // Main event body
        Detail: JSON.stringify({
          status: 'success',
          data: putItemParams,
        }),
      },
    ],
  };

  const ebResult = await eventbridge
    .putEvents(eventBridgeParams)
    .promise()
    .catch((error: any) => {
      throw new Error(error);
    });
  console.log(ebResult);
};
