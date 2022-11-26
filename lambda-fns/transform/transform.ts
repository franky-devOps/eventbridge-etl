import { config, EventBridge } from 'aws-sdk';

config.region = process.env.AWS_REGION || 'us-east-1';
const eventbridge = new EventBridge();

type TransformedData = {
  [key: string]: string;
};

export const handler = async (event: any) => {
  console.log(JSON.stringify(event, null, 2));

  const headers: string = event.detail.headers;
  const data: string = event.detail.data;

  const headerArray = headers.split(',');
  const dataArray = data.split(',');
  const transformedObject: TransformedData = {};

  for (let index in headerArray) {
    const header = headerArray[index];
    const rowFieldValue = dataArray[index];
    transformedObject[header] = rowFieldValue;
  }

  // Building our transform event for EventBridge
  const params = {
    Entries: [
      {
        DetailType: 'transform',
        EventBusName: 'default',
        Source: 'cdkpatterns.the-eventbridge-etl',
        Time: new Date(),
        // Main event body
        Detail: JSON.stringify({
          status: 'transformed',
          data: transformedObject,
        }),
      },
    ],
  };

  const result = await eventbridge.putEvents(params).promise();
  console.log(result);
};
