import {
  EventBridgeClient,
  PutEventsCommandInput,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';

type TransformedData = {
  [key: string]: string;
};

// Static initialization
const eventbridgeClient = new EventBridgeClient({
  region: process.env.AWS_REGION,
});

const putEventAsTransformed = async (transformedObject: TransformedData) => {
  try {
    // Building our data transformed event for EventBridge
    const putEventsCommandInput: PutEventsCommandInput = {
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

  await putEventAsTransformed(transformedObject);
};
