import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as events_targets from 'aws-cdk-lib/aws-events-targets';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

export class EventbridgeEtlStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    /**
     * If left unchecked this pattern could "fan out" on the transform and load
     * lambdas to the point that it consumes all resources on the account. This is
     * why we are limiting concurrency to 2 on all 3 lambdas. Feel free to raise this.
     */
    const LAMBDA_THROTTLE_SIZE = 2;

    /**
     * DynamoDB Table
     * This is where our transformed data ends up
     */
    const table = new dynamodb.Table(this, 'TransformedData', {
      tableName: 'eventbridge-etl-address',
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
    });

    /**
     * S3 Landing Bucket
     * This is where the user uploads the file to be transformed
     */
    const landingBucket = new s3.Bucket(this, 'LandingBucket', {});

    /**
     * Queue that listens for S3 Bucket events
     */
    const landingBucketQueue = new sqs.Queue(
      this,
      'newObjectInLandingBucketEventQueue',
      {
        visibilityTimeout: Duration.seconds(300),
      }
    );

    landingBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SqsDestination(landingBucketQueue)
    );

    /**
     * EventBridge Permissions
     */
    let eventbridgePutPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: ['events:PutEvents'],
    });

    /**
     * Fargate ECS Task Creation to pull data from S3
     *
     * Fargate is used here because if you had a seriously large file,
     * you could stream the data to fargate for as long as needed before
     * putting the data onto eventbridge or up the memory/storage to
     * download the whole file. Lambda has limitations on runtime and
     * memory/storage
     */
    // const vpc = new ec2.Vpc(this, 'Vpc', {
    //   maxAzs: 2, // Default is all AZs in the region
    // });
    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVpc', {
      isDefault: true,
      vpcId: 'vpc-0732d0a34e98bf396',
    });

    const logging = new ecs.AwsLogDriver({
      streamPrefix: 'EventBridgeETL',
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    const cluster = new ecs.Cluster(this, 'Ec2Cluster', {
      vpc: vpc,
    });

    const taskDefinition = new ecs.FargateTaskDefinition(
      this,
      'FargateTaskDefinition',
      {
        memoryLimitMiB: 512,
        cpu: 256,
      }
    );

    // We need to give our fargate container permission to put events on our EventBridge
    taskDefinition.addToTaskRolePolicy(eventbridgePutPolicy);
    // Grant fargate container access to the object that was uploaded to s3
    landingBucket.grantRead(taskDefinition.taskRole);

    let container = taskDefinition.addContainer('AppContainer', {
      image: ecs.ContainerImage.fromAsset('container/s3DataExtractionTask'),
      logging,
      environment: {
        // clear text, not for sensitive data
        // Will be overriden in the extract Lambda function
        S3_BUCKET_NAME: landingBucket.bucketName,
        S3_OBJECT_KEY: '',
      },
    });

    /**
     * Lambdas
     *
     * These are used for 4 phases:
     *
     * Extract    - kicks of ecs fargate task to download data and splinter to eventbridge events
     * Transform  - takes the two comma separated strings and produces a json object
     * Load       - inserts the data into dynamodb
     * Observe    - This is a lambda that subscribes to all events and logs them centrally
     */

    /**
     * Extract
     */
    // defines an AWS Lambda resource to trigger our fargate ecs task
    const extractLambda = new lambda.Function(this, 'extractLambdaHandler', {
      runtime: lambda.Runtime.NODEJS_18_X,
      code: lambda.Code.fromAsset('lambda-fns/extract'),
      handler: 's3SqsEventConsumer.handler',
      reservedConcurrentExecutions: LAMBDA_THROTTLE_SIZE,
      environment: {
        CLUSTER_NAME: cluster.clusterName,
        TASK_DEFINITION: taskDefinition.taskDefinitionArn,
        SUBNETS: JSON.stringify(
          Array.from(vpc.publicSubnets, (x) => x.subnetId)
        ),
        CONTAINER_NAME: container.containerName,
      },
    });
    // Configure extract lambda to have permission to consume SQS message
    landingBucketQueue.grantConsumeMessages(extractLambda);
    extractLambda.addEventSource(new SqsEventSource(landingBucketQueue, {}));
    extractLambda.addToRolePolicy(eventbridgePutPolicy);

    const runTaskPolicyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['ecs:RunTask'],
      resources: [taskDefinition.taskDefinitionArn],
    });
    extractLambda.addToRolePolicy(runTaskPolicyStatement);

    // Allow the extract lambda to pass roles to the ecs task
    const taskExecutionRolePolicyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['iam:PassRole'],
      resources: [
        taskDefinition.obtainExecutionRole().roleArn,
        taskDefinition.taskRole.roleArn,
      ],
    });
    extractLambda.addToRolePolicy(taskExecutionRolePolicyStatement);

    /**
     * Transform
     */
    // defines a lambda to transform the data that was extracted from s3
    const transformLambda = new lambda.Function(
      this,
      'TransformLambdaHandler',
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        code: lambda.Code.fromAsset('lambda-fns/transform'),
        handler: 'transform.handler',
        reservedConcurrentExecutions: LAMBDA_THROTTLE_SIZE,
        // It should be a very fast execution
        timeout: Duration.seconds(3),
      }
    );
    transformLambda.addToRolePolicy(eventbridgePutPolicy);

    // Create EventBridge rule to route extraction events
    const transformRule = new events.Rule(this, 'transformRule', {
      description: 'Data extracted from S3, Needs transformed',
      eventPattern: {
        source: ['cdkpatterns.the-eventbridge-etl'],
        detailType: ['s3RecordExtraction'],
        detail: {
          status: ['extracted'],
        },
      },
    });

    transformRule.addTarget(new events_targets.LambdaFunction(transformLambda));

    /**
     * Load
     */
    // load the transformed data in dynamodb
    const loadLambda = new lambda.Function(this, 'LoadLambdaHandler', {
      runtime: lambda.Runtime.NODEJS_18_X,
      code: lambda.Code.fromAsset('lambda-fns/load'),
      handler: 'load.handler',
      // It should be a very fast execution
      timeout: Duration.seconds(3),
      reservedConcurrentExecutions: LAMBDA_THROTTLE_SIZE,
      environment: {
        TABLE_NAME: table.tableName,
      },
    });
    loadLambda.addToRolePolicy(eventbridgePutPolicy);
    table.grantReadWriteData(loadLambda);

    // Create EventBridge rule to route transform events
    const loadRule = new events.Rule(this, 'loadRule', {
      description: 'Data transformed, Needs loaded into dynamodb',
      eventPattern: {
        source: ['cdkpatterns.the-eventbridge-etl'],
        detailType: ['transform'],
        detail: {
          status: ['transformed'],
        },
      },
    });

    loadRule.addTarget(new events_targets.LambdaFunction(loadLambda));

    /**
     * Observe
     */
    // Watch for all cdkpatterns.the-eventbridge-etl events and log them centrally
    const observeLambda = new lambda.Function(this, 'ObserveLambdaHandler', {
      runtime: lambda.Runtime.NODEJS_18_X,
      code: lambda.Code.fromAsset('lambda-fns/observe'),
      handler: 'observe.handler',
      // It should be a very fast execution
      timeout: Duration.seconds(3),
    });

    // Create EventBridge rule to route events
    const observeRule = new events.Rule(this, 'observeRule', {
      description: 'all events are caught here and logged centrally',
      eventPattern: {
        source: ['cdkpatterns.the-eventbridge-etl'],
      },
    });

    observeRule.addTarget(new events_targets.LambdaFunction(observeLambda));
  }
}
