import {
   SQSClient, ReceiveMessageCommand, DeleteMessageCommand
} from '@aws-sdk/client-sqs';
import { Context, SQSHandler } from 'aws-lambda';

const fakeContext: Context = {
    awsRequestId: 'fake-request-id',
    callbackWaitsForEmptyEventLoop: false,
    functionName: 'fake-function-name',
    functionVersion: 'fake-function-version',
    invokedFunctionArn: 'fake-invoked-function-arn',
    logGroupName: 'fake-log-group-name',
    logStreamName: 'fake-log-stream-name',
    memoryLimitInMB: 'fake-memory-limit-in-mb',
    done: () => {},
    fail: () => {},
    getRemainingTimeInMillis: () => 0,
    succeed: () => {}
};

const testHandler: SQSHandler = async (event: any, context: Context) => {
    console.log('sqs handler', event, context);
}

type CheckForMessageParams = {
    debugLogs?: boolean;
    queueUrl?: string;
    interval?: number;
    region?: string;
    endpoint?: string;
    maxAge?: number;
    credentials?: {
        accessKeyId: string;
        secretAccessKey: string;
    };
    handler?: SQSHandler;
};

export const checkForMessage = async ({
    debugLogs = false,
    queueUrl = 'http://localhost:4566/000000000000/my-test-queue',
    region = 'eu-central-1',
    endpoint= 'http://localhost:4566/',
    maxAge = 1000 * 60 * 5, // 5 minutes
    credentials = {
        accessKeyId: 'fake-access-key-id',
        secretAccessKey: 'fake-secret-access-key'
    },
    handler = testHandler
}: CheckForMessageParams = {}) => {
    try {
        if (debugLogs) console.log('Checking for new messages in SQS');
        const client = new SQSClient({ 
            region, 
            endpoint,
            credentials
        });
        const command = new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 5,
            WaitTimeSeconds: 5
        });
        const response = await client.send(command);
    
        if (!response.Messages || response.Messages.length === 0) {
            if (debugLogs) console.log('No messages found');
            return;
        }
    
        if (debugLogs) console.log(response);
    
        await Promise.all(response.Messages.map(async (message) => {
            try {
                if (debugLogs) console.log('New message:', message.Body);
    
                const sqsEvent = {
                    Records: [
                        {
                            messageId: 'fake-message-id',
                            receiptHandle: 'fake-receipt-handle',
                            body: message.Body,
                            attributes: {
                                ApproximateReceiveCount: '1',
                                SentTimestamp: Date.now().toString(),
                                SenderId: 'fake-sender-id',
                                approximateFirstReceiveTimestamp: Date.now().toString()
                            },
                            messageAttributes: {},
                            md5OfBody: 'fake-md5-of-body',
                            eventSource: 'aws:sqs',
                            eventSourceARN: 'fake-event-source-arn',
                            awsRegion: region
                        }
                    ]
                }
                
                const response = await handler(sqsEvent as any, fakeContext, () => {});
    
                if (response && response.batchItemFailures?.length > 0) {
                    console.error('Error processing message', response.batchItemFailures);
                } 
                
                const deleteCommand = new DeleteMessageCommand({
                    QueueUrl: queueUrl,
                    ReceiptHandle: message.ReceiptHandle
                });
                await client.send(deleteCommand);
                if (debugLogs) console.log('Message deleted', message.MessageId);

            } catch (error) {   
                console.error('Error processing object', error);
            }
        }));
    }
    catch (error) {
        console.error('Error checking for messages', error);
    }
}

export const startPollingSQS = (config: CheckForMessageParams) => {
    setInterval(async () => await checkForMessage(config), config.interval || 5000);
}