# Lambda Debugging Wrapper
A wrapper around lambda to facilitate a better development experience.  

Using this library, you are able to run your lambda function code locally but still trigger it with real evente, ie. when a file is uploaded to an S3 bucket.  This avoids the need to build, package and deploy your lambda function every time you want to test it.

Currently supported triggers:
- S3NewFileTrigger

## Usage
Full example:

```javascript
import { startPollingS3 } from './lambda-debug-local';
import { handler } from './example-lambda';

console.log('Debugging example-lambda.ts');

startPollingS3({
    handler,
    bucketName: 'my-first-bucket',
    interval: 5000,
    region: 'eu-central-1',
    endpoint: 'http://localhost:4566',
    forcePathStyle: true,
    maxAge: 1000 * 60 * 5, // 5 minutes
    credentials: {
        accessKeyId: 'fake-access-key-id',
        secretAccessKey: 'fake-secret-access-key'
    }
})
```

Install using npm:
```bash
npm install -s lambda-debug-local
```

Import the startPollingS3 method 
```javascript
import { startPollingS3 } from './lambda-debug-local';
```

Call the startPollingS3 method with the following parameters:
```javascript

startPollingS3({
    handler,
    bucketName: 'my-first-bucket',
    interval: 5000,
    region: 'eu-central-1',
    endpoint: 'http://localhost:4566',
    forcePathStyle: true,
    maxAge: 1000 * 60 * 5, // 5 minutes
    credentials: {
        accessKeyId: 'fake-access-key-id',
        secretAccessKey: 'fake-secret-access-key'
    }
})
```

### Development: 

Please help me to improve this project by contributing to it. 
The main thing needed is more available triggers.

#### Getting started:

Required tools:
- Docker
- Node.js
- npm
- Localstack (brew install localstack/tap/localstack-cli)
- Make

npm install
make localstack-setup
npm run dev
