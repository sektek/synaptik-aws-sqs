import {
  DeleteMessageCommand,
  ListQueuesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { expect } from 'chai';

import { ElasticMqQueues } from './elasticmq-queues.js';

describe('ElasticMqQueues', function () {
  let queues: ElasticMqQueues;

  beforeEach(function () {
    queues = new ElasticMqQueues();
  });

  afterEach(async function () {
    await queues.close();
  });

  it('should round-trip a message through a standard queue', async function () {
    const QueueUrl = await queues.createQueue();

    await queues.client.send(
      new SendMessageCommand({ QueueUrl, MessageBody: 'hello' }),
    );

    const { Messages } = await queues.client.send(
      new ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 1 }),
    );

    expect(Messages).to.have.lengthOf(1);
    expect(Messages?.[0].Body).to.equal('hello');

    await queues.client.send(
      new DeleteMessageCommand({
        QueueUrl,
        ReceiptHandle: Messages?.[0].ReceiptHandle,
      }),
    );

    const empty = await queues.client.send(
      new ReceiveMessageCommand({ QueueUrl }),
    );

    expect(empty.Messages ?? []).to.have.lengthOf(0);
  });

  it('should create a FIFO queue', async function () {
    const QueueUrl = await queues.createQueue({ fifo: true });

    expect(QueueUrl).to.match(/\.fifo$/);

    await queues.client.send(
      new SendMessageCommand({
        QueueUrl,
        MessageBody: 'ordered',
        MessageGroupId: 'group',
        MessageDeduplicationId: 'one',
      }),
    );

    const { Messages } = await queues.client.send(
      new ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 1 }),
    );

    expect(Messages?.[0].Body).to.equal('ordered');
  });

  it('should delete the queues it created on close', async function () {
    const QueueUrl = await queues.createQueue();

    await queues.close();

    const other = new ElasticMqQueues();

    try {
      const { QueueUrls } = await other.client.send(new ListQueuesCommand({}));

      expect(QueueUrls ?? []).to.not.include(QueueUrl);
    } finally {
      await other.close();
    }
  });
});
