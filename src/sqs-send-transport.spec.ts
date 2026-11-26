import {
  ReceiveMessageCommand,
  SendMessageCommandInput,
  SendMessageCommandOutput,
} from '@aws-sdk/client-sqs';
import { expect, use } from 'chai';
import { EventBuilder } from '@sektek/synaptik';
import chaiAsPromised from 'chai-as-promised';

import { ElasticMqQueues } from '../test/elasticmq-queues.js';
import { SqsChannel } from './sqs-channel.js';
import { SqsSendTransport } from './sqs-send-transport.js';

use(chaiAsPromised);

describe('SqsSendTransport', function () {
  let queues: ElasticMqQueues;
  let transport: SqsSendTransport;

  beforeEach(function () {
    queues = new ElasticMqQueues();
    transport = new SqsSendTransport({ client: () => queues.client });
  });

  afterEach(async function () {
    await queues.close();
  });

  async function receive(queueUrl: string) {
    const { Messages } = await queues.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        AttributeNames: ['All'],
        MessageAttributeNames: ['All'],
        WaitTimeSeconds: 2,
      }),
    );

    return Messages ?? [];
  }

  it('should deliver a message with its attributes to a standard queue', async function () {
    const queueUrl = await queues.createQueue();

    await transport.send({
      QueueUrl: queueUrl,
      MessageBody: 'hello',
      MessageAttributes: {
        kind: { DataType: 'String', StringValue: 'greeting' },
      },
    });

    const messages = await receive(queueUrl);

    expect(messages).to.have.length(1);
    expect(messages[0].Body).to.equal('hello');
    expect(messages[0].MessageAttributes?.kind?.StringValue).to.equal(
      'greeting',
    );
  });

  it('should send FIFO group and deduplication fields', async function () {
    const queueUrl = await queues.createQueue({ fifo: true });
    const request = {
      QueueUrl: queueUrl,
      MessageBody: 'first',
      MessageGroupId: 'group-1',
      MessageDeduplicationId: 'dedup-1',
    };

    await transport.send(request);
    await transport.send(request);

    const messages = await receive(queueUrl);

    expect(messages).to.have.length(1);
    expect(messages[0].Attributes?.MessageGroupId).to.equal('group-1');
  });

  it('should emit message:sent with the request and SDK result', async function () {
    const queueUrl = await queues.createQueue();
    const sent: [SendMessageCommandInput, SendMessageCommandOutput][] = [];
    let queued = 0;
    transport.on('message:sent', (request, result) =>
      sent.push([request, result]),
    );
    transport.on('message:queued', () => queued++);
    const request = { QueueUrl: queueUrl, MessageBody: 'hello' };

    await transport.send(request);

    expect(sent).to.have.length(1);
    expect(sent[0][0]).to.equal(request);
    expect(sent[0][1].MessageId).to.be.a('string');
    expect(sent[0][1].$metadata.httpStatusCode).to.equal(200);
    expect(queued).to.equal(0);
  });

  it('should reject and emit message:error when the queue is missing', async function () {
    const queueUrl = await queues.createQueue();
    const missing = `${queueUrl}-missing`;
    const errors: [unknown, SendMessageCommandInput][] = [];
    let sent = 0;
    transport.on('message:error', (error, request) =>
      errors.push([error, request]),
    );
    transport.on('message:sent', () => sent++);
    const request = { QueueUrl: missing, MessageBody: 'hello' };

    await expect(transport.send(request)).to.be.rejected;

    expect(errors).to.have.length(1);
    expect(errors[0][0]).to.be.instanceOf(Error);
    expect(errors[0][1]).to.equal(request);
    expect(sent).to.equal(0);
  });

  it('should work as the transport of an SqsChannel', async function () {
    const queueUrl = await queues.createQueue();
    const event = await new EventBuilder().create();
    const channel = new SqsChannel({ queueUrl: () => queueUrl, transport });
    const sent: SendMessageCommandInput[] = [];
    channel.on('message:sent', request => sent.push(request));

    await channel.send(event);

    const messages = await receive(queueUrl);

    expect(sent).to.have.length(1);
    expect(messages).to.have.length(1);
    expect(JSON.parse(messages[0].Body as string)).to.deep.equal(event);
  });
});
