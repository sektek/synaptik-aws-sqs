import {
  Message,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import {
  ProcessManager,
  ProcessingIterableProvider,
} from '@sektek/utility-belt';
import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';

import { ElasticMqQueues } from '../test/elasticmq-queues.js';
import { SqsMessageProvider } from './sqs-message-provider.js';

use(chaiAsPromised);
use(sinonChai);

describe('SqsMessageProvider', function () {
  let queues: ElasticMqQueues;
  let queueUrl: string;
  let processManager: ProcessManager;

  const send = (body: string, attributes?: Record<string, string>) =>
    queues.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: body,
        MessageAttributes: attributes
          ? Object.fromEntries(
              Object.entries(attributes).map(([key, value]) => [
                key,
                { DataType: 'String', StringValue: value },
              ]),
            )
          : undefined,
      }),
    );

  const newProvider = (
    opts: Partial<ConstructorParameters<typeof SqsMessageProvider>[0]> = {},
  ) =>
    new SqsMessageProvider({
      client: () => queues.client,
      queueUrl,
      waitTimeSeconds: 1,
      processManager,
      ...opts,
    });

  before(function () {
    processManager = new ProcessManager({});
  });

  beforeEach(async function () {
    queues = new ElasticMqQueues();
    queueUrl = await queues.createQueue();
  });

  afterEach(async function () {
    await queues.close();
  });

  it('should return native messages with receipt handles and ids intact', async function () {
    const first = await send('one');
    const second = await send('two');

    const messages = await newProvider().values();

    expect(messages.map(m => m.Body)).to.have.members(['one', 'two']);
    expect(messages.map(m => m.MessageId)).to.have.members([
      first.MessageId,
      second.MessageId,
    ]);
    for (const message of messages) {
      expect(message.ReceiptHandle).to.be.a('string').that.is.not.empty;
    }
  });

  it('should return an empty array for an empty poll', async function () {
    expect(await newProvider().values()).to.deep.equal([]);
  });

  it('should emit message:received once per message', async function () {
    await send('one');
    await send('two');
    const provider = newProvider();
    const received: Message[] = [];
    provider.on('message:received', message => received.push(message));

    const messages = await provider.values();

    expect(received).to.have.lengthOf(2);
    expect(received).to.have.members(messages);
  });

  it('should not delete the messages it receives', async function () {
    await send('one');

    await newProvider({ visibilityTimeout: 1 }).values();
    await new Promise(resolve => setTimeout(resolve, 1500));
    const again = await newProvider().values();

    expect(again.map(m => m.Body)).to.deep.equal(['one']);
  });

  describe('request options', function () {
    let client: SQSClient;
    let sendSpy: sinon.SinonSpy;

    beforeEach(function () {
      client = queues.client;
      sendSpy = sinon.spy(client, 'send');
    });

    const lastInput = () => {
      const command = sendSpy.lastCall.args[0] as ReceiveMessageCommand;

      return command.input;
    };

    it('should send the default long-poll options', async function () {
      const provider = newProvider({ waitTimeSeconds: undefined });

      const pending = provider.values();
      await new Promise(resolve => setTimeout(resolve, 200));
      await provider.stop();
      await pending;

      expect(lastInput()).to.include({
        QueueUrl: queueUrl,
        WaitTimeSeconds: 20,
        MaxNumberOfMessages: 10,
      });
    });

    it('should honour the configured options', async function () {
      await newProvider({
        waitTimeSeconds: 1,
        maxNumberOfMessages: 3,
        visibilityTimeout: 45,
        messageAttributeNames: ['kind'],
        messageSystemAttributeNames: ['SentTimestamp'],
      }).values();

      expect(lastInput()).to.deep.include({
        WaitTimeSeconds: 1,
        MaxNumberOfMessages: 3,
        VisibilityTimeout: 45,
        MessageAttributeNames: ['kind'],
        MessageSystemAttributeNames: ['SentTimestamp'],
      });
    });

    it('should return the requested attributes', async function () {
      await send('one', { kind: 'greeting' });

      const [message] = await newProvider({
        messageAttributeNames: ['kind'],
        messageSystemAttributeNames: ['SentTimestamp'],
      }).values();

      expect(message.MessageAttributes?.kind?.StringValue).to.equal('greeting');
      expect(message.Attributes?.SentTimestamp).to.be.a('string');
    });

    it('should resolve a plain function client', async function () {
      const provider = new SqsMessageProvider({
        client: { get: () => client },
        queueUrl,
        waitTimeSeconds: 1,
        processManager,
      });

      expect(await provider.values()).to.deep.equal([]);
    });
  });

  describe('stop', function () {
    it('should abort an in-flight poll without an error', async function () {
      const provider = newProvider({ waitTimeSeconds: 20 });
      const errors: Error[] = [];
      provider.on('message:error', err => errors.push(err));

      const startedAt = Date.now();
      const pending = provider.values();
      await new Promise(resolve => setTimeout(resolve, 200));
      await provider.stop();

      expect(await pending).to.deep.equal([]);
      expect(Date.now() - startedAt).to.be.lessThan(5000);
      expect(errors).to.be.empty;
    });

    it('should emit service:stopped', async function () {
      const provider = newProvider();
      const stopped = sinon.fake();
      provider.on('service:stopped', stopped);

      await provider.stop();

      expect(stopped).to.have.been.calledOnce;
    });

    it('should return no messages while stopped and resume on start', async function () {
      await send('one');
      const provider = newProvider();
      const started = sinon.fake();
      provider.on('service:started', started);

      await provider.stop();
      expect(await provider.values()).to.deep.equal([]);

      await provider.start();
      const messages = await provider.values();

      expect(started).to.have.been.calledOnce;
      expect(messages.map(m => m.Body)).to.deep.equal(['one']);
    });
  });

  describe('retrieval errors', function () {
    it('should emit message:error and rethrow', async function () {
      const provider = newProvider({ queueUrl: `${queueUrl}-missing` });
      const errors: Error[] = [];
      provider.on('message:error', err => errors.push(err));

      const error = await provider.values().then(
        () => undefined,
        (err: unknown) => err,
      );

      expect(error).to.be.instanceOf(Error);
      expect(errors).to.deep.equal([error]);
    });

    it('should emit and rethrow when the client cannot be resolved', async function () {
      const failure = new Error('no client');
      const provider = newProvider({
        client: () => {
          throw failure;
        },
      });
      const errors: Error[] = [];
      provider.on('message:error', err => errors.push(err));

      await expect(provider.values()).to.be.rejectedWith(failure);
      expect(errors).to.deep.equal([failure]);
    });
  });

  it('should work as the values provider of a ProcessingIterableProvider', async function () {
    await send('one');
    await send('two');
    const processing = new ProcessingIterableProvider({
      provider: newProvider(),
      processor: (message: Message) => message.Body,
    });

    const bodies: (string | undefined)[] = [];
    for await (const body of processing.values()) {
      bodies.push(body);
    }

    expect(bodies).to.have.members(['one', 'two']);
  });
});
