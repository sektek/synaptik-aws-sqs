import {
  CollectorComponent,
  ProcessManager,
  getComponent,
  sleep,
} from '@sektek/utility-belt';
import {
  DeleteMessageBatchCommand,
  DeleteMessageBatchCommandInput,
  DeleteMessageBatchCommandOutput,
  Message,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageBatchCommand,
} from '@aws-sdk/client-sqs';
import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';

import { ElasticMqQueues } from '../test/elasticmq-queues.js';
import { SqsBatchDeleteSettler } from './sqs-batch-delete-settler.js';
import { SqsBatchDeleteSettlerOptions } from './types/sqs-batch-delete-settler.js';

use(chaiAsPromised);

type BatchResponse = Partial<DeleteMessageBatchCommandOutput>;
type Handler = (
  input: DeleteMessageBatchCommandInput,
  signal: AbortSignal | undefined,
  call: number,
) => BatchResponse | Promise<BatchResponse>;

const succeed = (input: DeleteMessageBatchCommandInput): BatchResponse => ({
  Successful: (input.Entries ?? []).map(({ Id }) => ({ Id })),
  Failed: [],
  $metadata: {},
});

const untilAborted = (signal: AbortSignal | undefined) =>
  new Promise<BatchResponse>((_resolve, reject) => {
    signal?.addEventListener('abort', () => reject(new Error('aborted')));
  });

const waitFor = async (condition: () => boolean, timeoutMs = 3000) => {
  const deadline = Date.now() + timeoutMs;

  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('Timed out waiting for condition');
    }

    await sleep(2);
  }
};

describe('SqsBatchDeleteSettler', function () {
  let queues: ElasticMqQueues;
  let queueUrl: string;
  let processManager: ProcessManager;
  let settlers: SqsBatchDeleteSettler[];
  let deleted: Message[];
  let errors: Array<{ error: unknown; message: Message }>;

  const newSettler = (opts: Partial<SqsBatchDeleteSettlerOptions> = {}) => {
    const settler = new SqsBatchDeleteSettler({
      client: () => queues.client,
      queueUrl,
      visibilityTimeoutSeconds: 60,
      flushIntervalMs: 20,
      processManager,
      ...opts,
    });

    settler.on('message:deleted', message => deleted.push(message));
    settler.on('message:deleteError', (error, message) =>
      errors.push({ error, message }),
    );
    settlers.push(settler);

    return settler;
  };

  const fakeClient = (handler: Handler) => {
    const calls: DeleteMessageBatchCommandInput[] = [];
    const client = {
      send: async (
        command: DeleteMessageBatchCommand,
        options?: { abortSignal?: AbortSignal },
      ) => {
        calls.push(command.input);

        return handler(command.input, options?.abortSignal, calls.length);
      },
    } as unknown as SQSClient;

    return { calls, client: () => client };
  };

  const spyClient = () => {
    const calls: DeleteMessageBatchCommandInput[] = [];
    const client = {
      send: (
        command: DeleteMessageBatchCommand,
        options?: { abortSignal?: AbortSignal },
      ) => {
        calls.push(command.input);

        return queues.client.send(command, options);
      },
    } as unknown as SQSClient;

    return { calls, client: () => client };
  };

  const fakeMessages = (count: number): Message[] =>
    Array.from({ length: count }, (_, i) => ({
      MessageId: `id-${i}`,
      ReceiptHandle: `handle-${i}`,
    }));

  const receive = async (
    count: number,
    visibilityTimeout = 30,
  ): Promise<Message[]> => {
    const messages: Message[] = [];

    while (messages.length < count) {
      const { Messages = [] } = await queues.client.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          VisibilityTimeout: visibilityTimeout,
          WaitTimeSeconds: 0,
        }),
      );

      if (Messages.length === 0) {
        throw new Error('expected more messages');
      }

      messages.push(...Messages);
    }

    return messages;
  };

  const receiveSent = async (count: number, visibilityTimeout = 30) => {
    for (let start = 0; start < count; start += 10) {
      await queues.client.send(
        new SendMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: Array.from(
            { length: Math.min(10, count - start) },
            (_, i) => ({ Id: String(i), MessageBody: `body-${start + i}` }),
          ),
        }),
      );
    }

    return receive(count, visibilityTimeout);
  };

  before(function () {
    processManager = new ProcessManager({});
  });

  beforeEach(async function () {
    queues = new ElasticMqQueues();
    queueUrl = await queues.createQueue();
    settlers = [];
    deleted = [];
    errors = [];
  });

  afterEach(async function () {
    await Promise.allSettled(settlers.map(settler => settler.stop()));
    await queues.close();
  });

  describe('batching', function () {
    it('should delete in batches of at most 10 against ElasticMQ', async function () {
      const messages = await receiveSent(25);
      const { calls, client } = spyClient();
      const settler = newSettler({ client, flushIntervalMs: 60_000 });

      for (const message of messages) {
        await settler.add(message);
      }

      await waitFor(() => deleted.length === 20);
      expect(calls.map(c => c.Entries?.length)).to.deep.equal([10, 10]);

      await settler.stop();

      expect(calls.map(c => c.Entries?.length)).to.deep.equal([10, 10, 5]);
      expect(deleted).to.have.members(messages);
      expect(errors).to.be.empty;
    });

    it('should honor a smaller maxBatchSize', async function () {
      const { calls, client } = fakeClient(succeed);
      const settler = newSettler({
        client,
        maxBatchSize: 3,
        flushIntervalMs: 60_000,
      });

      for (const message of fakeMessages(6)) {
        await settler.add(message);
      }

      await waitFor(() => deleted.length === 6);
      expect(calls.map(c => c.Entries?.length)).to.deep.equal([3, 3]);
    });

    it('should flush a partial batch after flushIntervalMs', async function () {
      const messages = await receiveSent(3);
      const { calls, client } = spyClient();
      const settler = newSettler({ client, flushIntervalMs: 30 });

      for (const message of messages) {
        await settler.add(message);
      }

      expect(calls).to.be.empty;

      await waitFor(() => deleted.length === 3);
      expect(calls).to.have.lengthOf(1);
      expect(calls[0].Entries).to.have.lengthOf(3);
    });

    it('should resolve settle once the delete is queued', async function () {
      const { calls, client } = fakeClient(succeed);
      const settler = newSettler({ client, flushIntervalMs: 60_000 });

      await settler.add(fakeMessages(1)[0]);

      expect(calls).to.be.empty;
      expect(deleted).to.be.empty;
    });

    it('should flush early when the earliest deadline is near', async function () {
      const messages = await receiveSent(2, 5);
      const { calls, client } = spyClient();
      const settler = newSettler({
        client,
        flushIntervalMs: 60_000,
        visibilityTimeoutSeconds: 1,
        safetyMarginMs: 450,
      });

      await settler.add(messages[0]);
      await sleep(20);
      await settler.add(messages[1]);
      await sleep(30);

      expect(calls).to.be.empty;

      await waitFor(() => deleted.length === 2);
      expect(calls).to.have.lengthOf(1);
    });
  });

  describe('visibility timeout', function () {
    it('should not let a deleted message reappear', async function () {
      const [message] = await receiveSent(1, 1);
      const settler = newSettler({
        visibilityTimeoutSeconds: 1,
        safetyMarginMs: 500,
      });

      await settler.add(message);
      await waitFor(() => deleted.length === 1);
      await sleep(1200);

      const { Messages = [] } = await queues.client.send(
        new ReceiveMessageCommand({ QueueUrl: queueUrl, WaitTimeSeconds: 0 }),
      );

      expect(Messages).to.be.empty;
    });

    it('should report a late delete and not retry it', async function () {
      const { calls, client } = fakeClient(async () => {
        await sleep(150);

        return {
          Successful: [],
          Failed: [{ Id: '0', SenderFault: false, Code: 'Busy' }],
          $metadata: {},
        };
      });
      const settler = newSettler({
        client,
        visibilityTimeoutSeconds: 1,
        safetyMarginMs: 900,
      });
      const [message] = fakeMessages(1);

      await settler.add(message);
      await waitFor(() => errors.length === 1);
      await sleep(60);

      expect(calls).to.have.lengthOf(1);
      expect(errors[0].message).to.equal(message);
      expect(errors[0].error).to.be.instanceOf(Error);
      expect((errors[0].error as Error).message).to.match(/deadline/);
      expect(deleted).to.be.empty;
    });

    it('should report a delete that is late before it is sent', async function () {
      const { calls, client } = fakeClient(succeed);
      const settler = newSettler({
        client,
        flushIntervalMs: 60_000,
        visibilityTimeoutSeconds: 1,
        safetyMarginMs: 999,
      });

      await settler.add(fakeMessages(1)[0]);
      await sleep(20);
      await settler.stop();

      expect(errors).to.have.lengthOf(1);
      expect(calls).to.be.empty;
    });
  });

  describe('failures', function () {
    it('should report a sender fault per entry without retrying it', async function () {
      const { calls, client } = fakeClient(input => ({
        Successful: [{ Id: '0' }],
        Failed: (input.Entries ?? []).slice(1).map(({ Id }) => ({
          Id,
          SenderFault: true,
          Code: 'ReceiptHandleIsInvalid',
          Message: 'bad handle',
        })),
        $metadata: {},
      }));
      const settler = newSettler({ client });
      const messages = fakeMessages(3);

      for (const message of messages) {
        await settler.add(message);
      }

      await waitFor(() => errors.length === 2);
      await sleep(60);

      expect(calls).to.have.lengthOf(1);
      expect(deleted).to.deep.equal([messages[0]]);
      expect(errors.map(e => e.message)).to.deep.equal(messages.slice(1));
      expect((errors[0].error as Error).message).to.match(
        /ReceiptHandleIsInvalid/,
      );
    });

    it('should retry entries that failed for a reason other than the sender', async function () {
      const { calls, client } = fakeClient((input, _signal, call) =>
        call === 1
          ? {
              Successful: [{ Id: '0' }],
              Failed: [{ Id: '1', SenderFault: false, Code: 'Busy' }],
              $metadata: {},
            }
          : succeed(input),
      );
      const settler = newSettler({ client });
      const messages = fakeMessages(2);

      for (const message of messages) {
        await settler.add(message);
      }

      await waitFor(() => deleted.length === 2);

      expect(calls.map(c => c.Entries?.length)).to.deep.equal([2, 1]);
      expect(calls[1].Entries?.[0].ReceiptHandle).to.equal(
        messages[1].ReceiptHandle,
      );
      expect(errors).to.be.empty;
    });

    it('should retry entries the response omitted', async function () {
      const { calls, client } = fakeClient((input, _signal, call) =>
        call === 1
          ? { Successful: [], Failed: [], $metadata: {} }
          : succeed(input),
      );
      const settler = newSettler({ client });

      await settler.add(fakeMessages(1)[0]);
      await waitFor(() => deleted.length === 1);

      expect(calls).to.have.lengthOf(2);
    });

    it('should retry a request that timed out', async function () {
      const { calls, client } = fakeClient((input, signal, call) =>
        call === 1 ? untilAborted(signal) : succeed(input),
      );
      const settler = newSettler({ client, requestTimeoutMs: 20 });

      await settler.add(fakeMessages(1)[0]);
      await waitFor(() => deleted.length === 1);

      expect(calls).to.have.lengthOf(2);
      expect(errors).to.be.empty;
    });

    it('should report every entry of a request that failed permanently', async function () {
      const { calls, client } = fakeClient(() => {
        throw Object.assign(new Error('denied'), { $fault: 'client' });
      });
      const settler = newSettler({ client });

      for (const message of fakeMessages(2)) {
        await settler.add(message);
      }

      await waitFor(() => errors.length === 2);
      await sleep(60);

      expect(calls).to.have.lengthOf(1);
      expect(errors[0].error).to.have.property('message', 'denied');
    });

    it('should report the message when it has no receipt handle', async function () {
      const settler = newSettler();
      const message: Message = { MessageId: 'x' };

      await expect(settler.add(message)).to.be.rejectedWith(/ReceiptHandle/);

      expect(errors).to.have.lengthOf(1);
      expect(errors[0].message).to.equal(message);
    });

    it('should not change the outcome of settle when a delete fails later', async function () {
      const { client } = fakeClient(() => {
        throw Object.assign(new Error('denied'), { $fault: 'client' });
      });
      const settler = newSettler({ client });

      await expect(settler.add(fakeMessages(1)[0])).to.be.fulfilled;
    });
  });

  describe('stop', function () {
    it('should drain pending deletes and emit service:stopped', async function () {
      const messages = await receiveSent(3);
      const settler = newSettler({ flushIntervalMs: 60_000 });
      const events: string[] = [];

      settler.on('service:stopped', () => events.push('stopped'));

      for (const message of messages) {
        await settler.add(message);
      }

      await settler.stop();

      expect(deleted).to.have.members(messages);
      expect(events).to.deep.equal(['stopped']);
    });

    it('should cover retries within the stop drain', async function () {
      const { calls, client } = fakeClient((input, _signal, call) =>
        call === 1
          ? {
              Successful: [],
              Failed: [{ Id: '0', SenderFault: false, Code: 'Busy' }],
              $metadata: {},
            }
          : succeed(input),
      );
      const settler = newSettler({
        client,
        flushIntervalMs: 60_000,
        stopTimeoutMs: 2000,
      });

      await settler.add(fakeMessages(1)[0]);
      await settler.stop();

      expect(calls).to.have.lengthOf(2);
      expect(deleted).to.have.lengthOf(1);
    });

    it('should reject settle after stop and accept it again after start', async function () {
      const settler = newSettler();
      const started: string[] = [];

      settler.on('service:started', () => started.push('started'));
      await settler.stop();

      await expect(settler.add(fakeMessages(1)[0])).to.be.rejectedWith(
        /stopped/,
      );
      expect(errors).to.have.lengthOf(1);

      await settler.start();
      await settler.add(fakeMessages(1)[0]);
      await sleep(10);

      expect(started).to.have.lengthOf(1);
    });

    it('should emit service:stopError, abort and report undeleted messages on timeout', async function () {
      const { client } = fakeClient((_input, signal) => untilAborted(signal));
      const settler = newSettler({
        client,
        flushIntervalMs: 60_000,
        stopTimeoutMs: 30,
      });
      const stopErrors: unknown[] = [];
      const stopped: string[] = [];
      const messages = fakeMessages(2);

      settler.on('service:stopError', error => stopErrors.push(error));
      settler.on('service:stopped', () => stopped.push('stopped'));

      for (const message of messages) {
        await settler.add(message);
      }

      await expect(settler.stop()).to.be.rejectedWith(/did not drain/);
      await waitFor(() => errors.length === 2);

      expect(stopErrors).to.have.lengthOf(1);
      expect(stopped).to.be.empty;
      expect(errors.map(e => e.message)).to.have.members(messages);
    });

    it('should resolve stop on an idle settler', async function () {
      const settler = newSettler();
      const stopped: string[] = [];

      settler.on('service:stopped', () => stopped.push('stopped'));
      await settler.stop();

      expect(stopped).to.have.lengthOf(1);
    });
  });

  describe('options', function () {
    it('should reject invalid options', function () {
      expect(() => newSettler({ maxBatchSize: 11 })).to.throw(RangeError);
      expect(() => newSettler({ maxBatchSize: 0 })).to.throw(RangeError);
      expect(() => newSettler({ visibilityTimeoutSeconds: 0 })).to.throw(
        RangeError,
      );
      expect(() => newSettler({ flushIntervalMs: -1 })).to.throw(RangeError);
      expect(() => newSettler({ stopTimeoutMs: 0 })).to.throw(RangeError);
      expect(() => newSettler({ safetyMarginMs: -1 })).to.throw(RangeError);
      expect(() =>
        newSettler({ visibilityTimeoutSeconds: 1, safetyMarginMs: 1000 }),
      ).to.throw(RangeError);
    });
  });

  describe('as a component', function () {
    it('should be usable as an SqsSettlerComponent', async function () {
      const messages = await receiveSent(1);
      const component: CollectorComponent<Message> = newSettler();
      const settle = getComponent(component, 'add');

      await settle(messages[0]);
      await waitFor(() => deleted.length === 1);

      expect(deleted[0]).to.equal(messages[0]);
    });
  });
});
