import { ProcessManager, sleep } from '@sektek/utility-belt';
import {
  ReceiveMessageCommand,
  SQSClient,
  SendMessageBatchCommand,
  SendMessageBatchCommandInput,
  SendMessageBatchCommandOutput,
  SendMessageCommandInput,
  SendMessageCommandOutput,
} from '@aws-sdk/client-sqs';
import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';

import { ElasticMqQueues } from '../test/elasticmq-queues.js';
import { SqsBatchTransport } from './sqs-batch-transport.js';
import { SqsBatchTransportOptions } from './types/index.js';

use(chaiAsPromised);

type BatchResponse = Partial<SendMessageBatchCommandOutput>;
type Handler = (
  input: SendMessageBatchCommandInput,
  signal: AbortSignal | undefined,
) => BatchResponse | Promise<BatchResponse>;

const succeed = (input: SendMessageBatchCommandInput): BatchResponse => ({
  Successful: (input.Entries ?? []).map(({ Id }) => ({
    Id,
    MessageId: `message-${Id}`,
    MD5OfMessageBody: 'md5',
  })),
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

const PENDING = Symbol('pending');

const isPending = async (promise: Promise<unknown>, ms = 40) => {
  const timer = async () => {
    await sleep(ms);

    return PENDING;
  };

  return (await Promise.race([promise, timer()])) === PENDING;
};

describe('SqsBatchTransport', function () {
  let queues: ElasticMqQueues;
  let queueUrl: string;
  let processManager: ProcessManager;
  let transports: SqsBatchTransport[];
  let sent: SendMessageCommandInput[];
  let results: SendMessageCommandOutput[];
  let errors: Array<{ error: unknown; request: SendMessageCommandInput }>;
  let queued: SendMessageCommandInput[];

  const message = (body: string, url = queueUrl): SendMessageCommandInput => ({
    QueueUrl: url,
    MessageBody: body,
  });

  const newTransport = (opts: Partial<SqsBatchTransportOptions> = {}) => {
    const transport = new SqsBatchTransport({
      client: () => queues.client,
      flushIntervalMs: 20,
      retryBackoffMs: 5,
      processManager,
      ...opts,
    });

    transport.on('message:sent', (request, result) => {
      sent.push(request);
      results.push(result);
    });
    transport.on('message:error', (error, request) =>
      errors.push({ error, request }),
    );
    transport.on('message:queued', request => queued.push(request));
    transports.push(transport);

    return transport;
  };

  const fakeClient = (handler: Handler) => {
    const calls: SendMessageBatchCommandInput[] = [];
    const client = {
      send: async (
        command: SendMessageBatchCommand,
        options?: { abortSignal?: AbortSignal },
      ) => {
        calls.push(command.input);

        return handler(command.input, options?.abortSignal);
      },
    } as unknown as SQSClient;

    return { calls, client: () => client };
  };

  const spyClient = () => {
    const calls: SendMessageBatchCommandInput[] = [];
    const client = {
      send: (
        command: SendMessageBatchCommand,
        options?: { abortSignal?: AbortSignal },
      ) => {
        calls.push(command.input);

        return queues.client.send(command, options);
      },
    } as unknown as SQSClient;

    return { calls, client: () => client };
  };

  const receiveAll = async (url = queueUrl) => {
    const bodies: string[] = [];

    for (;;) {
      const { Messages = [] } = await queues.client.send(
        new ReceiveMessageCommand({
          QueueUrl: url,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
        }),
      );

      if (Messages.length === 0) {
        return bodies;
      }

      bodies.push(...Messages.map(m => m.Body ?? ''));
    }
  };

  before(function () {
    processManager = new ProcessManager({});
  });

  beforeEach(async function () {
    queues = new ElasticMqQueues();
    queueUrl = await queues.createQueue();
    transports = [];
    sent = [];
    results = [];
    errors = [];
    queued = [];
  });

  afterEach(async function () {
    await Promise.allSettled(transports.map(transport => transport.stop()));
    await queues.close();
  });

  describe('batching', function () {
    it('should roll a batch over at maxBatchSize', async function () {
      const { calls, client } = spyClient();
      const transport = newTransport({ client, flushIntervalMs: 60_000 });

      for (let i = 0; i < 25; i++) {
        await transport.send(message(`m${i}`));
      }

      await waitFor(() => sent.length === 20);
      expect(calls.map(c => c.Entries?.length)).to.deep.equal([10, 10]);

      await transport.stop();

      expect(calls.map(c => c.Entries?.length)).to.deep.equal([10, 10, 5]);
      expect(await receiveAll()).to.have.lengthOf(25);
    });

    it('should honor a smaller maxBatchSize', async function () {
      const { calls, client } = spyClient();
      const transport = newTransport({
        client,
        maxBatchSize: 3,
        flushIntervalMs: 60_000,
      });

      for (let i = 0; i < 6; i++) {
        await transport.send(message(`m${i}`));
      }

      await waitFor(() => sent.length === 6);

      expect(calls.map(c => c.Entries?.length)).to.deep.equal([3, 3]);
    });

    it('should flush a partial batch after flushIntervalMs', async function () {
      const { calls, client } = spyClient();
      const transport = newTransport({ client, flushIntervalMs: 30 });

      await transport.send(message('a'));
      await transport.send(message('b'));
      await transport.send(message('c'));

      expect(sent).to.be.empty;

      await waitFor(() => sent.length === 3);

      expect(calls).to.have.lengthOf(1);
      expect(calls[0].Entries).to.have.lengthOf(3);
      expect((await receiveAll()).sort()).to.deep.equal(['a', 'b', 'c']);
    });

    it('should batch separately per queue URL', async function () {
      const otherUrl = await queues.createQueue();
      const { calls, client } = spyClient();
      const transport = newTransport({ client });

      await transport.send(message('a'));
      await transport.send(message('b', otherUrl));
      await waitFor(() => sent.length === 2);

      expect(calls.map(c => c.QueueUrl)).to.have.members([queueUrl, otherUrl]);
      expect(await receiveAll(queueUrl)).to.deep.equal(['a']);
      expect(await receiveAll(otherUrl)).to.deep.equal(['b']);
    });

    it('should send message options with the entry', async function () {
      const fifoUrl = await queues.createQueue({ fifo: true });
      const transport = newTransport();

      await transport.send({
        QueueUrl: fifoUrl,
        MessageBody: 'fifo',
        MessageGroupId: 'group',
        MessageDeduplicationId: 'dedup',
        MessageAttributes: { k: { DataType: 'String', StringValue: 'v' } },
      });
      await waitFor(() => sent.length === 1);

      const { Messages } = await queues.client.send(
        new ReceiveMessageCommand({
          QueueUrl: fifoUrl,
          MessageAttributeNames: ['All'],
          MessageSystemAttributeNames: ['MessageGroupId'],
          WaitTimeSeconds: 0,
        }),
      );

      expect(Messages?.[0].Body).to.equal('fifo');
      expect(Messages?.[0].Attributes?.MessageGroupId).to.equal('group');
      expect(Messages?.[0].MessageAttributes?.k.StringValue).to.equal('v');
    });

    it('should reject a request without a queue URL or body', async function () {
      const transport = newTransport();

      await expect(
        transport.send({ MessageBody: 'x' } as SendMessageCommandInput),
      ).to.be.rejectedWith(/QueueUrl/);
      await expect(
        transport.send({ QueueUrl: queueUrl } as SendMessageCommandInput),
      ).to.be.rejectedWith(/MessageBody/);
    });
  });

  describe('events', function () {
    it('should emit message:queued on send and message:sent only after AWS confirms', async function () {
      let release!: () => void;
      const gate = new Promise<void>(resolve => (release = resolve));
      const { client } = fakeClient(async input => {
        await gate;

        return succeed(input);
      });
      const transport = newTransport({ client, flushIntervalMs: 5 });
      const request = message('a');

      await transport.send(request);

      expect(queued).to.deep.equal([request]);

      await sleep(40);

      expect(sent).to.be.empty;

      release();
      await waitFor(() => sent.length === 1);

      expect(sent[0]).to.equal(request);
      expect(results[0].MessageId).to.equal('message-0');
      expect(results[0].$metadata).to.be.an('object');
    });

    it('should report the AWS result for each message', async function () {
      const transport = newTransport();

      await transport.send(message('a'));
      await transport.send(message('b'));
      await waitFor(() => sent.length === 2);

      expect(results.map(r => r.MessageId)).to.satisfy((ids: unknown[]) =>
        ids.every(id => typeof id === 'string' && id.length > 0),
      );
      expect(results[0].MD5OfMessageBody).to.be.a('string');
    });
  });

  describe('backpressure', function () {
    it('should block send at maxBatches until capacity frees up', async function () {
      let release!: () => void;
      const gate = new Promise<void>(resolve => (release = resolve));
      const { client } = fakeClient(async input => {
        await gate;

        return succeed(input);
      });
      const transport = newTransport({
        client,
        maxBatches: 1,
        maxBatchSize: 1,
        capacityWaitTimeoutMs: 5000,
      });

      await transport.send(message('a'));
      const second = transport.send(message('b'));

      expect(await isPending(second)).to.be.true;
      expect(queued).to.have.lengthOf(1);

      release();
      await second;

      expect(queued).to.have.lengthOf(2);
      await waitFor(() => sent.length === 2);
    });

    it('should count an open batch against maxBatches', async function () {
      const otherUrl = await queues.createQueue();
      const transport = newTransport({
        maxBatches: 1,
        flushIntervalMs: 30,
        capacityWaitTimeoutMs: 5000,
      });

      await transport.send(message('a'));
      const other = transport.send(message('b', otherUrl));

      expect(await isPending(other, 10)).to.be.true;

      await other;
      await waitFor(() => sent.length === 2);
    });

    it('should reject send after capacityWaitTimeoutMs', async function () {
      const { client } = fakeClient(() => untilAborted(undefined));
      const transport = newTransport({
        client,
        maxBatches: 1,
        maxBatchSize: 1,
        capacityWaitTimeoutMs: 30,
        stopTimeoutMs: 20,
      });

      await transport.send(message('a'));

      await expect(transport.send(message('b'))).to.be.rejectedWith(/capacity/);
      expect(queued).to.have.lengthOf(1);
    });

    it('should keep a retrying entry counted against capacity', async function () {
      let calls = 0;
      const { client } = fakeClient(input => {
        calls++;

        if (calls === 1) {
          return {
            Successful: [],
            Failed: [
              {
                Id: '0',
                Code: 'InternalError',
                Message: 'x',
                SenderFault: false,
              },
            ],
            $metadata: {},
          };
        }

        return succeed(input);
      });
      const transport = newTransport({
        client,
        maxBatches: 1,
        maxBatchSize: 1,
        retryBackoffMs: 40,
      });
      const order: string[] = [];

      transport.on('message:sent', request =>
        order.push(`sent:${request.MessageBody}`),
      );
      await transport.send(message('a'));
      await transport.send(message('b'));
      order.push('accepted:b');
      await waitFor(() => sent.length === 2);

      expect(order.indexOf('sent:a')).to.be.lessThan(
        order.indexOf('accepted:b'),
      );
    });
  });

  describe('failures and retries', function () {
    it('should retry only the failed entries', async function () {
      let calls = 0;
      const { calls: inputs, client } = fakeClient(input => {
        calls++;

        if (calls > 1) {
          return succeed(input);
        }

        return {
          Successful: [{ Id: '0', MessageId: 'ok', MD5OfMessageBody: 'md5' }],
          Failed: [
            {
              Id: '1',
              Code: 'ServiceUnavailable',
              Message: 'x',
              SenderFault: false,
            },
          ],
          $metadata: {},
        };
      });
      const transport = newTransport({ client, maxBatchSize: 2 });

      await transport.send(message('a'));
      await transport.send(message('b'));
      await waitFor(() => sent.length === 2);

      expect(sent.map(r => r.MessageBody)).to.deep.equal(['a', 'b']);
      expect(inputs).to.have.lengthOf(2);
      expect(inputs[1].Entries?.map(e => e.MessageBody)).to.deep.equal(['b']);
      expect(errors).to.be.empty;
    });

    it('should emit message:error for an entry that runs out of attempts', async function () {
      const { calls, client } = fakeClient(() => ({
        Successful: [],
        Failed: [
          {
            Id: '0',
            Code: 'ServiceUnavailable',
            Message: 'down',
            SenderFault: false,
          },
        ],
        $metadata: {},
      }));
      const transport = newTransport({ client, maxAttempts: 3 });
      const request = message('a');

      await transport.send(request);
      await waitFor(() => errors.length === 1);

      expect(calls).to.have.lengthOf(3);
      expect(errors[0].request).to.equal(request);
      expect((errors[0].error as Error).message).to.match(/ServiceUnavailable/);
      expect(sent).to.be.empty;
    });

    it('should fail a sender-fault entry at once without retrying', async function () {
      const { calls, client } = fakeClient(() => ({
        Successful: [],
        Failed: [
          {
            Id: '0',
            Code: 'InvalidParameterValue',
            Message: 'bad',
            SenderFault: true,
          },
        ],
        $metadata: {},
      }));
      const transport = newTransport({ client });

      await transport.send(message('a'));
      await waitFor(() => errors.length === 1);
      await sleep(30);

      expect(calls).to.have.lengthOf(1);
    });

    it('should retry a request that throws and then succeed', async function () {
      let calls = 0;
      const { client } = fakeClient(input => {
        if (++calls === 1) {
          throw new Error('socket hang up');
        }

        return succeed(input);
      });
      const transport = newTransport({ client });

      await transport.send(message('a'));
      await waitFor(() => sent.length === 1);

      expect(calls).to.equal(2);
      expect(errors).to.be.empty;
    });

    it('should not retry a non-retryable client error', async function () {
      const { calls, client } = fakeClient(() => {
        throw Object.assign(new Error('denied'), { $fault: 'client' });
      });
      const transport = newTransport({ client });

      await transport.send(message('a'));
      await waitFor(() => errors.length === 1);

      expect(calls).to.have.lengthOf(1);
      expect((errors[0].error as Error).message).to.equal('denied');
    });

    it('should treat an entry missing from the response as retryable', async function () {
      let calls = 0;
      const { client } = fakeClient(input => {
        if (++calls === 1) {
          return { Successful: [], Failed: [], $metadata: {} };
        }

        return succeed(input);
      });
      const transport = newTransport({ client });

      await transport.send(message('a'));
      await waitFor(() => sent.length === 1);

      expect(calls).to.equal(2);
    });

    it('should wait with exponentially growing backoff between attempts', async function () {
      const times: number[] = [];
      const { client } = fakeClient(() => {
        times.push(Date.now());

        throw new Error('fail');
      });
      const transport = newTransport({
        client,
        maxAttempts: 3,
        retryBackoffMs: 30,
      });

      await transport.send(message('a'));
      await waitFor(() => errors.length === 1);

      expect(times[1] - times[0]).to.be.at.least(25);
      expect(times[2] - times[1]).to.be.at.least(55);
    });
  });

  describe('timeouts', function () {
    it('should abort a request after requestTimeoutMs and count it as a failed attempt', async function () {
      const { calls, client } = fakeClient((_, signal) => untilAborted(signal));
      const transport = newTransport({
        client,
        requestTimeoutMs: 20,
        maxAttempts: 2,
      });

      await transport.send(message('a'));
      await waitFor(() => errors.length === 1);

      expect(calls).to.have.lengthOf(2);
      expect((errors[0].error as Error).message).to.match(
        /timed out after 20ms/,
      );
      expect(sent).to.be.empty;
    });
  });

  describe('stop', function () {
    it('should drain queued messages and emit service:stopped', async function () {
      const transport = newTransport({ flushIntervalMs: 60_000 });
      const events: string[] = [];

      transport.on('service:stopped', () => events.push('stopped'));
      await transport.send(message('a'));
      await transport.send(message('b'));

      await transport.stop();

      expect(sent).to.have.lengthOf(2);
      expect(events).to.deep.equal(['stopped']);
      expect((await receiveAll()).sort()).to.deep.equal(['a', 'b']);
    });

    it('should reject sends after stop and accept them again after start', async function () {
      const transport = newTransport();

      await transport.stop();

      await expect(transport.send(message('a'))).to.be.rejectedWith(/stopped/);

      await transport.start();
      await transport.send(message('b'));
      await waitFor(() => sent.length === 1);
    });

    it('should reject a send that is waiting for capacity when stopped', async function () {
      const { client } = fakeClient((_, signal) => untilAborted(signal));
      const transport = newTransport({
        client,
        maxBatches: 1,
        maxBatchSize: 1,
        stopTimeoutMs: 20,
      });

      await transport.send(message('a'));
      const waiting = transport.send(message('b'));
      const assertion = expect(waiting).to.be.rejectedWith(/stopped/);

      await expect(transport.stop()).to.be.rejected;
      await assertion;
    });

    it('should cover retries within the stop drain', async function () {
      let calls = 0;
      const { client } = fakeClient(input => {
        if (++calls < 3) {
          throw new Error('fail');
        }

        return succeed(input);
      });
      const transport = newTransport({
        client,
        flushIntervalMs: 60_000,
        retryBackoffMs: 10,
        stopTimeoutMs: 2000,
      });

      await transport.send(message('a'));
      await transport.stop();

      expect(calls).to.equal(3);
      expect(sent).to.have.lengthOf(1);
    });

    it('should emit service:stopError, abort and report undelivered messages on timeout', async function () {
      const { client } = fakeClient((_, signal) => untilAborted(signal));
      const transport = newTransport({
        client,
        flushIntervalMs: 60_000,
        stopTimeoutMs: 30,
        requestTimeoutMs: 60_000,
      });
      const stopErrors: unknown[] = [];
      const stopped: string[] = [];

      transport.on('service:stopError', error => stopErrors.push(error));
      transport.on('service:stopped', () => stopped.push('stopped'));
      await transport.send(message('a'));

      await expect(transport.stop()).to.be.rejectedWith(/did not drain/);
      await waitFor(() => errors.length === 1);

      expect(stopErrors).to.have.lengthOf(1);
      expect(stopped).to.be.empty;
      expect(errors[0].request.MessageBody).to.equal('a');
      expect(sent).to.be.empty;
    });

    it('should resolve stop on an idle transport', async function () {
      const transport = newTransport();
      const stopped: string[] = [];

      transport.on('service:stopped', () => stopped.push('stopped'));
      await transport.stop();

      expect(stopped).to.have.lengthOf(1);
    });
  });

  describe('options', function () {
    it('should reject a maxBatchSize above 10', function () {
      expect(() => newTransport({ maxBatchSize: 11 })).to.throw(RangeError);
    });

    it('should reject a non-positive option', function () {
      expect(() => newTransport({ maxBatches: 0 })).to.throw(RangeError);
      expect(() => newTransport({ stopTimeoutMs: -1 })).to.throw(RangeError);
    });
  });
});
