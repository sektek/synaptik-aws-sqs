import { Event, EventBuilder } from '@sektek/synaptik';
import { expect, use } from 'chai';
import { AbstractComponent } from '@sektek/utility-belt';
import { SendMessageCommandInput } from '@aws-sdk/client-sqs';
import chaiAsPromised from 'chai-as-promised';

import { SqsChannel } from './sqs-channel.js';

use(chaiAsPromised);

const QUEUE_URL = 'http://localhost:9324/000000000000/test-queue';
const FIFO_QUEUE_URL = 'http://localhost:9324/000000000000/test-queue.fifo';

class FakeTransport extends AbstractComponent {
  requests: SendMessageCommandInput[] = [];
  onSend: (request: SendMessageCommandInput) => void = () => {};

  async send(request: SendMessageCommandInput) {
    this.requests.push(request);
    this.onSend(request);
  }
}

describe('SqsChannel', function () {
  let event: Event;
  let requests: SendMessageCommandInput[];
  let transport: (request: SendMessageCommandInput) => Promise<void>;

  beforeEach(async function () {
    event = await new EventBuilder().create();
    requests = [];
    transport = async request => {
      requests.push(request);
    };
  });

  describe('request building', function () {
    it('should serialize the event as JSON by default', async function () {
      const channel = new SqsChannel({ queueUrl: () => QUEUE_URL, transport });

      await channel.send(event);

      expect(requests).to.have.length(1);
      expect(requests[0].QueueUrl).to.equal(QUEUE_URL);
      expect(JSON.parse(requests[0].MessageBody as string)).to.deep.equal(
        event,
      );
    });

    it('should use a custom serializer', async function () {
      const channel = new SqsChannel({
        queueUrl: () => QUEUE_URL,
        serializer: (e: Event) => `custom:${e.id}`,
        transport,
      });

      await channel.send(event);

      expect(requests[0].MessageBody).to.equal(`custom:${event.id}`);
    });

    it('should accept provider and transport instances', async function () {
      const fake = new FakeTransport();
      const channel = new SqsChannel({
        queueUrl: { get: () => QUEUE_URL },
        transport: fake,
      });

      await channel.send(event);

      expect(fake.requests).to.have.length(1);
      expect(fake.requests[0].QueueUrl).to.equal(QUEUE_URL);
    });

    it('should resolve the queue URL from the event', async function () {
      const channel = new SqsChannel({
        queueUrl: (e: Event) => `${QUEUE_URL}-${e.type}`,
        transport,
      });

      await channel.send(event);

      expect(requests[0].QueueUrl).to.equal(`${QUEUE_URL}-${event.type}`);
    });

    it('should omit optional fields by default', async function () {
      const channel = new SqsChannel({ queueUrl: () => QUEUE_URL, transport });

      await channel.send(event);

      expect(Object.keys(requests[0]).sort()).to.deep.equal([
        'MessageBody',
        'QueueUrl',
      ]);
    });

    it('should apply each optional provider', async function () {
      const seen: Event[] = [];
      const track = <R>(value: R) => {
        return (e: Event) => {
          seen.push(e);

          return value;
        };
      };
      const messageAttributes = {
        kind: { DataType: 'String', StringValue: 'test' },
      };
      const messageSystemAttributes = {
        AWSTraceHeader: { DataType: 'String', StringValue: 'Root=1-abc' },
      };
      const channel = new SqsChannel({
        queueUrl: () => QUEUE_URL,
        messageAttributes: track(messageAttributes),
        delaySeconds: track(7),
        messageDeduplicationId: track('dedup'),
        messageGroupId: track('group'),
        messageSystemAttributes: track(messageSystemAttributes),
        transport,
      });

      await channel.send(event);

      expect(seen).to.have.length(5);
      seen.forEach(e => expect(e).to.equal(event));
      expect(requests[0]).to.deep.include({
        MessageAttributes: messageAttributes,
        DelaySeconds: 7,
        MessageDeduplicationId: 'dedup',
        MessageGroupId: 'group',
        MessageSystemAttributes: messageSystemAttributes,
      });
    });
  });

  describe('FIFO queues', function () {
    it('should preserve the group and deduplication IDs', async function () {
      const channel = new SqsChannel({
        queueUrl: () => FIFO_QUEUE_URL,
        messageGroupId: (e: Event) => `group-${e.type}`,
        messageDeduplicationId: (e: Event) => e.id,
        transport,
      });

      await channel.send(event);

      expect(requests[0].MessageGroupId).to.equal(`group-${event.type}`);
      expect(requests[0].MessageDeduplicationId).to.equal(event.id);
    });

    it('should allow a missing deduplication ID', async function () {
      const channel = new SqsChannel({
        queueUrl: () => FIFO_QUEUE_URL,
        messageGroupId: () => 'group',
        transport,
      });

      await channel.send(event);

      expect(requests[0]).to.not.have.property('MessageDeduplicationId');
    });

    it('should reject a delay', async function () {
      const channel = new SqsChannel({
        queueUrl: () => FIFO_QUEUE_URL,
        messageGroupId: () => 'group',
        delaySeconds: () => 5,
        transport,
      });

      await expect(channel.send(event)).to.be.rejectedWith(/delaySeconds/);
      expect(requests).to.be.empty;
    });

    it('should reject a delay of zero', async function () {
      const channel = new SqsChannel({
        queueUrl: () => FIFO_QUEUE_URL,
        messageGroupId: () => 'group',
        delaySeconds: () => 0,
        transport,
      });

      await expect(channel.send(event)).to.be.rejectedWith(/delaySeconds/);
    });

    it('should require a message group ID', async function () {
      const channel = new SqsChannel({
        queueUrl: () => FIFO_QUEUE_URL,
        transport,
      });

      await expect(channel.send(event)).to.be.rejectedWith(/messageGroupId/);
      expect(requests).to.be.empty;
    });

    it('should allow a delay and no group ID on standard queues', async function () {
      const channel = new SqsChannel({
        queueUrl: () => QUEUE_URL,
        delaySeconds: () => 5,
        transport,
      });

      await channel.send(event);

      expect(requests[0].DelaySeconds).to.equal(5);
    });
  });

  describe('channel events', function () {
    it('should emit received and delivered events', async function () {
      const emitted: string[] = [];
      const channel = new SqsChannel({ queueUrl: () => QUEUE_URL, transport });
      channel.on('event:received', () => emitted.push('received'));
      channel.on('event:delivered', () => emitted.push('delivered'));

      await channel.send(event);

      expect(emitted).to.deep.equal(['received', 'delivered']);
    });

    it('should reject with a transport error and emit event:error', async function () {
      const error = new Error('transport failed');
      const errors: unknown[] = [];
      let delivered = false;
      const channel = new SqsChannel({
        queueUrl: () => QUEUE_URL,
        transport: async () => {
          throw error;
        },
      });
      channel.on('event:error', err => errors.push(err));
      channel.on('event:delivered', () => (delivered = true));

      await expect(channel.send(event)).to.be.rejectedWith(error);
      expect(errors).to.deep.equal([error]);
      expect(delivered).to.be.false;
    });

    it('should reject with a provider error and not call the transport', async function () {
      const error = new Error('no queue');
      const channel = new SqsChannel({
        queueUrl: () => {
          throw error;
        },
        transport,
      });

      await expect(channel.send(event)).to.be.rejectedWith(error);
      expect(requests).to.be.empty;
    });
  });

  describe('transport event forwarding', function () {
    const request: SendMessageCommandInput = {
      QueueUrl: QUEUE_URL,
      MessageBody: 'body',
    };
    let fake: FakeTransport;
    let channel: SqsChannel;
    let calls: { name: string; args: unknown[] }[];

    beforeEach(function () {
      fake = new FakeTransport();
      channel = new SqsChannel({ queueUrl: () => QUEUE_URL, transport: fake });
      calls = [];
      channel.on('message:sent', (...args) =>
        calls.push({ name: 'sent', args }),
      );
      channel.on('message:error', (...args) =>
        calls.push({ name: 'error', args }),
      );
      channel.on('message:queued', (...args) =>
        calls.push({ name: 'queued', args }),
      );
    });

    it('should forward message:sent with the same payload', function () {
      const result = { MessageId: 'id', $metadata: {} };

      fake.emit('message:sent', request, result);

      expect(calls).to.deep.equal([{ name: 'sent', args: [request, result] }]);
      expect(calls[0].args[0]).to.equal(request);
      expect(calls[0].args[1]).to.equal(result);
    });

    it('should forward message:error with the same payload', function () {
      const error = new Error('aws failed');

      fake.emit('message:error', error, request);

      expect(calls).to.deep.equal([{ name: 'error', args: [error, request] }]);
    });

    it('should forward message:queued without emitting message:sent', async function () {
      fake.onSend = req => fake.emit('message:queued', req);

      await channel.send(event);

      expect(calls.map(c => c.name)).to.deep.equal(['queued']);
      expect(calls[0].args[0]).to.have.property('MessageBody');
    });

    it('should emit message:sent only when the transport reports it', async function () {
      fake.onSend = req => {
        fake.emit('message:queued', req);
        fake.emit('message:sent', req, { $metadata: {} });
      };

      await channel.send(event);

      expect(calls.map(c => c.name)).to.deep.equal(['queued', 'sent']);
    });

    it('should not emit transport events for a function transport', async function () {
      const plain = new SqsChannel({
        queueUrl: () => QUEUE_URL,
        transport,
      });
      let emitted = false;
      plain.on('message:sent', () => (emitted = true));
      plain.on('message:queued', () => (emitted = true));

      await plain.send(event);

      expect(emitted).to.be.false;
    });
  });
});
