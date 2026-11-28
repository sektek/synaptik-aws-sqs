import { expect, use } from 'chai';
import { Event } from '@sektek/synaptik';
import { Message } from '@aws-sdk/client-sqs';
import { ProcessingIterableProvider } from '@sektek/utility-belt';
import chaiAsPromised from 'chai-as-promised';

import { SqsEventProcessor } from './sqs-event-processor.js';

use(chaiAsPromised);

const event: Event = { id: 'evt-1', type: 'test', data: { a: 1 } };

const messageFor = (body: string | undefined, id = 'msg-1'): Message => ({
  MessageId: id,
  ReceiptHandle: `handle-${id}`,
  Body: body,
});

describe('SqsEventProcessor', function () {
  describe('default deserializer', function () {
    it('should parse the body as an Event', async function () {
      const processor = new SqsEventProcessor();

      expect(
        await processor.process(messageFor(JSON.stringify(event))),
      ).to.deep.equal(event);
    });

    it('should keep optional headers', async function () {
      const full: Event = {
        ...event,
        parentId: 'p-1',
        replyTo: ['a', 'b'],
      };
      const processor = new SqsEventProcessor();

      expect(
        await processor.process(messageFor(JSON.stringify(full))),
      ).to.deep.equal(full);
    });

    it('should reject bodies that are not valid Events with the message ID', async function () {
      const bodies = [
        '{not json',
        '42',
        'null',
        JSON.stringify({ type: 't', data: {} }),
        JSON.stringify({ id: '1', data: {} }),
        JSON.stringify({ id: '1', type: 't' }),
        JSON.stringify({ id: '1', type: 't', data: [] }),
        JSON.stringify({ id: '1', type: 't', data: {}, replyTo: 'x' }),
      ];
      const processor = new SqsEventProcessor();

      for (const body of bodies) {
        await expect(
          processor.process(messageFor(body, 'bad-1')),
        ).to.be.rejectedWith('bad-1');
      }
    });
  });

  describe('custom deserializer', function () {
    it('should convert the body with the provided function', async function () {
      const bodies: string[] = [];
      const processor = new SqsEventProcessor({
        deserializer: (body: string) => {
          bodies.push(body);

          return { id: body, type: 'custom', data: {} };
        },
      });

      const result = await processor.process(messageFor('raw-body'));

      expect(bodies).to.deep.equal(['raw-body']);
      expect(result).to.deep.equal({
        id: 'raw-body',
        type: 'custom',
        data: {},
      });
    });

    it('should accept an object with a get method', async function () {
      const processor = new SqsEventProcessor({
        deserializer: { get: async () => event },
      });

      expect(await processor.process(messageFor('x'))).to.equal(event);
    });

    it('should reject with the message ID when it fails', async function () {
      const processor = new SqsEventProcessor({
        deserializer: () => {
          throw new Error('boom');
        },
      });

      await expect(
        processor.process(messageFor('x', 'fail-1')),
      ).to.be.rejectedWith('fail-1');
    });
  });

  describe('missing body', function () {
    it('should reject with the message ID', async function () {
      const processor = new SqsEventProcessor();

      await expect(
        processor.process(messageFor(undefined, 'nobody-1')),
      ).to.be.rejectedWith('nobody-1');
    });
  });

  describe('getMessage', function () {
    it('should return the original message for the produced event', async function () {
      const processor = new SqsEventProcessor();
      const message = messageFor(JSON.stringify(event));
      const result = await processor.process(message);

      expect(processor.getMessage(result)).to.equal(message);
    });

    it('should return undefined for any other event', async function () {
      const processor = new SqsEventProcessor();

      await processor.process(messageFor(JSON.stringify(event)));

      expect(processor.getMessage({ ...event })).to.be.undefined;
    });

    it('should track each event to its own message', async function () {
      const processor = new SqsEventProcessor();
      const first = messageFor(JSON.stringify(event), 'm1');
      const second = messageFor(JSON.stringify(event), 'm2');
      const firstEvent = await processor.process(first);
      const secondEvent = await processor.process(second);

      expect(processor.getMessage(firstEvent)).to.equal(first);
      expect(processor.getMessage(secondEvent)).to.equal(second);
    });
  });

  describe('with ProcessingIterableProvider', function () {
    it('should process each message from the provider', async function () {
      const processor = new SqsEventProcessor();
      const messages = [
        messageFor(JSON.stringify({ ...event, id: 'e1' }), 'm1'),
        messageFor(JSON.stringify({ ...event, id: 'e2' }), 'm2'),
      ];
      const provider = new ProcessingIterableProvider({
        provider: {
          values: async function* () {
            yield* messages;
          },
        },
        processor,
      });
      const results: Event[] = [];

      for await (const result of provider.values()) {
        results.push(result);
      }

      expect(results.map(r => r.id)).to.deep.equal(['e1', 'e2']);
      expect(results.map(r => processor.getMessage(r))).to.deep.equal(messages);
    });
  });
});
