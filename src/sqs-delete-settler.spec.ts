import {
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import { fake } from 'sinon';
import { sleep } from '@sektek/utility-belt';

import { ElasticMqQueues } from '../test/elasticmq-queues.js';
import { SqsDeleteSettler } from './sqs-delete-settler.js';

use(chaiAsPromised);

describe('SqsDeleteSettler', function () {
  let queues: ElasticMqQueues;
  let queueUrl: string;

  const receive = async (): Promise<Message | undefined> => {
    const { Messages } = await queues.client.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 1 }),
    );

    return Messages?.[0];
  };

  const receiveOne = async (): Promise<Message> => {
    await queues.client.send(
      new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: 'hello' }),
    );
    const message = await receive();
    if (!message) {
      throw new Error('expected a message');
    }

    return message;
  };

  beforeEach(async function () {
    queues = new ElasticMqQueues();
    queueUrl = await queues.createQueue({
      attributes: { VisibilityTimeout: '1' },
    });
  });

  afterEach(async function () {
    await queues.close();
  });

  it('deletes the message so it does not reappear', async function () {
    const settler = new SqsDeleteSettler({
      client: () => queues.client,
      queueUrl,
    });
    const message = await receiveOne();

    await settler.add(message);
    await sleep(1500);

    expect(await receive()).to.be.undefined;
  });

  it('emits message:deleted once', async function () {
    const settler = new SqsDeleteSettler({
      client: () => queues.client,
      queueUrl,
    });
    const deleted = fake();
    const failed = fake();
    settler.on('message:deleted', deleted);
    settler.on('message:deleteError', failed);
    const message = await receiveOne();

    await settler.add(message);

    expect(deleted.calledOnceWithExactly(message)).to.be.true;
    expect(failed.called).to.be.false;
  });

  it('accepts a provider object for the client', async function () {
    const settler = new SqsDeleteSettler({
      client: { get: async () => queues.client },
      queueUrl,
    });
    const deleted = fake();
    settler.on('message:deleted', deleted);

    await settler.add(await receiveOne());

    expect(deleted.calledOnce).to.be.true;
  });

  it('emits message:deleteError and rejects when deletion fails', async function () {
    const settler = new SqsDeleteSettler({
      client: () => queues.client,
      queueUrl,
    });
    const deleted = fake();
    const failed = fake();
    settler.on('message:deleted', deleted);
    settler.on('message:deleteError', failed);
    const message: Message = { ...(await receiveOne()), ReceiptHandle: 'bad' };

    await expect(settler.add(message)).to.be.rejected;

    expect(deleted.called).to.be.false;
    expect(failed.calledOnce).to.be.true;
    expect(failed.firstCall.args[0]).to.be.instanceOf(Error);
    expect(failed.firstCall.args[1]).to.equal(message);
  });

  it('emits message:deleteError when the client provider fails', async function () {
    const error = new Error('no client');
    const settler = new SqsDeleteSettler({
      client: () => {
        throw error;
      },
      queueUrl,
    });
    const failed = fake();
    settler.on('message:deleteError', failed);
    const message: Message = { ReceiptHandle: 'x' };

    await expect(settler.add(message)).to.be.rejectedWith(error);

    expect(failed.calledOnceWithExactly(error, message)).to.be.true;
  });

  it('leaves an unsettled message to reappear after the visibility timeout', async function () {
    const message = await receiveOne();

    expect(await receive()).to.be.undefined;
    await sleep(1500);

    const again = await receive();
    expect(again?.MessageId).to.equal(message.MessageId);
  });
});
