import {
  AbstractEventComponent,
  EventComponentOptions,
} from '@sektek/synaptik';
import { DeleteMessageCommand, Message, SQSClient } from '@aws-sdk/client-sqs';
import { ProviderComponent, getComponent } from '@sektek/utility-belt';

import {
  MESSAGE_DELETED,
  MESSAGE_DELETE_ERROR,
  SqsSettler,
} from './types/index.js';

/**
 * Options for {@link SqsDeleteSettler}.
 */
export type SqsDeleteSettlerOptions = EventComponentOptions & {
  /** Provides the SQS client used to delete messages. */
  client: ProviderComponent<SQSClient>;
  /** The URL of the queue the messages were received from. */
  queueUrl: string;
};

/**
 * Settles a message by deleting it from the queue immediately.
 *
 * Emits `message:deleted` once a message is deleted, and
 * `message:deleteError` when the deletion fails.
 */
export class SqsDeleteSettler
  extends AbstractEventComponent
  implements SqsSettler
{
  #client: () => SQSClient | PromiseLike<SQSClient>;
  #queueUrl: string;

  /**
   * @param opts - The settler options.
   */
  constructor(opts: SqsDeleteSettlerOptions) {
    super(opts);
    this.#client = getComponent(opts.client, 'get', { name: 'client' });
    this.#queueUrl = opts.queueUrl;
  }

  /**
   * Deletes the message from the queue using its receipt handle.
   *
   * @param message - The message to delete.
   * @returns A promise that resolves once the message is deleted.
   * @throws {Error} When the deletion fails, after emitting `message:deleteError`.
   */
  async settle(message: Message): Promise<void> {
    try {
      const client = await this.#client();
      await client.send(
        new DeleteMessageCommand({
          QueueUrl: this.#queueUrl,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
    } catch (error) {
      this.emit(MESSAGE_DELETE_ERROR, error, message);
      throw error;
    }

    this.emit(MESSAGE_DELETED, message);
  }
}
