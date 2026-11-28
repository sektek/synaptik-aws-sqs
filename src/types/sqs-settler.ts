import { Collector, EventEmittingService } from '@sektek/utility-belt';
import { EventService } from '@sektek/synaptik';
import { Message } from '@aws-sdk/client-sqs';

export const MESSAGE_DELETED = 'message:deleted';
export const MESSAGE_DELETE_ERROR = 'message:deleteError';

/**
 * Events emitted by an {@link SqsSettler}.
 */
export type SqsSettlerEvents = {
  /** The message was deleted from the queue. */
  [MESSAGE_DELETED]: (message: Message) => void;
  /** Deleting the message failed. */
  [MESSAGE_DELETE_ERROR]: (error: unknown, message: Message) => void;
};

/**
 * Collects SQS messages that were processed successfully and settles them, so
 * that they are not delivered again. A settler knows nothing about the
 * component that adds messages to it.
 */
export interface SqsSettler
  extends
    Collector<Message>,
    EventService,
    EventEmittingService<SqsSettlerEvents> {
  /**
   * Adds a successfully processed message to be settled.
   *
   * @param message - The message to settle.
   * @returns A promise that resolves once the message is settled, or, for a
   *   batching settler, once it is queued for settlement.
   * @throws {Error} When the message could not be settled. The failure is also
   *   reported through the `message:deleteError` event.
   */
  add(message: Message): Promise<void>;
}
