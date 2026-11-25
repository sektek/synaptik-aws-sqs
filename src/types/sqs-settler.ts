import { Component, EventEmittingService } from '@sektek/utility-belt';
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
 * Settles an SQS message that was processed successfully, so that it is not
 * delivered again. A settler knows nothing about the component that calls it.
 */
export interface SqsSettler
  extends EventService, EventEmittingService<SqsSettlerEvents> {
  /**
   * Settles a successfully processed message.
   *
   * @param message - The message to settle.
   * @returns A promise that resolves once the message is settled.
   * @throws {Error} When the message could not be settled. The failure is also
   *   reported through the `message:deleteError` event.
   */
  settle(message: Message): Promise<void>;
}

/**
 * A settler, or a plain function with the same signature as
 * {@link SqsSettler.settle}.
 */
export type SqsSettlerComponent = Component<SqsSettler, 'settle'>;
