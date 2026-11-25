import { Processor, getComponent } from '@sektek/utility-belt';
import { Event } from '@sektek/synaptik';
import { Message } from '@aws-sdk/client-sqs';

import { SqsEventProcessorOptions } from './types/index.js';

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isOptionalString = (value: unknown): boolean =>
  value === undefined || typeof value === 'string';

const isEvent = (value: unknown): value is Event =>
  isRecord(value) &&
  typeof value.id === 'string' &&
  typeof value.type === 'string' &&
  isRecord(value.data) &&
  isOptionalString(value.parentId) &&
  (value.replyTo === undefined ||
    (Array.isArray(value.replyTo) &&
      value.replyTo.every(item => typeof item === 'string')));

const deserializeJsonEvent = (body: string): Event => {
  const parsed: unknown = JSON.parse(body);

  if (!isEvent(parsed)) {
    throw new Error('Message body is not a valid Event');
  }

  return parsed;
};

/**
 * Converts native SQS messages into Events and remembers which message each
 * Event came from, so a consumer can later find the message (for example its
 * receipt handle) for an Event it was handed.
 *
 * It never deletes or acknowledges a message.
 */
export class SqsEventProcessor implements Processor<Message, Event> {
  #deserializer: (body: string) => Event | PromiseLike<Event>;
  #messages = new WeakMap<Event, Message>();

  /**
   * Creates a new SqsEventProcessor.
   *
   * @param opts The options for the processor.
   */
  constructor(opts: SqsEventProcessorOptions = {}) {
    this.#deserializer = getComponent(opts.deserializer, 'get', {
      default: deserializeJsonEvent,
      name: 'deserializer',
    });
  }

  /**
   * Converts an SQS message into an Event and records the message it came
   * from.
   *
   * @param message The SQS message to convert.
   * @returns The Event built from the message body.
   * @throws {Error} If the message has no body or the deserializer fails. The error
   *         message includes the message ID.
   */
  async process(message: Message): Promise<Event> {
    if (message.Body === undefined) {
      throw new Error(`SQS message ${message.MessageId} has no body`);
    }

    let event: Event;

    try {
      event = await this.#deserializer(message.Body);
    } catch (err) {
      throw new Error(
        `Failed to deserialize SQS message ${message.MessageId}: ${
          err instanceof Error ? err.message : String(err)
        }`,
        { cause: err },
      );
    }

    this.#messages.set(event, message);

    return event;
  }

  /**
   * Gets the SQS message an Event was produced from.
   *
   * @param event An Event returned by {@link SqsEventProcessor.process}.
   * @returns The original message, or undefined if this processor did not
   *          produce the Event.
   */
  getMessage(event: Event): Message | undefined {
    return this.#messages.get(event);
  }
}
