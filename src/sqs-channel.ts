import {
  AbstractEventComponent,
  EVENT_DELIVERED,
  EVENT_ERROR,
  EVENT_RECEIVED,
  Event,
  EventBasedProviderComponent,
  EventBasedProviderFn,
  EventBasedStringProviderComponent,
  EventChannelEvents,
  EventComponentOptions,
} from '@sektek/synaptik';
import {
  EventEmittingService,
  getComponent,
  nothing,
} from '@sektek/utility-belt';
import {
  MessageAttributeValue,
  MessageSystemAttributeNameForSends,
  MessageSystemAttributeValue,
  SendMessageCommandInput,
} from '@aws-sdk/client-sqs';

import { SqsTransportComponent, SqsTransportEvents } from './types/index.js';

const FIFO_SUFFIX = '.fifo';
const DEFAULT_SERIALIZER = (event: Event) => JSON.stringify(event);

/**
 * Events emitted by an {@link SqsChannel}: the standard channel events plus the
 * transport's `message:sent`, `message:error` and `message:queued` events with
 * unchanged payloads.
 *
 * @template T - The event type.
 */
export type SqsChannelEvents<T extends Event = Event> = EventChannelEvents<T> &
  SqsTransportEvents;

/**
 * Options for {@link SqsChannel}.
 *
 * @template T - The event type.
 */
export type SqsChannelOptions<T extends Event = Event> =
  EventComponentOptions & {
    /** Resolves the queue URL (not the ARN) an event is sent to. */
    queueUrl: EventBasedStringProviderComponent<T>;
    /** Hands the built message to AWS. */
    transport: SqsTransportComponent;
    /** Serializes the event into the message body. Defaults to `JSON.stringify`. */
    serializer?: EventBasedStringProviderComponent<T>;
    /** Resolves the message attributes. Defaults to none. */
    messageAttributes?: EventBasedProviderComponent<
      Record<string, MessageAttributeValue> | undefined,
      T
    >;
    /** Resolves the delay in seconds. Rejected for FIFO queues. Defaults to none. */
    delaySeconds?: EventBasedProviderComponent<number | undefined, T>;
    /** Resolves the FIFO deduplication ID. Defaults to none. */
    messageDeduplicationId?: EventBasedProviderComponent<string | undefined, T>;
    /** Resolves the FIFO message group ID, required for FIFO queues. Defaults to none. */
    messageGroupId?: EventBasedProviderComponent<string | undefined, T>;
    /** Resolves the message system attributes. Defaults to none. */
    messageSystemAttributes?: EventBasedProviderComponent<
      | Partial<
          Record<
            MessageSystemAttributeNameForSends,
            MessageSystemAttributeValue
          >
        >
      | undefined,
      T
    >;
  };

/**
 * Publishes Synaptik events to an SQS queue.
 *
 * The channel serializes the event, builds the SQS message through its
 * providers and hands it to the transport. `message:sent` means AWS confirmed
 * the message; a message that is only held in a transport's local batch is
 * reported as `message:queued`.
 *
 * @template T - The event type.
 */
export class SqsChannel<T extends Event = Event>
  extends AbstractEventComponent
  implements EventEmittingService<SqsChannelEvents<T>>
{
  #queueUrl: EventBasedProviderFn<string, T>;
  #serializer: EventBasedProviderFn<string, T>;
  #messageAttributes: EventBasedProviderFn<
    Record<string, MessageAttributeValue> | undefined,
    T
  >;
  #delaySeconds: EventBasedProviderFn<number | undefined, T>;
  #messageDeduplicationId: EventBasedProviderFn<string | undefined, T>;
  #messageGroupId: EventBasedProviderFn<string | undefined, T>;
  #messageSystemAttributes: EventBasedProviderFn<
    | Partial<
        Record<MessageSystemAttributeNameForSends, MessageSystemAttributeValue>
      >
    | undefined,
    T
  >;
  #transport: (request: SendMessageCommandInput) => Promise<void>;

  /**
   * @param opts - The channel options.
   */
  constructor(opts: SqsChannelOptions<T>) {
    super(opts);

    this.#queueUrl = getComponent(opts.queueUrl, 'get', { name: 'queueUrl' });
    this.#serializer = getComponent(opts.serializer, 'get', {
      name: 'serializer',
      default: DEFAULT_SERIALIZER,
    });
    this.#messageAttributes = getComponent(opts.messageAttributes, 'get', {
      name: 'messageAttributes',
      default: nothing,
    });
    this.#delaySeconds = getComponent(opts.delaySeconds, 'get', {
      name: 'delaySeconds',
      default: nothing,
    });
    this.#messageDeduplicationId = getComponent(
      opts.messageDeduplicationId,
      'get',
      { name: 'messageDeduplicationId', default: nothing },
    );
    this.#messageGroupId = getComponent(opts.messageGroupId, 'get', {
      name: 'messageGroupId',
      default: nothing,
    });
    this.#messageSystemAttributes = getComponent(
      opts.messageSystemAttributes,
      'get',
      { name: 'messageSystemAttributes', default: nothing },
    );
    this.#transport = getComponent(opts.transport, 'send', {
      name: 'transport',
    });

    if ('on' in opts.transport) {
      this.#forwardTransportEvents(opts.transport);
    }
  }

  /**
   * Sends an event to the queue its queue URL provider resolves.
   *
   * @param event - The event to send.
   * @throws {Error} If a provider or the transport fails, if a delay is set for a FIFO
   * queue, or if no message group ID is set for a FIFO queue.
   */
  async send(event: T): Promise<void> {
    this.emit(EVENT_RECEIVED, event);

    try {
      const request = await this.#buildRequest(event);

      await this.#transport(request);
      this.emit(EVENT_DELIVERED, event);
    } catch (err) {
      this.emit(EVENT_ERROR, err, event);

      throw err;
    }
  }

  async #buildRequest(event: T): Promise<SendMessageCommandInput> {
    const queueUrl = await this.#queueUrl(event);
    const messageBody = await this.#serializer(event);
    const messageAttributes = await this.#messageAttributes(event);
    const delaySeconds = await this.#delaySeconds(event);
    const messageDeduplicationId = await this.#messageDeduplicationId(event);
    const messageGroupId = await this.#messageGroupId(event);
    const messageSystemAttributes = await this.#messageSystemAttributes(event);

    if (queueUrl.endsWith(FIFO_SUFFIX)) {
      if (delaySeconds !== undefined) {
        throw new Error(
          `delaySeconds is not supported on FIFO queue ${queueUrl}`,
        );
      }

      if (!messageGroupId) {
        throw new Error(
          `messageGroupId is required for FIFO queue ${queueUrl}`,
        );
      }
    }

    return {
      QueueUrl: queueUrl,
      MessageBody: messageBody,
      ...(messageAttributes !== undefined && {
        MessageAttributes: messageAttributes,
      }),
      ...(delaySeconds !== undefined && { DelaySeconds: delaySeconds }),
      ...(messageDeduplicationId !== undefined && {
        MessageDeduplicationId: messageDeduplicationId,
      }),
      ...(messageGroupId !== undefined && { MessageGroupId: messageGroupId }),
      ...(messageSystemAttributes !== undefined && {
        MessageSystemAttributes: messageSystemAttributes,
      }),
    };
  }

  #forwardTransportEvents(transport: EventEmittingService<SqsTransportEvents>) {
    transport.on('message:sent', (request, result) =>
      this.emit('message:sent', request, result),
    );
    transport.on('message:error', (error, request) =>
      this.emit('message:error', error, request),
    );
    transport.on('message:queued', request =>
      this.emit('message:queued', request),
    );
  }
}
