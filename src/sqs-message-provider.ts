import {
  AbstractService,
  EventEmittingService,
  IterableProvider,
  ProviderFn,
  SERVICE_STARTED,
  SERVICE_STOPPED,
  getComponent,
} from '@sektek/utility-belt';
import { Message, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';

import {
  SqsMessageProviderEvents,
  SqsMessageProviderOptions,
} from './types/index.js';

const DEFAULT_WAIT_TIME_SECONDS = 20;
const DEFAULT_MAX_NUMBER_OF_MESSAGES = 10;

/**
 * A long-polling {@link IterableProvider} of native SQS `Message` objects.
 *
 * Each call to {@link SqsMessageProvider.values} issues one ReceiveMessage
 * request and returns the messages it yielded, with receipt handle and
 * metadata intact. The provider only retrieves messages. It never deletes or
 * acknowledges them, so an unsettled message reappears after its visibility
 * timeout.
 *
 * Emits `message:received` once per message, and `message:error` with the
 * error when a poll fails, before rethrowing it. Also emits the `service:*`
 * lifecycle events.
 *
 * {@link SqsMessageProvider.stop} aborts any in-flight long poll. An aborted
 * poll resolves with no messages and is not reported as an error. While the
 * provider is stopped, `values()` returns no messages; `start()` resumes
 * polling.
 *
 * @example
 * ```ts
 * const provider = new SqsMessageProvider({
 *   client: () => sqsClient,
 *   queueUrl,
 * });
 * const processor = new ProcessingIterableProvider({
 *   provider,
 *   processor: sqsEventProcessor,
 * });
 * ```
 */
export class SqsMessageProvider
  extends AbstractService
  implements
    IterableProvider<Message>,
    EventEmittingService<SqsMessageProviderEvents>
{
  #client: ProviderFn<SQSClient>;
  #queueUrl: string;
  #waitTimeSeconds: number;
  #maxNumberOfMessages: number;
  #visibilityTimeout: number | undefined;
  #messageAttributeNames: string[] | undefined;
  #messageSystemAttributeNames: SqsMessageProviderOptions['messageSystemAttributeNames'];
  #abortController = new AbortController();

  /**
   * @param opts - The provider options.
   */
  constructor(opts: SqsMessageProviderOptions) {
    super(opts);
    this.#client = getComponent(opts.client, 'get');
    this.#queueUrl = opts.queueUrl;
    this.#waitTimeSeconds = opts.waitTimeSeconds ?? DEFAULT_WAIT_TIME_SECONDS;
    this.#maxNumberOfMessages =
      opts.maxNumberOfMessages ?? DEFAULT_MAX_NUMBER_OF_MESSAGES;
    this.#visibilityTimeout = opts.visibilityTimeout;
    this.#messageAttributeNames = opts.messageAttributeNames;
    this.#messageSystemAttributeNames = opts.messageSystemAttributeNames;
  }

  /**
   * Resumes polling after a {@link SqsMessageProvider.stop}. Emits
   * `service:started`.
   */
  async start(): Promise<void> {
    if (this.#abortController.signal.aborted) {
      this.#abortController = new AbortController();
    }

    this.emit(SERVICE_STARTED);
  }

  /**
   * Aborts any in-flight long poll and makes `values()` return no messages
   * until the provider is started again. Emits `service:stopped`.
   */
  async stop(): Promise<void> {
    this.#abortController.abort();
    this.emit(SERVICE_STOPPED);
  }

  /**
   * Issues one ReceiveMessage long poll.
   *
   * @returns The messages received, or an empty array when the poll timed out
   *          empty or was aborted by `stop()`.
   * @throws {Error} The error of a failed poll, after emitting `message:error`.
   */
  async values(): Promise<Message[]> {
    const { signal } = this.#abortController;

    if (signal.aborted) {
      return [];
    }

    try {
      const client = await this.#client();
      const { Messages = [] } = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: this.#queueUrl,
          WaitTimeSeconds: this.#waitTimeSeconds,
          MaxNumberOfMessages: this.#maxNumberOfMessages,
          VisibilityTimeout: this.#visibilityTimeout,
          MessageAttributeNames: this.#messageAttributeNames,
          MessageSystemAttributeNames: this.#messageSystemAttributeNames,
        }),
        { abortSignal: signal },
      );

      for (const message of Messages) {
        this.emit('message:received', message);
      }

      return Messages;
    } catch (err) {
      if (signal.aborted) {
        return [];
      }

      this.emit(
        'message:error',
        err instanceof Error ? err : new Error(String(err)),
      );

      throw err;
    }
  }
}
