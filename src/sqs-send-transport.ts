import {
  AbstractEventComponent,
  EventComponentOptions,
} from '@sektek/synaptik';
import { ProviderComponent, getComponent } from '@sektek/utility-belt';
import {
  SQSClient,
  SendMessageCommand,
  SendMessageCommandInput,
} from '@aws-sdk/client-sqs';

import { SqsTransport, SqsTransportEvents } from './types/index.js';

/**
 * Options for {@link SqsSendTransport}.
 */
export type SqsSendTransportOptions =
  EventComponentOptions<SqsTransportEvents> & {
    /** Provides the SQS client used to send messages. */
    client: ProviderComponent<SQSClient>;
  };

/**
 * Sends each message immediately with `SendMessage` and waits for the AWS
 * response.
 *
 * Emits `message:sent` with the SDK result once AWS confirms the message, and
 * `message:error` when the send fails. It never emits `message:queued`.
 */
export class SqsSendTransport
  extends AbstractEventComponent<SqsTransportEvents>
  implements SqsTransport
{
  #client: () => SQSClient | PromiseLike<SQSClient>;

  /**
   * @param opts - The transport options.
   */
  constructor(opts: SqsSendTransportOptions) {
    super(opts);
    this.#client = getComponent(opts.client, 'get', { name: 'client' });
  }

  /**
   * Sends the message and waits for AWS to confirm it.
   *
   * @param request - The message to send.
   * @returns A promise that resolves once AWS has confirmed the message.
   * @throws {Error} When the send fails, after emitting `message:error`.
   */
  async send(request: SendMessageCommandInput): Promise<void> {
    try {
      const client = await this.#client();
      const result = await client.send(new SendMessageCommand(request));

      this.emit('message:sent', request, result);
    } catch (error) {
      this.emit('message:error', error, request);

      throw error;
    }
  }
}
