import { Component, EventEmittingService } from '@sektek/utility-belt';
import {
  SendMessageCommandInput,
  SendMessageCommandOutput,
} from '@aws-sdk/client-sqs';

/**
 * Events emitted by an {@link SqsTransport}.
 *
 * `message:sent` and `message:error` report outcomes confirmed by AWS.
 * `message:queued` reports only that the transport accepted the message into a
 * local batch; it implies nothing about delivery to AWS.
 */
export type SqsTransportEvents = {
  'message:sent': (
    request: SendMessageCommandInput,
    result: SendMessageCommandOutput,
  ) => void;
  'message:error': (error: unknown, request: SendMessageCommandInput) => void;
  'message:queued': (request: SendMessageCommandInput) => void;
};

/**
 * Hands a fully built SQS message to AWS.
 */
export interface SqsTransport extends EventEmittingService<SqsTransportEvents> {
  /**
   * Accepts a message for delivery. Resolves once the transport has accepted
   * the message, which for a batching transport may be before AWS has
   * confirmed it.
   *
   * @param request - The message to send.
   */
  send(request: SendMessageCommandInput): Promise<void>;
}

/**
 * A {@link SqsTransport} instance, or a plain function with its `send`
 * signature.
 */
export type SqsTransportComponent = Component<SqsTransport, 'send'>;
