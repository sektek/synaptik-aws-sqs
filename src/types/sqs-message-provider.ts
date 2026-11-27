import {
  ErrorHandlerFn,
  ProviderComponent,
  ServiceEvents,
  ServiceOptions,
} from '@sektek/utility-belt';
import {
  Message,
  MessageSystemAttributeName,
  SQSClient,
} from '@aws-sdk/client-sqs';

/**
 * Options for `SqsMessageProvider`.
 */
export type SqsMessageProviderOptions = ServiceOptions & {
  /** Supplies the SQS client used to poll the queue. */
  client: ProviderComponent<SQSClient>;
  /** The URL of the queue to poll. */
  queueUrl: string;
  /**
   * Seconds each ReceiveMessage request waits for messages to arrive.
   * Defaults to `20`, the SQS maximum.
   */
  waitTimeSeconds?: number;
  /**
   * The maximum number of messages one poll returns. Defaults to `10`, the
   * SQS maximum.
   */
  maxNumberOfMessages?: number;
  /**
   * Seconds a received message stays invisible to other consumers. When
   * omitted, the queue's own visibility timeout applies.
   */
  visibilityTimeout?: number;
  /** Names of the message attributes to return with each message. */
  messageAttributeNames?: string[];
  /** Names of the system attributes to return with each message. */
  messageSystemAttributeNames?: MessageSystemAttributeName[];
};

/**
 * Events emitted by `SqsMessageProvider`, in addition to the service
 * lifecycle events.
 */
export type SqsMessageProviderEvents = ServiceEvents & {
  /** A message was retrieved from the queue. */
  'message:received': (message: Message) => void;
  /** A poll failed. The listener receives the error. */
  'message:error': ErrorHandlerFn;
};
