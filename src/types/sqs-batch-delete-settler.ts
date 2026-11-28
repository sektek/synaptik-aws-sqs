import {
  ProviderComponent,
  ServiceEvents,
  ServiceOptions,
} from '@sektek/utility-belt';
import { SQSClient } from '@aws-sdk/client-sqs';

import { SqsSettlerEvents } from './sqs-settler.js';

/**
 * Options for `SqsBatchDeleteSettler`.
 */
export type SqsBatchDeleteSettlerOptions = ServiceOptions & {
  /** Supplies the SQS client used to delete messages. */
  client: ProviderComponent<SQSClient>;
  /** The URL of the queue the messages were received from. */
  queueUrl: string;
  /**
   * The visibility timeout, in seconds, that applies to the received messages.
   * A delete that lands after it is reported as a failure, because SQS may
   * already have returned the message to the queue.
   */
  visibilityTimeoutSeconds: number;
  /**
   * Milliseconds before the visibility timeout expires that a delete is given
   * up on. A queued delete's deadline is the time it was queued plus the
   * visibility timeout minus this margin. A batch is also sent as soon as the
   * earliest deadline in it is this close, which leaves the margin for the
   * request itself. Defaults to `1000`.
   */
  safetyMarginMs?: number;
  /**
   * The most deletes one `DeleteMessageBatch` request carries. A batch is sent
   * as soon as it holds this many deletes. Defaults to `10`, which is also the
   * SQS maximum.
   */
  maxBatchSize?: number;
  /**
   * Milliseconds a queued delete waits for a batch to fill before the batch is
   * sent. Defaults to `100`.
   */
  flushIntervalMs?: number;
  /**
   * Milliseconds one `DeleteMessageBatch` request may take before it is
   * aborted and counted as a failed attempt. Defaults to `10000`.
   */
  requestTimeoutMs?: number;
  /**
   * Milliseconds `stop()` waits for queued and in-flight deletes before it
   * aborts the remaining requests and fails. Defaults to `30000`.
   */
  stopTimeoutMs?: number;
};

/**
 * Events emitted by `SqsBatchDeleteSettler`: the settler events and the
 * service lifecycle events.
 */
export type SqsBatchDeleteSettlerEvents = SqsSettlerEvents & ServiceEvents;
