import {
  ProviderComponent,
  ServiceEvents,
  ServiceOptions,
} from '@sektek/utility-belt';
import { SQSClient } from '@aws-sdk/client-sqs';

import { SqsTransportEvents } from './sqs-transport.js';

/**
 * Options for `SqsBatchTransport`.
 */
export type SqsBatchTransportOptions = ServiceOptions & {
  /** Supplies the SQS client used to send batches. */
  client: ProviderComponent<SQSClient>;
  /**
   * The most messages one `SendMessageBatch` request carries. A batch is sent
   * as soon as it holds this many messages. Defaults to `10`, which is also
   * the SQS maximum.
   */
  maxBatchSize?: number;
  /**
   * Milliseconds a partial batch waits, counted from its first message,
   * before it is sent. Defaults to `100`.
   */
  flushIntervalMs?: number;
  /**
   * The most batches that may exist at once, counting open batches and
   * batches still being delivered, retries included. Defaults to `10`.
   */
  maxBatches?: number;
  /**
   * Milliseconds `send()` waits for capacity when `maxBatches` is reached
   * before it rejects. Defaults to `5000`.
   */
  capacityWaitTimeoutMs?: number;
  /**
   * Milliseconds one `SendMessageBatch` request may take before it is aborted
   * and counted as a failed attempt. Defaults to `10000`.
   */
  requestTimeoutMs?: number;
  /**
   * The most times one message is attempted, the first attempt included.
   * Defaults to `3`.
   */
  maxAttempts?: number;
  /**
   * Milliseconds to wait before the first retry. Each further retry doubles
   * the wait, up to `maxRetryBackoffMs`. Defaults to `100`.
   */
  retryBackoffMs?: number;
  /** The longest wait between two retries, in milliseconds. Defaults to `5000`. */
  maxRetryBackoffMs?: number;
  /**
   * Milliseconds `stop()` waits for queued and in-flight batches, retries
   * included, before it aborts the remaining requests and fails. Defaults to
   * `30000`.
   */
  stopTimeoutMs?: number;
};

/**
 * Events emitted by `SqsBatchTransport`: the transport events and the service
 * lifecycle events.
 */
export type SqsBatchTransportEvents = SqsTransportEvents & ServiceEvents;
