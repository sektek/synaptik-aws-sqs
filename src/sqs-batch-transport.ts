import {
  AbstractService,
  EventEmittingService,
  ProviderFn,
  SERVICE_STARTED,
  SERVICE_STOPPED,
  SERVICE_STOP_ERROR,
  getComponent,
  sleep,
  toError,
} from '@sektek/utility-belt';
import {
  SQSClient,
  SendMessageBatchCommand,
  SendMessageBatchRequestEntry,
  SendMessageCommandInput,
} from '@aws-sdk/client-sqs';

import {
  SqsBatchTransportEvents,
  SqsBatchTransportOptions,
  SqsTransport,
} from './types/index.js';

const MAX_BATCH_SIZE = 10;
const DEFAULT_FLUSH_INTERVAL_MS = 100;
const DEFAULT_MAX_BATCHES = 10;
const DEFAULT_CAPACITY_WAIT_TIMEOUT_MS = 5000;
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_RETRY_BACKOFF_MS = 100;
const DEFAULT_MAX_RETRY_BACKOFF_MS = 5000;
const DEFAULT_STOP_TIMEOUT_MS = 30_000;

type Entry = { id: string; request: SendMessageCommandInput };

type Batch = {
  queueUrl: string;
  entries: Entry[];
  timer?: NodeJS.Timeout;
};

type Failure = { entry: Entry; error: Error; retryable: boolean };

const assertPositive = (name: string, value: number) => {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive number`);
  }
};

const isPermanentFailure = (error: unknown): boolean =>
  typeof error === 'object' &&
  error !== null &&
  '$fault' in error &&
  error.$fault === 'client' &&
  !('$retryable' in error && error.$retryable);

const toBatchEntry = ({
  id,
  request,
}: Entry): SendMessageBatchRequestEntry => ({
  Id: id,
  MessageBody: request.MessageBody,
  DelaySeconds: request.DelaySeconds,
  MessageAttributes: request.MessageAttributes,
  MessageSystemAttributes: request.MessageSystemAttributes,
  MessageDeduplicationId: request.MessageDeduplicationId,
  MessageGroupId: request.MessageGroupId,
});

/**
 * An asynchronous, batching {@link SqsTransport} that delivers messages with
 * `SendMessageBatch`.
 *
 * `send()` accepts a message into a local batch and returns, emitting
 * `message:queued`. It does not wait for AWS, so the channel call that
 * produced the message may already have returned when delivery fails.
 * Delivery outcomes are reported only through events: `message:sent` with the
 * request and the AWS result once AWS has confirmed the message, and
 * `message:error` with the error and the request once delivery has failed
 * terminally. `message:queued` never implies delivery.
 *
 * Messages are batched per queue URL. A batch is sent when it holds
 * `maxBatchSize` messages or `flushIntervalMs` after its first message,
 * whichever comes first. At most `maxBatches` batches, open and in flight
 * together, exist at once; `send()` waits up to `capacityWaitTimeoutMs` for a
 * free slot and then rejects.
 *
 * A `SendMessageBatch` response can fail single entries. Each failed entry is
 * retried on its own, with exponential backoff, up to `maxAttempts` attempts
 * in total, and keeps its batch's capacity slot until it has succeeded or
 * failed terminally. An entry AWS rejects as the sender's fault, a request
 * that fails with a non-retryable client error, and an entry out of attempts
 * fail terminally. A request that exceeds `requestTimeoutMs` is aborted and
 * counts as a failed attempt for every entry in it.
 *
 * Delivery is at-least-once. A request that times out or fails after AWS
 * accepted it is retried, so the queue may receive a message more than once;
 * consumers must tolerate duplicates. Retrying a failed entry separately from
 * its batch can also reorder messages, so FIFO queues keep their
 * per-message-group order only when no entry needs a retry.
 *
 * `stop()` rejects new sends, flushes open batches and waits up to
 * `stopTimeoutMs` for every batch, retries included, to finish. On timeout it
 * aborts the remaining requests, reports their messages through
 * `message:error` and fails with `service:stopError`.
 *
 * @example
 * ```ts
 * const transport = new SqsBatchTransport({ client: () => sqsClient });
 * transport.on('message:sent', request => markDelivered(request));
 * transport.on('message:error', (error, request) => deadLetter(request));
 * const channel = new SqsChannel({ queueUrl, transport });
 * ```
 */
export class SqsBatchTransport
  extends AbstractService
  implements SqsTransport, EventEmittingService<SqsBatchTransportEvents>
{
  #client: ProviderFn<SQSClient>;
  #maxBatchSize: number;
  #flushIntervalMs: number;
  #maxBatches: number;
  #capacityWaitTimeoutMs: number;
  #requestTimeoutMs: number;
  #maxAttempts: number;
  #retryBackoffMs: number;
  #maxRetryBackoffMs: number;
  #stopTimeoutMs: number;
  #batches = new Set<Batch>();
  #open = new Map<string, Batch>();
  #inFlight = new Map<Batch, Promise<void>>();
  #waiters: Array<() => void> = [];
  #accepting = true;
  #abortController = new AbortController();

  /**
   * @param opts - The transport options.
   * @throws {RangeError} If `maxBatchSize` is not between 1 and 10, or another
   * numeric option is not a positive number.
   */
  constructor(opts: SqsBatchTransportOptions) {
    super(opts);
    this.#client = getComponent(opts.client, 'get', { name: 'client' });
    this.#maxBatchSize = opts.maxBatchSize ?? MAX_BATCH_SIZE;
    this.#flushIntervalMs = opts.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.#maxBatches = opts.maxBatches ?? DEFAULT_MAX_BATCHES;
    this.#capacityWaitTimeoutMs =
      opts.capacityWaitTimeoutMs ?? DEFAULT_CAPACITY_WAIT_TIMEOUT_MS;
    this.#requestTimeoutMs =
      opts.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.#maxAttempts = opts.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.#retryBackoffMs = opts.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF_MS;
    this.#maxRetryBackoffMs =
      opts.maxRetryBackoffMs ?? DEFAULT_MAX_RETRY_BACKOFF_MS;
    this.#stopTimeoutMs = opts.stopTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS;

    if (
      !Number.isInteger(this.#maxBatchSize) ||
      this.#maxBatchSize < 1 ||
      this.#maxBatchSize > MAX_BATCH_SIZE
    ) {
      throw new RangeError(
        `maxBatchSize must be an integer between 1 and ${MAX_BATCH_SIZE}`,
      );
    }

    assertPositive('flushIntervalMs', this.#flushIntervalMs);
    assertPositive('maxBatches', this.#maxBatches);
    assertPositive('capacityWaitTimeoutMs', this.#capacityWaitTimeoutMs);
    assertPositive('requestTimeoutMs', this.#requestTimeoutMs);
    assertPositive('maxAttempts', this.#maxAttempts);
    assertPositive('retryBackoffMs', this.#retryBackoffMs);
    assertPositive('maxRetryBackoffMs', this.#maxRetryBackoffMs);
    assertPositive('stopTimeoutMs', this.#stopTimeoutMs);
  }

  /**
   * Accepts a message into the local batch for its queue and emits
   * `message:queued`. Resolves without waiting for AWS; the outcome arrives as
   * `message:sent` or `message:error`.
   *
   * @param request - The message to send. `QueueUrl` and `MessageBody` are
   * required.
   * @throws {Error} If the transport is stopped, the request lacks a queue URL
   * or a message body, or no capacity became free within
   * `capacityWaitTimeoutMs`.
   */
  async send(request: SendMessageCommandInput): Promise<void> {
    const { QueueUrl: queueUrl, MessageBody: messageBody } = request;

    if (!queueUrl || messageBody === undefined) {
      throw new Error('QueueUrl and MessageBody are required');
    }

    const deadline = Date.now() + this.#capacityWaitTimeoutMs;
    let batch = this.#tryReserve(queueUrl);

    while (!batch) {
      await this.#waitForCapacity(deadline);
      batch = this.#tryReserve(queueUrl);
    }

    batch.entries.push({ id: String(batch.entries.length), request });
    this.emit('message:queued', request);

    if (batch.entries.length >= this.#maxBatchSize) {
      this.#dispatch(batch);
    }
  }

  #tryReserve(queueUrl: string): Batch | undefined {
    if (!this.#accepting) {
      throw new Error('SqsBatchTransport is stopped');
    }

    const open = this.#open.get(queueUrl);

    if (open) {
      return open;
    }

    if (this.#batches.size >= this.#maxBatches) {
      return undefined;
    }

    const batch: Batch = { queueUrl, entries: [] };
    batch.timer = setTimeout(
      () => this.#dispatch(batch),
      this.#flushIntervalMs,
    );
    this.#batches.add(batch);
    this.#open.set(queueUrl, batch);

    return batch;
  }

  #waitForCapacity(deadline: number): Promise<void> {
    const remaining = deadline - Date.now();

    if (remaining <= 0) {
      return Promise.reject(this.#capacityTimeoutError());
    }

    return new Promise<void>((resolve, reject) => {
      const waiter = () => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        this.#waiters = this.#waiters.filter(w => w !== waiter);
        reject(this.#capacityTimeoutError());
      }, remaining);

      this.#waiters.push(waiter);
    });
  }

  #capacityTimeoutError(): Error {
    return new Error(
      `No batch capacity became available within ${this.#capacityWaitTimeoutMs}ms`,
    );
  }

  #dispatch(batch: Batch) {
    clearTimeout(batch.timer);

    if (this.#open.get(batch.queueUrl) === batch) {
      this.#open.delete(batch.queueUrl);
    }

    this.#inFlight.set(batch, this.#track(batch));
  }

  async #track(batch: Batch) {
    try {
      await this.#deliver(batch, this.#abortController.signal);
    } catch (error) {
      this.logger().error('batch delivery failed', { error });
    } finally {
      this.#inFlight.delete(batch);
      this.#batches.delete(batch);
      this.#wake();
    }
  }

  async #deliver(batch: Batch, signal: AbortSignal) {
    let pending = batch.entries;

    let attempt = 0;

    while (pending.length > 0) {
      attempt++;

      const failures = await this.#attempt(batch.queueUrl, pending, signal);
      const exhausted = attempt >= this.#maxAttempts || signal.aborted;

      pending = [];

      for (const { entry, error, retryable } of failures) {
        if (retryable && !exhausted) {
          pending.push(entry);
        } else {
          this.emit('message:error', error, entry.request);
        }
      }

      if (pending.length > 0) {
        await this.#backoff(attempt, signal);
      }
    }
  }

  async #attempt(
    queueUrl: string,
    entries: Entry[],
    signal: AbortSignal,
  ): Promise<Failure[]> {
    const timeoutSignal = AbortSignal.timeout(this.#requestTimeoutMs);

    try {
      if (signal.aborted) {
        throw new Error('SqsBatchTransport stopped before delivery completed');
      }

      const client = await this.#client();
      const response = await client.send(
        new SendMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: entries.map(toBatchEntry),
        }),
        { abortSignal: AbortSignal.any([signal, timeoutSignal]) },
      );
      const unreported = new Map(entries.map(entry => [entry.id, entry]));
      const failures: Failure[] = [];

      for (const { Id, ...result } of response.Successful ?? []) {
        const entry = unreported.get(Id ?? '');

        if (entry) {
          unreported.delete(entry.id);
          this.emit('message:sent', entry.request, {
            ...result,
            $metadata: response.$metadata,
          });
        }
      }

      for (const failed of response.Failed ?? []) {
        const entry = unreported.get(failed.Id ?? '');

        if (entry) {
          unreported.delete(entry.id);
          failures.push({
            entry,
            error: new Error(
              `SendMessageBatch entry failed: ${failed.Code}: ${failed.Message}`,
              { cause: failed },
            ),
            retryable: !failed.SenderFault,
          });
        }
      }

      for (const entry of unreported.values()) {
        failures.push({
          entry,
          error: new Error('SendMessageBatch response omitted the entry'),
          retryable: true,
        });
      }

      return failures;
    } catch (thrown) {
      const error =
        timeoutSignal.aborted && !signal.aborted
          ? new Error(
              `SendMessageBatch timed out after ${this.#requestTimeoutMs}ms`,
              { cause: thrown },
            )
          : toError(thrown);

      return entries.map(entry => ({
        entry,
        error,
        retryable: !isPermanentFailure(thrown),
      }));
    }
  }

  async #backoff(attempt: number, signal: AbortSignal) {
    if (signal.aborted) {
      return;
    }

    const delay = Math.min(
      this.#maxRetryBackoffMs,
      this.#retryBackoffMs * 2 ** (attempt - 1),
    );
    const nap = sleep(delay);
    const cancel = () => nap.cancel();

    signal.addEventListener('abort', cancel, { once: true });
    await nap;
    signal.removeEventListener('abort', cancel);
  }

  /**
   * Resumes accepting messages after a {@link SqsBatchTransport.stop}. Emits
   * `service:started`.
   */
  async start(): Promise<void> {
    if (this.#abortController.signal.aborted) {
      this.#abortController = new AbortController();
    }

    this.#accepting = true;
    this.emit(SERVICE_STARTED);
  }

  /**
   * Rejects new sends, flushes the open batches and waits for every queued and
   * in-flight batch, retries included, to finish. Emits `service:stopped` once
   * everything is delivered or has failed terminally.
   *
   * @throws {Error} If delivery does not finish within `stopTimeoutMs`, after
   * aborting the remaining requests and emitting `service:stopError`. The
   * messages still undelivered are then reported through `message:error`.
   */
  async stop(): Promise<void> {
    this.#accepting = false;
    this.#wake();

    try {
      for (const batch of [...this.#open.values()]) {
        this.#dispatch(batch);
      }

      await this.#drain();
    } catch (error) {
      this.emit(SERVICE_STOP_ERROR, toError(error));

      throw error;
    }

    this.emit(SERVICE_STOPPED);
  }

  #wake() {
    const waiters = this.#waiters;

    this.#waiters = [];
    waiters.forEach(waiter => waiter());
  }

  async #drain() {
    const timeout = sleep(this.#stopTimeoutMs);
    const winner = await Promise.race([
      Promise.allSettled([...this.#inFlight.values()]),
      timeout,
    ]);

    timeout.cancel();

    if (!Array.isArray(winner)) {
      this.#abortController.abort();

      throw new Error(
        `SqsBatchTransport did not drain within ${this.#stopTimeoutMs}ms`,
      );
    }
  }
}
