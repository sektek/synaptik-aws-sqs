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
  DeleteMessageBatchCommand,
  DeleteMessageBatchRequestEntry,
  Message,
  SQSClient,
} from '@aws-sdk/client-sqs';

import {
  MESSAGE_DELETED,
  MESSAGE_DELETE_ERROR,
  SqsBatchDeleteSettlerEvents,
  SqsBatchDeleteSettlerOptions,
  SqsSettler,
} from './types/index.js';

const MAX_BATCH_SIZE = 10;
const DEFAULT_SAFETY_MARGIN_MS = 1000;
const DEFAULT_FLUSH_INTERVAL_MS = 100;
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_STOP_TIMEOUT_MS = 30_000;
const MAX_RETRY_DELAY_MS = 50;

type Entry = {
  message: Message;
  deadline: number;
  flushAt: number;
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

/**
 * An {@link SqsSettler} that deletes messages in batches with
 * `DeleteMessageBatch`.
 *
 * `settle()` queues the delete and resolves once it is queued, without waiting
 * for AWS. Outcomes are reported only through events: `message:deleted` once
 * AWS confirmed the delete, and `message:deleteError` when it failed. The
 * handler outcome is never affected.
 *
 * Every delete has a deadline: the time it was queued plus
 * `visibilityTimeoutSeconds`, minus `safetyMarginMs`. Past the visibility
 * timeout SQS returns the message to the queue and its receipt handle may no
 * longer be valid, so a delete that has missed its deadline is not sent or
 * retried; it is reported through `message:deleteError` as a late delete, and
 * the message may be delivered again.
 *
 * A batch is sent when it holds `maxBatchSize` deletes, when
 * `flushIntervalMs` has passed since its oldest delete was queued, or when the
 * earliest deadline in it is within `safetyMarginMs`, whichever comes first.
 * A `DeleteMessageBatch` response can fail single entries. An entry that
 * failed for a reason other than the sender's fault is queued again and sent
 * with a later batch for as long as its deadline has not passed.
 *
 * `stop()` rejects new deletes, flushes the pending ones and waits up to
 * `stopTimeoutMs` for them, retries included. On timeout it aborts the
 * remaining requests, reports their messages through `message:deleteError` and
 * fails with `service:stopError`.
 *
 * @example
 * ```ts
 * const settler = new SqsBatchDeleteSettler({
 *   client: () => sqsClient,
 *   queueUrl,
 *   visibilityTimeoutSeconds: 30,
 * });
 * settler.on('message:deleteError', (error, message) => alert(message));
 * ```
 */
export class SqsBatchDeleteSettler
  extends AbstractService
  implements SqsSettler, EventEmittingService<SqsBatchDeleteSettlerEvents>
{
  #client: ProviderFn<SQSClient>;
  #queueUrl: string;
  #visibilityTimeoutMs: number;
  #safetyMarginMs: number;
  #maxBatchSize: number;
  #flushIntervalMs: number;
  #requestTimeoutMs: number;
  #stopTimeoutMs: number;
  #pending: Entry[] = [];
  #inFlight = new Map<Entry[], Promise<void>>();
  #timer?: NodeJS.Timeout;
  #retryDelayMs: number;
  #accepting = true;
  #abortController = new AbortController();

  /**
   * @param opts - The settler options.
   * @throws {RangeError} If `maxBatchSize` is not between 1 and 10, a numeric
   * option is not a positive number, or `safetyMarginMs` is negative or leaves
   * no time within the visibility timeout.
   */
  constructor(opts: SqsBatchDeleteSettlerOptions) {
    super(opts);
    this.#client = getComponent(opts.client, 'get', { name: 'client' });
    this.#queueUrl = opts.queueUrl;
    this.#visibilityTimeoutMs = opts.visibilityTimeoutSeconds * 1000;
    this.#safetyMarginMs = opts.safetyMarginMs ?? DEFAULT_SAFETY_MARGIN_MS;
    this.#maxBatchSize = opts.maxBatchSize ?? MAX_BATCH_SIZE;
    this.#flushIntervalMs = opts.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.#requestTimeoutMs =
      opts.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.#stopTimeoutMs = opts.stopTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS;
    this.#retryDelayMs = Math.min(this.#flushIntervalMs, MAX_RETRY_DELAY_MS);

    if (
      !Number.isInteger(this.#maxBatchSize) ||
      this.#maxBatchSize < 1 ||
      this.#maxBatchSize > MAX_BATCH_SIZE
    ) {
      throw new RangeError(
        `maxBatchSize must be an integer between 1 and ${MAX_BATCH_SIZE}`,
      );
    }

    assertPositive('visibilityTimeoutSeconds', opts.visibilityTimeoutSeconds);
    assertPositive('flushIntervalMs', this.#flushIntervalMs);
    assertPositive('requestTimeoutMs', this.#requestTimeoutMs);
    assertPositive('stopTimeoutMs', this.#stopTimeoutMs);

    if (
      !Number.isFinite(this.#safetyMarginMs) ||
      this.#safetyMarginMs < 0 ||
      this.#safetyMarginMs >= this.#visibilityTimeoutMs
    ) {
      throw new RangeError(
        'safetyMarginMs must be at least 0 and less than the visibility timeout',
      );
    }
  }

  /**
   * Queues the delete of a message. Resolves once the delete is queued; the
   * outcome arrives as `message:deleted` or `message:deleteError`.
   *
   * @param message - The message to delete. It needs a `ReceiptHandle`.
   * @returns A promise that resolves once the delete is queued.
   * @throws {Error} If the settler is stopped or the message has no receipt
   * handle, after emitting `message:deleteError`.
   */
  async settle(message: Message): Promise<void> {
    if (!this.#accepting || !message.ReceiptHandle) {
      const error = new Error(
        this.#accepting
          ? 'The message has no ReceiptHandle'
          : 'SqsBatchDeleteSettler is stopped',
      );

      this.emit(MESSAGE_DELETE_ERROR, error, message);

      throw error;
    }

    const now = Date.now();

    this.#pending.push({
      message,
      deadline: now + this.#visibilityTimeoutMs - this.#safetyMarginMs,
      flushAt: now + this.#flushIntervalMs,
    });
    this.#schedule();
  }

  #schedule() {
    clearTimeout(this.#timer);
    this.#timer = undefined;

    while (this.#pending.length >= this.#maxBatchSize) {
      this.#dispatch();
    }

    if (this.#pending.length === 0) {
      return;
    }

    const due = Math.min(
      ...this.#pending.map(({ flushAt, deadline }) =>
        Math.min(flushAt, deadline - this.#safetyMarginMs),
      ),
    );

    this.#timer = setTimeout(
      () => {
        this.#timer = undefined;
        this.#dispatch();
        this.#schedule();
      },
      Math.max(0, due - Date.now()),
    );
  }

  #dispatch() {
    const entries = this.#pending.splice(0, this.#maxBatchSize);

    if (entries.length === 0) {
      return;
    }

    this.#inFlight.set(entries, this.#track(entries));
  }

  async #track(entries: Entry[]) {
    try {
      await this.#deliver(entries, this.#abortController.signal);
    } catch (error) {
      this.logger().error('batch delete failed', { error });
    } finally {
      this.#inFlight.delete(entries);
    }
  }

  async #deliver(entries: Entry[], signal: AbortSignal) {
    const now = Date.now();
    const live = entries.filter(entry => {
      if (entry.deadline > now) {
        return true;
      }

      this.#reportLate(entry);

      return false;
    });

    if (live.length === 0) {
      return;
    }

    for (const { entry, error, retryable } of await this.#attempt(
      live,
      signal,
    )) {
      if (!retryable || signal.aborted) {
        this.emit(MESSAGE_DELETE_ERROR, error, entry.message);
      } else if (entry.deadline <= Date.now()) {
        this.#reportLate(entry);
      } else {
        entry.flushAt = Date.now() + this.#retryDelayMs;
        this.#pending.push(entry);
      }
    }

    this.#schedule();
  }

  #reportLate(entry: Entry) {
    this.emit(
      MESSAGE_DELETE_ERROR,
      new Error(
        'The delete missed its deadline; the visibility timeout may have expired and the message may be delivered again',
      ),
      entry.message,
    );
  }

  async #attempt(entries: Entry[], signal: AbortSignal): Promise<Failure[]> {
    const timeoutSignal = AbortSignal.timeout(this.#requestTimeoutMs);

    try {
      if (signal.aborted) {
        throw new Error(
          'SqsBatchDeleteSettler stopped before the delete completed',
        );
      }

      const client = await this.#client();
      const response = await client.send(
        new DeleteMessageBatchCommand({
          QueueUrl: this.#queueUrl,
          Entries: entries.map(
            ({ message }, index): DeleteMessageBatchRequestEntry => ({
              Id: String(index),
              ReceiptHandle: message.ReceiptHandle,
            }),
          ),
        }),
        { abortSignal: AbortSignal.any([signal, timeoutSignal]) },
      );
      const unreported = new Map(
        entries.map((entry, index) => [String(index), entry]),
      );
      const failures: Failure[] = [];

      for (const { Id } of response.Successful ?? []) {
        const entry = unreported.get(Id ?? '');

        if (entry) {
          unreported.delete(Id ?? '');
          this.emit(MESSAGE_DELETED, entry.message);
        }
      }

      for (const failed of response.Failed ?? []) {
        const entry = unreported.get(failed.Id ?? '');

        if (entry) {
          unreported.delete(failed.Id ?? '');
          failures.push({
            entry,
            error: new Error(
              `DeleteMessageBatch entry failed: ${failed.Code}: ${failed.Message}`,
              { cause: failed },
            ),
            retryable: !failed.SenderFault,
          });
        }
      }

      for (const entry of unreported.values()) {
        failures.push({
          entry,
          error: new Error('DeleteMessageBatch response omitted the entry'),
          retryable: true,
        });
      }

      return failures;
    } catch (thrown) {
      const error =
        timeoutSignal.aborted && !signal.aborted
          ? new Error(
              `DeleteMessageBatch timed out after ${this.#requestTimeoutMs}ms`,
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

  /**
   * Resumes accepting deletes after a {@link SqsBatchDeleteSettler.stop}. Emits
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
   * Rejects new deletes, sends the pending ones and waits for every queued and
   * in-flight delete, retries included, to finish. Emits `service:stopped` once
   * all of them are confirmed or have been reported as failed.
   *
   * @throws {Error} If the deletes do not finish within `stopTimeoutMs`, after
   * aborting the remaining requests and emitting `service:stopError`. The
   * messages still not deleted are then reported through
   * `message:deleteError`.
   */
  async stop(): Promise<void> {
    this.#accepting = false;

    try {
      clearTimeout(this.#timer);

      while (this.#pending.length > 0) {
        this.#dispatch();
      }

      await this.#drain();
    } catch (error) {
      this.emit(SERVICE_STOP_ERROR, toError(error));

      throw error;
    }

    this.emit(SERVICE_STOPPED);
  }

  async #drain() {
    const timeout = sleep(this.#stopTimeoutMs);
    const timedOut = await Promise.race([
      this.#untilIdle(),
      this.#expire(timeout),
    ]);

    timeout.cancel();

    if (timedOut) {
      this.#abortController.abort();
      clearTimeout(this.#timer);

      const abandoned = this.#pending.splice(0);
      const error = new Error(
        'SqsBatchDeleteSettler stopped before the delete completed',
      );

      abandoned.forEach(({ message }) =>
        this.emit(MESSAGE_DELETE_ERROR, error, message),
      );

      throw new Error(
        `SqsBatchDeleteSettler did not drain within ${this.#stopTimeoutMs}ms`,
      );
    }
  }

  async #expire(timeout: Promise<void>): Promise<boolean> {
    await timeout;

    return true;
  }

  async #untilIdle(): Promise<boolean> {
    while (
      (this.#pending.length > 0 || this.#inFlight.size > 0) &&
      !this.#abortController.signal.aborted
    ) {
      await Promise.race([
        ...this.#inFlight.values(),
        sleep(this.#retryDelayMs),
      ]);
    }

    return false;
  }
}
