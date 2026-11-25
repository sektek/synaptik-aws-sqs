import {
  CreateQueueCommand,
  DeleteQueueCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

const DEFAULT_ENDPOINT = 'http://localhost:9324';

/**
 * Options for {@link ElasticMqQueues}.
 */
export type ElasticMqQueuesOptions = {
  /**
   * The ElasticMQ endpoint. Defaults to the `SQS_ENDPOINT` environment
   * variable, then `http://localhost:9324`.
   */
  endpoint?: string;
};

/**
 * Options for {@link ElasticMqQueues.createQueue}.
 */
export type ElasticMqQueueOptions = {
  /** Create a FIFO queue. The queue name gets the required `.fifo` suffix. */
  fifo?: boolean;
  /** Extra queue attributes, such as `VisibilityTimeout`. */
  attributes?: Record<string, string>;
};

/**
 * Test support for running against ElasticMQ. Provides an `SQSClient`
 * configured for it, creates uniquely named queues, and deletes everything
 * it created.
 */
export class ElasticMqQueues {
  #client: SQSClient;
  #queueUrls: string[] = [];

  /**
   * @param opts - The helper options.
   */
  constructor(opts: ElasticMqQueuesOptions = {}) {
    this.#client = new SQSClient({
      endpoint: opts.endpoint ?? process.env.SQS_ENDPOINT ?? DEFAULT_ENDPOINT,
      region: 'elasticmq',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });
  }

  /**
   * The client configured for ElasticMQ.
   *
   * @returns The SQS client.
   */
  get client(): SQSClient {
    return this.#client;
  }

  /**
   * Creates a uniquely named queue and remembers it for cleanup.
   *
   * @param opts - The queue options.
   * @returns The queue URL.
   */
  async createQueue(opts: ElasticMqQueueOptions = {}): Promise<string> {
    const suffix = opts.fifo ? '.fifo' : '';
    const attributes = opts.fifo
      ? { FifoQueue: 'true', ...opts.attributes }
      : { ...opts.attributes };

    const { QueueUrl } = await this.#client.send(
      new CreateQueueCommand({
        QueueName: `test-${globalThis.crypto.randomUUID()}${suffix}`,
        Attributes: attributes,
      }),
    );

    if (!QueueUrl) {
      throw new Error('ElasticMQ did not return a queue URL');
    }

    this.#queueUrls.push(QueueUrl);

    return QueueUrl;
  }

  /**
   * Deletes every queue this helper created and releases the client.
   */
  async close(): Promise<void> {
    const queueUrls = this.#queueUrls.splice(0);

    await Promise.all(
      queueUrls.map(QueueUrl =>
        this.#client.send(new DeleteQueueCommand({ QueueUrl })),
      ),
    );

    this.#client.destroy();
  }
}
