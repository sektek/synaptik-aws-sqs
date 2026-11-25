# @sektek/synaptik-aws-sqs

AWS SQS adapter for Synaptik. Keep it a thin translation layer between SQS messages and the transport-neutral event pipeline.

## Design boundaries

- `SqsChannel` publishes events, `SqsGateway` polls and processes messages, and small injectable components do the rest: transports, the message provider, the event processor and the settlers.
- Components accept their dependencies through `*Component` types resolved with `getComponent`, as a class instance or a plain function. The AWS client is supplied as a `ProviderComponent<SQSClient>`. There is no default client and no environment assumptions.
- A message is deleted only after it was processed successfully. A failed message is left for redelivery after the visibility timeout. Batched deletes must land before that timeout.
- Delivery is at-least-once. Never claim exactly-once.
- `message:sent` means AWS confirmed the message. A message accepted into a local batch is `message:queued`.
- Signal and shutdown handling goes through `ProcessManager` from `@sektek/utility-belt`, never `process.on` in `start()`.
- Component types belong in `src/types/`; use `.js` suffixes in local imports, document exported declarations and public methods, and update `src/index.ts` plus `src/types/index.ts` for public API changes.
- This package owns SDK-based publishing and polling. Lambda-triggered SQS batches belong to `synaptik-aws-lambda`.

## Testing

- Tests run against ElasticMQ, not mocked SDK clients. The workspace dev container provides it at `SQS_ENDPOINT` (default `http://localhost:9324` elsewhere). A component that only delegates to another component can use plain-function fakes.
- Use `ElasticMqQueues` from `test/elasticmq-queues.ts`: it creates uniquely named standard or FIFO queues and deletes them in `close()`. Call `close()` in `afterEach`.
- Run `npm run test:min`, `npm run lint -- --no-cache` and `npm run build`; use `npx mocha --import tsx/esm <file>` for one spec.
- Do not add dependencies without approval.
