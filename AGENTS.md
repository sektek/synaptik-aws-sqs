# @sektek/synaptik-aws-sqs

AWS SQS adapter for Synaptik, following the Gateway/Channel duality: `SqsChannel` publishes events and `SqsGateway` consumes them. Keep it a thin translation layer between SQS messages and the transport-neutral event pipeline. See the Linear project `synaptik-aws-sqs` for the planned components and decisions.

## Design boundaries

- `SqsChannel` publishes events, `SqsGateway` polls and processes messages, and small injectable components do the rest: transports, the message provider, the event processor and the settlers.
- Components accept their dependencies through `*Component` types resolved with `getComponent`, as a class instance or a plain function. The AWS client is supplied as a `ProviderComponent<SQSClient>`. There is no default client and no environment assumptions.
- A message is deleted only after it was processed successfully. A failed message is left for redelivery after the visibility timeout. Batched deletes must land before that timeout.
- Delivery is at-least-once. Never claim exactly-once.
- `message:sent` means AWS confirmed the message. A message accepted into a local batch is `message:queued`.
- Signal and shutdown handling goes through `ProcessManager` from `@sektek/utility-belt`, never `process.on` in `start()`.
- Component types belong in `src/types/`; use `.js` suffixes in local imports, document exported declarations and public methods, and update `src/index.ts` plus `src/types/index.ts` for public API changes.
- This package owns SDK-based publishing and polling. Lambda-triggered SQS batches belong to `synaptik-aws-lambda`.

## Commands

```bash
npm run build        # Compile (tsc -p tsconfig.build.json)
npm run test:min     # Run all tests (mocha + tsx/esm, dot reporter)
npm run test:cover   # Coverage via c8
npm run lint -- --no-cache

# Single test file:
npx mocha --import tsx/esm src/path/to/file.spec.ts
```

## Source layout

```
index.ts                  # Public barrel, re-exports src/index.ts
src/
  index.ts                # Package exports
  types/                  # All TypeScript interfaces and component types
  *.ts, *.spec.ts         # Components, with tests co-located
test/
  elasticmq-queues.ts     # ElasticMqQueues test helper
```

The package currently exports no components. They are added by the tickets in the Linear project.

## Testing

- Tests run against ElasticMQ, not mocked SDK clients. In the workspace dev container it is preset to `SQS_ENDPOINT=http://elasticmq:9324`. Elsewhere, run it locally with `docker run -d --rm -p 9324:9324 softwaremill/elasticmq-native` (the helper defaults to `http://localhost:9324`). A component that only delegates to another component can use plain-function fakes.
- Use `ElasticMqQueues` from `test/elasticmq-queues.ts`. Call `close()` in `afterEach`.
- Run `npm run test:min`, `npm run lint -- --no-cache` and `npm run build`.
- Do not add dependencies without approval.

### `ElasticMqQueues`

Wraps an `SQSClient` configured for ElasticMQ (dummy credentials, region `elasticmq`).

| Member | Purpose |
|--------|---------|
| `new ElasticMqQueues({ endpoint? })` | Endpoint defaults to `SQS_ENDPOINT`, then `http://localhost:9324` |
| `client` | The configured `SQSClient` |
| `createQueue({ fifo?, attributes? })` | Creates a uniquely named standard or FIFO queue and returns its URL |
| `close()` | Deletes every queue it created and destroys the client |
