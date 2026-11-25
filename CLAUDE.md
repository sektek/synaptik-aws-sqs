# CLAUDE.md — @sektek/synaptik-aws-sqs

AWS SQS adapter for `@sektek/synaptik`, following the Gateway/Channel duality: `SqsChannel` publishes events and `SqsGateway` consumes them. The package is built from small injectable components. See `AGENTS.md` for the design boundaries and testing rules, and the Linear project `synaptik-aws-sqs` for the planned components and decisions.

## Commands

```bash
npm run build        # Compile (tsc -p tsconfig.build.json)
npm run test:min     # Run all tests (mocha + tsx/esm, dot reporter)
npm run test:cover   # Coverage via c8
npm run lint -- --no-cache

# Single test file:
npx mocha --import tsx/esm src/path/to/file.spec.ts
```

Tests need ElasticMQ at `SQS_ENDPOINT`. In the workspace dev container it is preset to `http://elasticmq:9324`. Elsewhere, run it locally:

```bash
docker run -d --rm -p 9324:9324 softwaremill/elasticmq-native
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

## Test helper

`ElasticMqQueues` (`test/elasticmq-queues.ts`) wraps an `SQSClient` configured for ElasticMQ (dummy credentials, region `elasticmq`).

| Member | Purpose |
|--------|---------|
| `new ElasticMqQueues({ endpoint? })` | Endpoint defaults to `SQS_ENDPOINT`, then `http://localhost:9324` |
| `client` | The configured `SQSClient` |
| `createQueue({ fifo?, attributes? })` | Creates a uniquely named queue and returns its URL |
| `close()` | Deletes every queue it created and destroys the client |
