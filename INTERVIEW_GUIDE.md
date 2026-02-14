# Trade Assignment Interview Guide (Detailed Q&A)

This guide is tailored to the current implementation in this repository and is designed for deep technical interviews.

---

## 1) NFR Considerations

### Q1. What were the key NFRs for this assignment?
**A:**
- **Performance:** high-throughput ingest through Kafka for write operations.
- **Scalability:** consumer replicas and partitioned topic for parallel processing.
- **Reliability:** at-least-once delivery model, manual offset commits after successful processing.
- **Availability:** decoupled write path (API accepts and queues) and independent consumer processing.
- **Security:** dependency and static security scans in CI.
- **Maintainability:** layered architecture, schemas for validation, service layer for business rules.
- **Testability:** isolated fixtures, mocked Kafka producer, deterministic API tests.

### Q2. How does the system handle throughput and burst traffic?
**A:**
Write operations (`POST/PUT/DELETE`) are asynchronous. API publishes messages and returns `202 Accepted` quickly, so client latency is bounded by message enqueue time, not DB transaction time.

### Q3. What reliability tradeoff was chosen?
**A:**
The design favors **durable eventual processing** over strict synchronous consistency for writes. Reads are strongly consistent with DB state, while writes become visible after consumer processing.

### Q4. What availability patterns are used?
**A:**
- API and consumer are separate failure domains.
- Kafka buffers writes when DB/consumer is transiently slow.
- Scheduler-based expiry runs independently of external producer traffic.

### Q5. What NFR gaps still exist?
**A:**
- No DLQ strategy yet.
- No idempotency key tracking table.
- No SLO/error budget definitions.
- No explicit rate limiting at API gateway.

---

## 2) Thorough Architecture Review

### Q1. What architecture style is implemented?
**A:**
Hybrid architecture:
- **Event-driven async write path:** FastAPI producer → Kafka → consumer → service → DB.
- **Request/response sync read path:** FastAPI directly queries DB for `GET` endpoints.

### Q2. Why split read and write paths?
**A:**
- Write path handles high volume and decouples ingestion from persistence.
- Read path stays simple and low-latency from relational storage.

### Q3. What are the major layers/components?
**A:**
1. **API Layer** (`app/main.py`): endpoints and producer interaction.
2. **Message Layer** (Kafka topic + consumer): transport and async execution.
3. **Business Layer** (`app/crud.py`): validations and versioning rules.
4. **Data Layer** (`app/models.py`, SQLAlchemy, PostgreSQL): persistence and schema constraints.

### Q4. How does a create request move through the system?
**A:**
1. API validates request via Pydantic schema.
2. API serializes `{operation, data}` message and produces to Kafka.
3. Consumer polls message and routes by operation.
4. `TradeService.create_trade` enforces business rules.
5. DB transaction commits, consumer commits offset.

### Q5. What response semantics are used?
**A:**
- Write endpoints return `202 Accepted` (queued).
- Read endpoints return `200` or `404` based on current DB state.
- Schema-level validation failures return `422` synchronously.

### Q6. What consistency model should interviewer expect?
**A:**
**Eventual consistency on writes**, immediate consistency on reads of persisted data.

---

## 3) Design Considerations Made

### Q1. Why was version handling designed in the service layer?
**A:**
Version logic is domain/business behavior (reject lower, replace same, accept higher), so it belongs in a reusable service instead of being duplicated in endpoint handlers.

### Q2. Why schema validation at API and consumer boundaries?
**A:**
Defense-in-depth:
- API protects direct clients.
- Consumer protects queue-fed input and revalidates payload integrity.

### Q3. Why composite primary key (`trade_id`, `version`)?
**A:**
It models trade version lineage naturally and enforces uniqueness per version without extra surrogate key complexity.

### Q4. Why not process writes synchronously?
**A:**
Synchronous writes create throughput bottlenecks and tighter coupling to DB latency under burst load.

### Q5. Why separate expiry logic from Kafka flow?
**A:**
Expiry is a deterministic DB state transition based on date/time. Running it synchronously via scheduler simplifies correctness and avoids unnecessary queue overhead.

---

## 4) Best Practices and Patterns Used

### Q1. What design patterns are present?
**A:**
- **Service Layer Pattern:** `TradeService` centralizes domain logic.
- **Repository-like data access via ORM session queries.**
- **Dependency Injection:** `get_db` dependency override for tests.
- **Producer-Consumer Pattern:** Kafka decouples command ingest from processing.

### Q2. What coding best practices are visible?
**A:**
- Input schema validation via Pydantic.
- Typed function signatures and clear method boundaries.
- Separation of concerns by files (`main`, `crud`, `models`, `schemas`, `kafka_consumer`).
- Automated quality gates in CI.

### Q3. What operational best practices are included?
**A:**
- Health checks for CI services.
- Coverage reporting.
- Security scans (dependency + static code).

### Q4. Which best practices are partially implemented and can be improved?
**A:**
- Structured logging is partial.
- Retry/backoff and DLQ patterns can be formalized.
- CI lint job currently allows continue-on-error; could be stricter.

---

## 5) Technology and Database Choices (and Why)

### Q1. Why FastAPI?
**A:**
- High developer productivity.
- Native Pydantic validation and automatic OpenAPI docs.
- Good async ecosystem compatibility.

### Q2. Why Kafka?
**A:**
- Handles high-throughput event ingestion.
- Partition-based horizontal scaling.
- Durable log for decoupled producer/consumer lifecycles.

### Q3. Why PostgreSQL?
**A:**
- Strong ACID guarantees for financial-style trade records.
- Reliable relational constraints and indexing.
- Mature SQL tooling and operational ecosystem.

### Q4. Why SQLAlchemy + Pydantic together?
**A:**
- SQLAlchemy maps persistence model and transactions.
- Pydantic enforces API/message contract validation.
- Together they cleanly split persistence concerns from boundary validation.

### Q5. Why containerization?
**A:**
- Reproducible environment for app, DB, Kafka, and CI parity.
- Simplifies onboarding and consistent local setups.

---

## 6) Tables Created and DB Best Practices

### Q1. What tables are created?
**A:**
Primary table: `trades` with columns:
- `trade_id`, `version` (composite primary key)
- `counter_party_id`, `book_id`
- `maturity_date`, `created_date`
- `expired` (boolean)

### Q2. What DB best practices were followed?
**A:**
- Composite PK to enforce record uniqueness at version granularity.
- Non-null constraints for required business fields.
- Explicit boolean flag for lifecycle state (`expired`).
- ORM-level model centralization and schema consistency.

### Q3. What additional DB best practices could be added?
**A:**
- Check constraints (e.g., `version > 0`).
- Index strategy review (query pattern specific).
- Migration tooling standardization (Alembic migration history).
- Soft delete/audit history tables where regulatory traceability is needed.

### Q4. How is data integrity protected today?
**A:**
- Schema-level validation.
- Business-rule checks in service layer.
- Relational constraints in PostgreSQL.

---

## 7) Deep Dive: Class, Sequence, and Design Diagrams

### Q1. What does the architecture diagram communicate?
**A:**
It separates **write ingestion**, **message transport**, **consumer processing**, **business validation**, and **persistence**, while retaining a direct read path for querying.

### Q2. What does the class diagram communicate?
**A:**
- Domain model (`Trade`), DTO/schema classes, service class, and infrastructure components.
- Shows how API, consumer, and service collaborate around shared domain rules.

### Q3. What key point from sequence diagrams should be emphasized?
**A:**
For all write operations, the API acknowledges quickly with `202`, and correctness is enforced in the consumer/service transaction path before offset commit.

### Q4. Why are sequence diagrams valuable in interviews?
**A:**
They clarify **temporal behavior**: where validation occurs, when failures are returned, and how async acknowledgment differs from actual persistence.

### Q5. What does expiry process diagram add?
**A:**
It shows a separate lifecycle path for time-based state transitions (`expired = true`) independent of external write operations.

### Q6. What weaknesses should you candidly mention?
**A:**
- Some diagrams are slightly ahead of implementation detail and should be kept in lockstep.
- Versioning of diagrams with architecture decisions can be tightened.

---

## 8) How Code Quality Was Ensured

### Q1. Which quality controls are implemented?
**A:**
- Unit and endpoint tests.
- Coverage reporting.
- Lint/format checks (`flake8`, `black`, `isort`).
- Security scanning (`safety`, `bandit`).

### Q2. How is code organized for maintainability?
**A:**
Modular file boundaries:
- API contract and transport in endpoint layer.
- Domain rules in service.
- Schema validation in dedicated schema models.
- Persistence model in SQLAlchemy model file.

### Q3. How is regression risk reduced?
**A:**
- Test fixtures isolate DB per test run.
- Kafka producer is mocked in API tests.
- CI runs tests on push and PR events.

### Q4. What could further improve quality?
**A:**
- Type-checking with `mypy`.
- Mutation testing for business-rule robustness.
- Architectural linting/contract tests for event schema evolution.

---

## 9) Test Walkthrough and TDD Approach

### Q1. What does the test suite currently cover?
**A:**
- Health endpoint behavior.
- Write endpoint acceptance and schema validation outcomes.
- Update/delete queuing behavior.
- Edge cases (invalid version, missing fields, date constraints).
- Kafka config default/env behavior in consumer tests.

### Q2. How was TDD reflected in implementation?
**A:**
- Business requirements translated into tests first (versioning, maturity date, expiry semantics).
- Code implemented to satisfy tests.
- Refactoring kept behavior stable under test guardrails.

### Q3. How do you explain async write testing in interviews?
**A:**
Because writes are queued, endpoint tests assert `202 Accepted` and response contract. Consumer/business correctness is validated in consumer-focused tests and service logic tests.

### Q4. What is one strong TDD talking point?
**A:**
Schema rules (e.g., past maturity date invalid) are enforced by tests as executable requirements, reducing ambiguity and preventing future regressions.

---

## 10) Adding New Requirements with TDD (and Demoing Working Code)

Use this as an interview live-walkthrough script.

### Requirement Example
“Add a new rule: updates to a trade are blocked if the trade is already expired, unless only `book_id` is being corrected by an admin flag.”

### Q1. What is your TDD sequence?
**A:**
1. **Red:** Write failing tests for allowed/blocked update scenarios.
2. **Green:** Implement minimal service logic to satisfy tests.
3. **Refactor:** Improve readability, keep all tests green.
4. **Verify:** Run targeted tests, then full suite.

### Q2. Which tests do you add first?
**A:**
- Update expired trade without admin flag → expect failure.
- Update expired trade with admin flag and only `book_id` change → expect success.
- Update expired trade with admin flag but changing other fields → expect failure.

### Q3. Which files likely change?
**A:**
- `tests/test_trades.py` (new API behavior tests)
- `tests/test_kafka_consumer.py` or service-level tests (processing rule)
- `app/schemas.py` (new optional flag field if needed)
- `app/crud.py` (business rule enforcement)
- Potentially `diagrams/sequence_update_trade.puml` (behavior update)

### Q4. How do you demonstrate “working code” in interview?
**A:**
Run:
- `pytest tests/test_trades.py -v`
- `pytest tests/test_kafka_consumer.py -v`
- `pytest tests/ -v`

Then show the passing tests and one curl/API call proving updated behavior.

### Q5. What does success look like?
**A:**
All new tests pass, old behavior remains valid, no regressions in CI checks, and documentation/diagram updates reflect the new rule.

---

## 11) CI/CD Pipeline Walkthrough

### Q1. What are the pipeline stages?
**A:**
1. **Test job:** spins up PostgreSQL, Zookeeper, Kafka; initializes DB; runs pytest with coverage.
2. **Security scan:** Safety + Bandit.
3. **Lint job:** Black/isort/flake8 checks.
4. **Build job:** builds and smoke-tests Docker image.
5. **Deploy job:** placeholder gated to main branch push.

### Q2. How are environment configs managed?
**A:**
GitHub Actions now reads runtime config from repository **Secrets** and **Variables**, avoiding hardcoded credentials in workflow steps.

### Q3. How does pipeline enforce confidence?
**A:**
- Build depends on successful test, scan, and lint jobs.
- Artifacts for security reports are uploaded.
- Coverage report is generated and uploaded.

### Q4. What can be improved in CI/CD?
**A:**
- Make lint/security gates non-optional for stricter governance.
- Add branch protection rules and required status checks.
- Add release versioning and deployment environment approvals.

---

## 12) What Could Be Better with More Time/Resources

### Q1. What are top architectural upgrades?
**A:**
- Introduce outbox pattern for producer reliability.
- Add DLQ + retry backoff strategy for poison messages.
- Add idempotency table keyed by message ID.

### Q2. What are top engineering process upgrades?
**A:**
- Stronger ADR process for design decisions.
- Expand contract tests for event schema evolution.
- Add performance/load tests and chaos/failure injection tests.

### Q3. What are top security upgrades?
**A:**
- Secret rotation policies.
- SAST/DAST integration with policy gates.
- Dependency update automation and SBOM generation.

### Q4. What are top operability upgrades?
**A:**
- Structured logging with correlation IDs.
- Metrics/traces with SLO dashboards.
- Alert routing with on-call runbooks.

---

## 13) Logging Strategy and Observability

### Q1. What should the logging strategy be for this architecture?
**A:**
- **Structured JSON logs** across API and consumer.
- Include `trace_id`, `request_id`, `trade_id`, `version`, `operation`, `topic`, `partition`, `offset`.
- Log key lifecycle events: accepted, consumed, validated, persisted, offset committed, failures.

### Q2. Why is that useful?
**A:**
It enables end-to-end traceability for async flows, faster root-cause analysis, and reliable auditability for business operations.

### Q3. What observability tools would you propose?
**A:**
- **Metrics:** Prometheus + Grafana.
- **Tracing:** OpenTelemetry + Jaeger/Tempo.
- **Logs:** ELK/OpenSearch/Loki stack.
- **Alerting:** Alertmanager/PagerDuty.

### Q4. What critical metrics should be captured?
**A:**
- API request rate/latency/error rate.
- Kafka consumer lag by partition.
- Message processing success/failure counts.
- DB transaction latency and error counts.
- Expiry job execution duration and affected rows.

### Q5. What rules/alerts should be in place?
**A:**
- **Consumer lag threshold:** sustained lag > N for M minutes.
- **Error-rate threshold:** write processing failures > X% over Y minutes.
- **Latency SLO breach:** p95 API/read latency above threshold.
- **Dead letter growth:** DLQ growth above baseline.
- **Scheduler failure:** missed expiry runs or zero-heartbeat.

### Q6. What dashboard views are most useful?
**A:**
- **Executive/SLO dashboard:** uptime, error budget burn, throughput.
- **Service dashboard:** API vs consumer health, lag, retries.
- **Incident dashboard:** traces, top errors, impacted trade IDs.

---

## 14) Interview Closing Script (2–3 minute summary)

### Q. Can you summarize this assignment in a concise executive-style answer?
**A:**
This system uses FastAPI + Kafka + PostgreSQL to balance low-latency write ingestion with reliable processing. Writes are accepted asynchronously (`202`) and executed by consumer-driven business logic with versioning and date validations; reads are synchronous from PostgreSQL for straightforward query performance. The design is layered for maintainability, tested with TDD-oriented cases, and supported by CI checks for quality and security. With more time, the next step would be stronger operational maturity: DLQ/idempotency, structured observability, strict release governance, and production-grade SLO-driven operations.

---

## 15) Rapid-Fire Q&A (for panel rounds)

### Q1. Why not use NoSQL for this use case?
**A:**
Trade records with strict version semantics and relational guarantees fit ACID SQL storage better; PostgreSQL gives robust transactional behavior and schema constraints.

### Q2. Why `202` for write APIs?
**A:**
Because writes are queued and processed asynchronously; `202` correctly represents accepted-but-not-yet-applied semantics.

### Q3. How do you avoid losing messages?
**A:**
Kafka durability + manual commit only after successful DB operation provides at-least-once processing.

### Q4. How do you avoid duplicate side effects?
**A:**
Current design relies on business key/version semantics; stronger idempotency can be added via message-id dedupe table.

### Q5. Where are business rules enforced?
**A:**
Primary enforcement in `TradeService`, with additional schema checks at boundaries.

### Q6. How did TDD help here?
**A:**
Requirements are encoded as tests first, giving confidence during refactor and extension.

### Q7. What is your immediate production hardening plan?
**A:**
Add DLQ/retry policy, structured telemetry, stricter CI gates, and formal SLO + alert runbooks.
