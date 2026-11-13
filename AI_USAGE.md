# AI Usage Disclosure

## Tools used
- **Google Antigravity** (IDE agent assistant for workflow execution, analysis, tool calling, and automated testing)
- **ChatGPT** (LLM for architectural brainstorming, regex design, and spec clarifications)

## What I used them for
- **Planning & Spec Analysis:** Deconstructing `assignment-a-data/README.md`, schema definitions (`motor_renewal.json`, `policy_feedback.json`), and `dispositions.csv` into a structured multi-phase implementation roadmap.
- **Architecture Discussions:** Comparing deterministic blind indexing strategies (workflow-scoped HMAC-SHA256 vs. global salt) and evaluating SQLite append-only trigger designs.
- **Implementation Scaffolding:** Drafting boilerplate code for CSV header normalization, JSONL event streaming, and Pytest test fixtures.
- **Regex & Validation Helpers:** Formulating strict regex patterns for Indian mobile phone numbers (`+91XXXXXXXXXX`), day-first dates, and ISO timestamp conversions.
- **Debugging & Test Execution:** Running test suites, analyzing collection errors, diagnosing edge-case assertion mismatches, and verifying dataset counts across Drop 1 and Drop 2.
- **Documentation Structuring:** Drafting initial outlines for `solution/README.md` and report format specifications.

## What I wrote or decided myself
- **Core Architecture & Threat Model:** Decided on AES-256-GCM authenticated encryption paired with workflow-scoped HMAC-SHA256 blind indexing (`workflow_id:canonical_phone`) to eliminate plaintext PII while strictly preventing cross-workflow correlation.
- **Database Schema & Invariant Enforcement:** Designed the relational schema, foreign key constraints (`ON DELETE RESTRICT`), and SQLite engine triggers (`BEFORE UPDATE` and `BEFORE DELETE` with `RAISE(FAIL)`) to guarantee the interaction ledger is physically immutable.
- **Schema-Order Walk Rule:** Enforced strict schema-order walking for lead row rejection reasons so that rows with multiple invalid fields fail deterministically on the first schema-defined field.
- **Duplicate Merging Semantics:** Formulated the rule that subsequent duplicate lead rows never overwrite the permanent origin `batch_id`, while non-null fields enrich previously empty optional fields.
- **Retroactive Orphan Resolution:** Architected the retroactive linking mechanism ensuring orphan interactions stored prior to lead arrival are linked as soon as matching leads are ingested.
- **Report Contract Adherence:** Designed the reporting queries to guarantee mutually exclusive bucket math, 4-decimal connectivity rate precision, and zero PII exposure.

## Where the AI was wrong or unhelpful
1. **Pytest Import Path Failure:**
   - *Issue:* Pytest initially failed to collect tests from the repository root with `ModuleNotFoundError: No module named 'ledger'`. The AI initially assumed `PYTHONPATH` was pre-configured in all terminal environments.
   - *Fix:* I diagnosed that external evaluators cloning the repo would hit this error, so we introduced `solution/conftest.py` to auto-inject `solution/src` into `sys.path`.
2. **SQLite In-Memory Test Isolation:**
   - *Issue:* Early test drafts used `sqlite3.connect(":memory:")` across separate fixture connections, causing subsequent transactions to see a blank database because each `:memory:` connection opened an isolated transient instance.
   - *Fix:* I replaced this with URI shared in-memory databases (`file:mem_<uuid>?mode=memory&cache=shared`, `uri=True`) with a persistent keepalive connection.
3. **Python 3.12+ `PARSE_DECLTYPES` Deprecation:**
   - *Issue:* The AI proposed using `sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES` for automatic timestamp parsing, which triggers runtime deprecation warnings in Python 3.12+.
   - *Fix:* I required all timestamps to be normalized explicitly in Python and persisted as canonical ISO-8601 strings, keeping the database layer lean and warning-free.
4. **Mental Math Typo in Test Assertion:**
   - *Issue:* In an early test draft, an AI-generated assertion expected 79 rejections in Drop 1 instead of the verified ground truth of 71.
   - *Fix:* I caught the discrepancy by cross-referencing our independent Python verification script against the synthetic dataset.

## How I verified AI-generated code
- **Line-by-Line Code Review:** Manually reviewed every function, type annotation, and SQL query in `solution/src/ledger/` for correctness, security, and edge-case handling.
- **Automated Pytest Suite:** Executed all 47 tests across 6 test modules (`test_database.py`, `test_security.py`, `test_lead_ingestion.py`, `test_event_ingestion.py`, `test_reporting.py`, `test_cli.py`), achieving 100% pass rate.
- **Independent Dataset Reconciliation:** Ran an independent verification script against the generated synthetic dataset (seed 42, small scale), matching every single lead row, rejected row, duplicate merge, event line, malformed line, duplicate event, and orphan count to the exact integer.
- **Idempotency Verification:** Verified that re-running ingestion on previously processed files produced identical database states without duplicate records or corrupted counters.
