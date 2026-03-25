# Order-to-Cash Graph Explorer

A context graph system with an LLM-powered natural language query interface for SAP Order-to-Cash data.

**Live Demo:** [chat-with-graph.vercel.app](https://chat-with-graph.vercel.app)

---

## What It Does

- Ingests SAP O2C JSONL data (Sales Orders, Deliveries, Billing, Journal Entries, Payments, Customers, Products) into an in-memory DuckDB database
- Constructs a graph of ~600 nodes and ~691 edges representing the full O2C flow
- Visualizes the graph in an interactive 3D force-directed view (react-force-graph-3d)
- Provides a conversational chat interface powered by Gemini 2.5 Flash that translates natural language into DuckDB SQL, executes it, and returns grounded natural language answers
- Highlights graph nodes referenced in each query response

---

## Architecture

```
Frontend (React + Vite + Tailwind)
  ├── GraphView.jsx     → 3D force-graph rendering, node inspection, highlight overlay
  └── ChatPanel.jsx     → Chat UI, starter chips, SQL viewer, results table, node highlight toggle

Backend (FastAPI + Python)
  ├── db.py             → DuckDB in-memory views over JSONL files, graph builder, SQL runner
  ├── main.py           → /graph, /chat, /broken-flows endpoints; Gemini API orchestration
  └── ingest.py         → Offline ingestion utility (JSONL → DuckDB persistent file)
```

**Request flow for a chat message:**
1. **Intent classification** — Gemini checks if the query is O2C-domain-relevant and refines vague phrasing
2. **SQL generation** — Gemini generates a DuckDB SELECT using the full schema prompt
3. **SQL execution** — DuckDB runs the query against in-memory views
4. **Auto-fix retry** — If SQL fails, Gemini is called once more with the error message to self-correct
5. **Answer synthesis** — Gemini converts the result rows into a natural language answer
6. **ID extraction** — Node IDs are parsed from result columns to drive graph highlighting

---

## Why These Choices

### DuckDB (not PostgreSQL / SQLite / Neo4j)
The dataset is a set of JSONL files — DuckDB can query them directly with `read_json_auto` without a schema migration step. It runs in-process (no separate server to deploy), handles analytical queries (GROUP BY, STRING_AGG, window functions) efficiently, and is fast enough for this dataset size. A graph database like Neo4j would have been overkill: the relationships here are well-suited to SQL joins, and the "graph" is primarily a visualization layer on top of relational data.

### In-memory views (not a persisted .db file)
The JSONL files are the source of truth. Using views means zero ingestion lag on startup and no file-sync issues on deployment. The dataset is small (~600 nodes, ~700 edges worth of data) so memory is not a constraint.

### Gemini 2.5 Flash (free tier)
Long context window needed to fit the full schema prompt (~3KB). Flash is fast and free-tiered. The three-call pipeline (intent → SQL → answer) keeps each prompt focused and avoids prompt confusion between SQL generation and natural language synthesis.

### react-force-graph-3d
Handles hundreds of nodes without custom layout code. The 3D view naturally separates dense clusters (Customer hub nodes vs. leaf Payment nodes) that would overlap in 2D. Force simulation parameters are tuned (`warmupTicks=120`, `d3AlphaDecay=0.025`) so the graph settles without exploding on load.

---

## LLM Prompting Strategy

**Schema prompt:** The full schema (table names, column names, confirmed join paths, forbidden aliases) is embedded in the system prompt for the SQL generation call. Explicit join chain documentation (`Customer → SalesOrder → Delivery → Billing → JournalEntry → Payment`) prevents the model from hallucinating intermediate joins.

**Forbidden alias list:** The dataset uses non-obvious table names (`outbound_delivery_headers`, `journal_entry_items_accounts_receivable`). The schema prompt includes a `FORBIDDEN TABLE ALIASES` block that maps common guesses to the correct names.

**Consistent output format:** The SQL prompt enforces `COUNT(DISTINCT ...)`, `STRING_AGG(DISTINCT ..., ',')`, and snake_case aliases to ensure that ID extraction from result columns is reliable across runs.

**Intent guard (guardrails):** A separate lightweight Gemini call classifies the query before any SQL is generated. Off-topic prompts (jokes, weather, general knowledge) are rejected at this step with a standard message: *"This system is designed to answer questions related to the provided dataset only."*

---

## Guardrails

- Two-stage LLM pipeline: intent classification blocks off-topic queries before SQL generation
- Only `SELECT` statements are allowed; the backend rejects any SQL not beginning with `SELECT`
- DuckDB runs in-memory with no write access from the chat endpoint
- Node highlighting IDs are extracted from actual query result columns — not from LLM output — to prevent hallucinated graph connections

---

## Local Setup

```bash
# Backend
cd backend
pip install -r requirements.txt
GEMINI_API_KEY=your_key uvicorn main:app --reload

# Frontend
cd frontend
npm install
VITE_API_BASE_URL=http://localhost:8000 npm run dev
```

Dataset path: `backend/sap-o2c-data/` (JSONL files, not committed to repo)

---

## Example Queries

- *Which products are associated with the highest number of billing documents?*
- *Trace the full flow of billing document 90504248*
- *Show sales orders that have been delivered but not billed*
- *Which customer has the highest total order value?*
- *Find journal entries with no corresponding payment*