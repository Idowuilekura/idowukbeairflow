# Codex DuckDB dbt Demo

This is a small dbt project that runs locally against DuckDB. It loads a seed
CSV of raw orders, cleans the data in a staging model, and builds a customer
order summary mart.

## Project Structure

```text
.
├── dbt_project.yml
├── profiles.yml
├── pyproject.toml
├── seeds/
│   └── raw_orders.csv
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   └── schema.yml
│   └── marts/
│       ├── customer_order_summary.sql
│       └── schema.yml
└── tests/
    └── assert_completed_revenue_non_negative.sql
```

## What It Builds

- `raw_orders`: seed table loaded from `seeds/raw_orders.csv`
- `stg_orders`: staging view with typed and cleaned order fields
- `customer_order_summary`: mart table with customer-level order counts,
  completed order counts, revenue, and first/last order dates

The DuckDB database is written to:

```text
codex_duckdb_demo.duckdb
```

## Requirements

This project uses `uv` to create an isolated Python environment and install dbt.

The dbt dependencies are pinned in `pyproject.toml`:

- `dbt-core>=1.8,<1.9`
- `dbt-duckdb>=1.8,<1.9`
- `dbt-adapters<1.9`

These versions were selected because they work together cleanly for this local
DuckDB setup.

## Run The Project

From the project root, first verify the dbt profile:

```bash
uv run dbt debug --profiles-dir .
```

Load the seed data:

```bash
uv run dbt seed --profiles-dir .
```

Run the models:

```bash
uv run dbt run --profiles-dir .
```

Run the tests:

```bash
uv run dbt test --profiles-dir .
```

Or run the full pipeline in one command:

```bash
uv run dbt build --profiles-dir .
```

## Tests

The project includes schema tests for:

- non-null order IDs, customer IDs, amounts, and mart metrics
- unique order IDs in `stg_orders`
- unique customer IDs in `customer_order_summary`
- accepted status values: `completed`, `returned`, `cancelled`

It also includes one custom data test:

```text
tests/assert_completed_revenue_non_negative.sql
```

That test fails if any customer has negative completed revenue.

## Verified Result

The full dbt build was run successfully:

```text
PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13
```

You can inspect the final mart with DuckDB from Python:

```bash
uv run python -c "import duckdb; con = duckdb.connect('codex_duckdb_demo.duckdb'); print(con.sql('select * from customer_order_summary order by customer_id').fetchall())"
```
