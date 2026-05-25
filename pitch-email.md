Subject: 15-minute dlt demo for the BIPM project work

Dear Prof. Mueller,

I'd like to show a 15-minute demo to the class on a tool that covers the ingestion step most BIPM projects end up hand-rolling.

[dltHub](https://dlthub.com/) is the company behind [dlt](https://github.com/dlt-hub/dlt). dlt is a Python library, not a platform or a SaaS, and there is no vendor account to sign up for. It is open source under Apache 2.0. You `pip install dlt` and run it anywhere. It is used in production at over 3,000 companies and developed by a Berlin team. The library handles pagination, auth, schema inference and evolution, incremental loading, and normalization of nested JSON into relational tables. Configuration is declarative, and one interface loads to DuckDB, BigQuery, Snowflake, ClickHouse, Postgres, Redshift, and filesystem. Docs: <https://dlthub.com/docs>.

The demo focuses on agentic pipeline generation in the dlt AI Workbench. An LLM agent reads an OpenAPI spec or documentation URL and produces a runnable `rest_api` source with endpoints, pagination, auth, and incremental cursors. The output is the same declarative config a human would write, so students can read and edit it.

Setup is `dlt ai init --agent claude` followed by `dlt ai toolkit rest-api-pipeline install`, which adds skills, rules, workflows, and an MCP server to the project. The agent then drives the build through an ordered sequence of slash commands. Each step lines up with a DE practice worth teaching:

1. `/find-source`. Discover the API through conversation, pick endpoints, confirm auth. Practice: source scoping and data contracts.
2. `/create-rest-api-pipeline`. Scaffold with `dlt init`, `dev_mode=True`, `.add_limit(1)` on each resource. Practice: dev/prod parity and short feedback loops.
3. `setup-secrets`. Credentials are written via MCP tools into `secrets.toml` outside the repo, never by the model. Practice: secret hygiene.
4. `/debug-pipeline`. Run, read the error, iterate. Practice: fail fast on the real API instead of mocks.
5. `/validate-data`. Inspect schema, row counts, cursor values, and column types. Practice: data validation before a pipeline is considered done.
6. `/adjust-endpoint`. Drop the dev limits and turn on `incremental=dlt.sources.incremental("updated_at")`. Practice: idempotent incremental loads.
7. `/new-endpoint`. Add more endpoints under the same source. Practice: one auth and pagination config shared across resources.
8. `/view-data`. Explore the dataset through the dlt Dashboard or the Python API. Practice: separation of load and query, and destination portability across DuckDB, BigQuery, Snowflake, Postgres, and others.

The always-on rules in the toolkit keep the agent inside dlt conventions: typed sources, schema evolution, no hand-rolled HTTP loops, secrets via MCP. The result is code a student can read, extend, and defend in a report.

For BIPM students, most of the ingestion work is handled by the library and the toolkit, so project time can go to the analytical or architectural question instead.

I can tailor the demo to an API or destination relevant to the current cohort. Let me know if a session slot works.

Best regards,
Jack
