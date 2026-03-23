# @measurelab/warehouse-check

**BigQuery warehouse health diagnostic — find ghost tables, cost hotspots, and undocumented pipelines.**

Scans your BigQuery project's `INFORMATION_SCHEMA` and `__TABLES__` metadata, scores 10 categories, and generates an HTML report with a narrative summary. No data leaves your machine.

## Quick start

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_PROJECT_ID

npx @measurelab/warehouse-check
```

## CLI options

```
warehouse-check                              Interactive mode
warehouse-check --project my-project         Scan a specific project
warehouse-check -p my-project -d dataset1    Scan specific dataset(s)
warehouse-check --quiet                      Score only, no prompts
warehouse-check --json                       Full results as JSON
```

| Flag | Short | Description |
|------|-------|-------------|
| `--project` | `-p` | Google Cloud project ID |
| `--dataset` | `-d` | Dataset(s) to scan (comma-separated) |
| `--quiet` | `-q` | Minimal output — just the score |
| `--json` | | Output full results as JSON |
| `--help` | `-h` | Show help |
| `--version` | `-v` | Show version |

## What it checks

| Check | What it finds |
|-------|---------------|
| Stale Tables | Tables not modified or queried in 90+ days |
| Null Columns | Columns >80% null (sampled at 1%) |
| Unpartitioned Tables | Large tables without partitioning, with cost estimates |
| Documentation | Tables without descriptions |
| Duplicate Tables | Identical schemas across datasets |
| Orphaned Views | Views referencing deleted tables |
| Primary Keys | Large tables missing PK constraints |
| Schema Drift | Same table name, different schema across datasets |
| Cost Hotspots | Expensive query patterns in last 30 days |
| External Consumers | Service accounts and extract jobs accessing your data |

## Output

- **HTML report** — visual report with narrative, issue cards, and passing checks
- **JSON report** — full structured results for programmatic use
- **CLI score** — quick pass/fail for CI pipelines (`--quiet`)

## Permissions

Read-only access to BigQuery metadata:

- `bigquery.metadataViewer` — read table metadata
- `bigquery.user` — run INFORMATION_SCHEMA queries

The null-columns check samples 1% of table data. All other checks read metadata only.

## Privacy

- All analysis runs locally — your data never leaves your machine
- Only metadata is read (schemas, job history, table stats)
- No external API calls
- Open source — audit the code yourself

## Development

```bash
git clone https://github.com/Measurelab/warehouse-check
cd warehouse-check
npm install
node bin/warehouse-check.js --project your-project
```

## Requirements

- Node.js 18+
- Google Cloud SDK with `gcloud auth application-default login`
- BigQuery API enabled on your project

## License

MIT

---

Made by [Measurelab](https://measurelab.co.uk) — boutique analytics consultancy specialising in BigQuery, GA4, and data engineering.
