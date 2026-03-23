# @measurelab/warehouse-check

**Data warehouse health check — find ghost tables, cost hotspots, and undocumented pipelines in your BigQuery project.**

Warehouse Check scans your BigQuery project for common data warehouse issues that drive up costs and reduce reliability. Get a comprehensive health report in minutes, not weeks.

## Quick Start

```bash
# Authenticate with Google Cloud
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_PROJECT_ID

# Run the warehouse check
npx @measurelab/warehouse-check

# View your report (opens automatically)
```

That's it! Your warehouse health report will be generated and opened in your browser.

## What It Checks

| Category | Description |
|----------|-------------|
| **Stale Tables** | Tables not modified or queried in 90+ days, consuming storage costs |
| **Null Columns** | Columns with >80% null values, indicating schema bloat |
| **Unpartitioned Tables** | Large tables without partitioning, causing expensive full scans |
| **Documentation** | Tables without descriptions, reducing team productivity |
| **Duplicate Tables** | Tables with identical schemas across datasets |
| **Orphaned Views** | Views referencing non-existent tables |
| **Primary Keys** | Tables missing primary key constraints |
| **Schema Drift** | Same table names with different schemas across datasets |
| **Cost Hotspots** | Expensive queries and users driving up your BigQuery bill |
| **External Consumers** | Service accounts and external tools accessing your data |

## Permissions Required

Warehouse Check needs read-only access to your BigQuery metadata:

- **`bigquery.metadataViewer`** role on your project
- **`bigquery.user`** role for running queries

⚠️ **Note**: The null-columns check samples actual data (max 1% of each table) to calculate null ratios. All other checks only read metadata.

## Privacy & Security

- **Your data never leaves your machine** — all analysis runs locally
- **We only read metadata** — table schemas, query history, job logs
- **No external API calls** — except for optional report upload to Measurelab
- **Open source** — MIT license, audit the code yourself

## Upload to Measurelab

After generating your report, you can optionally upload it to Measurelab for expert review. Our data engineering team will:

- Prioritise findings by business impact
- Provide implementation guidance
- Suggest BigQuery optimisation strategies
- Recommend governance frameworks

This is optional — your report works perfectly standalone.

## Development

```bash
git clone https://github.com/Measurelab/warehouse-check
cd warehouse-check
npm install
npm link

# Run locally
warehouse-check
```

## Requirements

- **Node.js** 18+
- **Google Cloud SDK** with authentication configured
- **BigQuery API** enabled on your project

## License

MIT License. See [LICENSE](LICENSE) for details.

---

**Made with ❤️ by [Measurelab](https://measurelab.co.uk)**

Need help optimising your data warehouse? We're a boutique analytics consultancy specialising in BigQuery, GA4, and data engineering.