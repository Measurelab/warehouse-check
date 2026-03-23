import { getTableMetadata, queryPerDataset } from '../src/utils.js';

export async function checkUnpartitioned(bigquery, projectId, datasets) {
  const findings = [];

  const allTables = await getTableMetadata(bigquery, projectId, datasets);

  // Get partitioned tables
  const partitionedTables = new Set();
  const partitionRows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT DISTINCT
      '${ds}' as dataset,
      table_name
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.PARTITIONS\`
    WHERE partition_id IS NOT NULL
  `);
  partitionRows.forEach(row => {
    partitionedTables.add(`${row.dataset}.${row.table_name}`);
  });

  // Find large unpartitioned base tables
  allTables
    .filter(row =>
      row.table_type === 'BASE TABLE' &&
      row.total_billable_bytes > 1073741824 && // > 1GB
      !partitionedTables.has(`${row.dataset}.${row.table_name}`)
    )
    .sort((a, b) => b.total_billable_bytes - a.total_billable_bytes)
    .slice(0, 100)
    .forEach(row => {
      const estimatedScansPerYear = 52; // assumes weekly full scan
      const costPerGbScan = 5.00 / 1000; // £5 per TB = £0.005 per GB
      const annualCostGBP = row.size_gb * costPerGbScan * estimatedScansPerYear;

      findings.push({
        dataset: row.dataset,
        tableName: row.table_name,
        sizeGb: row.size_gb,
        totalRows: parseInt(row.row_count),
        estimatedAnnualCostGBP: Math.round(annualCostGBP),
        severity: row.size_gb > 100 ? 'high' : 'medium'
      });
    });

  return findings;
}
