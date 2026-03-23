import { getTableMetadata, queryPerDataset } from '../src/utils.js';

export async function checkPrimaryKeys(bigquery, projectId, datasets) {
  const findings = [];

  // Get tables with primary keys
  const primaryKeyRows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS\`
    WHERE constraint_type = 'PRIMARY KEY'
  `);

  // Get all large base tables
  const allTables = await getTableMetadata(bigquery, projectId, datasets);

  // Create set of tables with primary keys
  const tablesWithPK = new Set();
  primaryKeyRows.forEach(row => {
    tablesWithPK.add(`${row.dataset}.${row.table_name}`);
  });

  // Find large base tables without primary keys
  allTables
    .filter(row =>
      row.table_type === 'BASE TABLE' &&
      row.total_billable_bytes > 104857600 // > 100MB
    )
    .sort((a, b) => b.total_billable_bytes - a.total_billable_bytes)
    .forEach(row => {
      const tableKey = `${row.dataset}.${row.table_name}`;

      if (!tablesWithPK.has(tableKey)) {
        findings.push({
          dataset: row.dataset,
          tableName: row.table_name,
          sizeGb: row.size_gb,
          totalRows: parseInt(row.row_count),
          tableType: row.table_type,
          severity: row.size_gb > 10 ? 'high' : 'medium'
        });
      }
    });

  return findings;
}
