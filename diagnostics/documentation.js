import { getTableMetadata, queryPerDataset } from '../src/utils.js';

export async function checkDocumentation(bigquery, projectId, datasets) {
  const findings = [];

  // Get documented tables
  const documentedRows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name,
      option_value as description
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.TABLE_OPTIONS\`
    WHERE option_name = 'description'
  `);

  // Get all tables with data
  const allTables = await getTableMetadata(bigquery, projectId, datasets);

  // Create set of documented tables
  const documented = new Set();
  documentedRows.forEach(row => {
    documented.add(`${row.dataset}.${row.table_name}`);
  });

  // Check which tables lack documentation
  allTables
    .filter(row => row.total_billable_bytes > 0 && row.table_type !== 'VIEW')
    .forEach(row => {
      const tableKey = `${row.dataset}.${row.table_name}`;
      const hasDescription = documented.has(tableKey);

      findings.push({
        dataset: row.dataset,
        tableName: row.table_name,
        tableType: row.table_type,
        sizeGb: row.size_gb,
        hasDescription,
        severity: !hasDescription && row.size_gb > 10 ? 'high' : 'low'
      });
    });

  return findings;
}
