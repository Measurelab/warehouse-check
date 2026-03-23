import { queryPerDataset } from '../src/utils.js';

export async function checkDuplicates(bigquery, projectId, datasets) {
  const findings = [];

  const rows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name,
      column_name,
      ordinal_position,
      data_type
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.COLUMNS\`
    ORDER BY table_name, ordinal_position
  `);

  // Group columns by table, excluding date-sharded tables (e.g. events_20210101)
  const dateShardPattern = /^(.+)_\d{8}$/;
  const tables = {};
  rows.forEach(row => {
    // Skip date-sharded tables — they share schemas by design
    if (dateShardPattern.test(row.table_name)) return;

    const tableKey = `${row.dataset}.${row.table_name}`;
    if (!tables[tableKey]) {
      tables[tableKey] = {
        dataset: row.dataset,
        tableName: row.table_name,
        columns: []
      };
    }
    tables[tableKey].columns.push({
      name: row.column_name,
      type: row.data_type,
      position: row.ordinal_position
    });
  });

  // Create column signatures for comparison
  const signatures = {};
  Object.values(tables).forEach(table => {
    const signature = table.columns
      .sort((a, b) => a.position - b.position)
      .map(col => `${col.name}:${col.type}`)
      .join('|');

    if (!signatures[signature]) {
      signatures[signature] = [];
    }
    signatures[signature].push(table);
  });

  // Find duplicates
  Object.entries(signatures).forEach(([signature, tableList]) => {
    if (tableList.length > 1) {
      tableList.forEach((table, index) => {
        findings.push({
          dataset: table.dataset,
          tableName: table.tableName,
          duplicateGroup: signature.substring(0, 50) + '...',
          duplicateCount: tableList.length,
          otherTables: tableList
            .filter((_, i) => i !== index)
            .map(t => `${t.dataset}.${t.tableName}`),
          columnCount: table.columns.length,
          severity: tableList.length > 3 ? 'high' : 'medium'
        });
      });
    }
  });

  return findings;
}
