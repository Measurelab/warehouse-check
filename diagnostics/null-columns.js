import { getTableMetadata, queryPerDataset } from '../src/utils.js';

export async function checkNullColumns(bigquery, projectId, datasets) {
  const findings = [];

  // Get row counts from metadata (no table scans)
  const allTables = await getTableMetadata(bigquery, projectId, datasets);
  const rowCounts = {};
  allTables.forEach(row => {
    if (parseInt(row.row_count) >= 1000 && row.size_bytes > 0) {
      rowCounts[`${row.dataset}.${row.table_name}`] = parseInt(row.row_count);
    }
  });

  // Get nullable columns
  const columnRows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name,
      column_name,
      data_type
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.COLUMNS\`
    WHERE is_nullable = 'YES'
    ORDER BY table_name, ordinal_position
  `);

  // Group columns by table, only for tables with enough rows
  const tableColumns = {};
  columnRows.forEach(row => {
    const tableKey = `${row.dataset}.${row.table_name}`;
    if (!rowCounts[tableKey]) return;
    if (!tableColumns[tableKey]) {
      tableColumns[tableKey] = [];
    }
    tableColumns[tableKey].push(row);
  });

  // Sample up to 50 tables
  const tablesToSample = Object.keys(tableColumns).slice(0, 50);

  for (const tableKey of tablesToSample) {
    const [dataset, tableName] = tableKey.split('.');
    const columns = tableColumns[tableKey];
    const rowCount = rowCounts[tableKey];

    try {
      const columnsToCheck = columns.slice(0, 10);
      if (columnsToCheck.length === 0) continue;

      const nullChecks = columnsToCheck.map(col =>
        `COUNTIF(\`${col.column_name}\` IS NULL) / COUNT(*) as \`${col.column_name}_null_ratio\``
      ).join(',\n    ');

      const nullRatioQuery = `
        SELECT
          ${nullChecks}
        FROM \`${projectId}.${dataset}.${tableName}\`
        TABLESAMPLE SYSTEM (1 PERCENT)
      `;

      const [nullResults] = await bigquery.query({ query: nullRatioQuery });
      const nullRatios = nullResults[0];

      columnsToCheck.forEach(col => {
        const ratioKey = `${col.column_name}_null_ratio`;
        const nullRatio = nullRatios[ratioKey];

        if (nullRatio > 0.8) {
          findings.push({
            dataset,
            tableName,
            columnName: col.column_name,
            dataType: col.data_type,
            nullRatio: Math.round(nullRatio * 100),
            rowCount,
            severity: nullRatio > 0.95 ? 'high' : 'medium'
          });
        }
      });

    } catch (error) {
      console.warn(`  Could not sample table ${tableKey}: ${error.message}`);
    }
  }

  return findings;
}
