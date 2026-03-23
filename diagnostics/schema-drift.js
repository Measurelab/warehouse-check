import { queryPerDataset } from '../src/utils.js';

export async function checkSchemaDrift(bigquery, projectId, datasets) {
  const findings = [];

  // Only meaningful with 2+ datasets
  if (!datasets || datasets.length < 2) {
    return findings;
  }

  const rows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name,
      column_name,
      data_type,
      ordinal_position
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.COLUMNS\`
    ORDER BY table_name, ordinal_position
  `);

  // Group by table name across datasets
  const tablesByName = {};
  rows.forEach(row => {
    if (!tablesByName[row.table_name]) {
      tablesByName[row.table_name] = {};
    }
    if (!tablesByName[row.table_name][row.dataset]) {
      tablesByName[row.table_name][row.dataset] = [];
    }
    tablesByName[row.table_name][row.dataset].push({
      name: row.column_name,
      type: row.data_type,
      position: row.ordinal_position
    });
  });

  // Find tables that exist in multiple datasets with different schemas
  Object.entries(tablesByName).forEach(([tableName, datasetSchemas]) => {
    const datasetNames = Object.keys(datasetSchemas);

    if (datasetNames.length > 1) {
      for (let i = 0; i < datasetNames.length; i++) {
        for (let j = i + 1; j < datasetNames.length; j++) {
          const dataset1 = datasetNames[i];
          const dataset2 = datasetNames[j];
          const schema1 = datasetSchemas[dataset1];
          const schema2 = datasetSchemas[dataset2];

          const differences = findSchemaDifferences(schema1, schema2);

          if (differences.length > 0) {
            findings.push({
              tableName,
              dataset1,
              dataset2,
              differences,
              schema1ColumnCount: schema1.length,
              schema2ColumnCount: schema2.length,
              severity: differences.length > 5 ? 'high' : 'medium'
            });
          }
        }
      }
    }
  });

  return findings;
}

function findSchemaDifferences(schema1, schema2) {
  const differences = [];

  const cols1 = new Map();
  const cols2 = new Map();

  schema1.forEach(col => cols1.set(col.name, col));
  schema2.forEach(col => cols2.set(col.name, col));

  cols1.forEach((col, name) => {
    if (!cols2.has(name)) {
      differences.push({
        type: 'missing_in_schema2',
        columnName: name,
        columnType: col.type
      });
    } else {
      const col2 = cols2.get(name);
      if (col.type !== col2.type) {
        differences.push({
          type: 'type_mismatch',
          columnName: name,
          schema1Type: col.type,
          schema2Type: col2.type
        });
      }
      if (col.position !== col2.position) {
        differences.push({
          type: 'position_mismatch',
          columnName: name,
          schema1Position: col.position,
          schema2Position: col2.position
        });
      }
    }
  });

  cols2.forEach((col, name) => {
    if (!cols1.has(name)) {
      differences.push({
        type: 'missing_in_schema1',
        columnName: name,
        columnType: col.type
      });
    }
  });

  return differences;
}
