import { queryPerDataset } from '../src/utils.js';

export async function checkOrphanedViews(bigquery, projectId, datasets) {
  const findings = [];

  const viewRows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name,
      view_definition
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.VIEWS\`
  `);

  const tableRows = await queryPerDataset(bigquery, projectId, datasets, (ds) => `
    SELECT
      '${ds}' as dataset,
      table_name
    FROM \`${projectId}.${ds}.INFORMATION_SCHEMA.TABLES\`
  `);

  // Create set of existing tables
  const existingTables = new Set();
  tableRows.forEach(row => {
    existingTables.add(`${row.dataset}.${row.table_name}`);
  });

  viewRows.forEach(row => {
    const viewDefinition = row.view_definition;
    const referencedTables = extractTableReferences(viewDefinition, projectId);
    const missingTables = [];

    referencedTables.forEach(tableRef => {
      if (!existingTables.has(tableRef)) {
        missingTables.push(tableRef);
      }
    });

    if (missingTables.length > 0) {
      findings.push({
        dataset: row.dataset,
        viewName: row.table_name,
        missingTables,
        referencedTablesCount: referencedTables.length,
        severity: missingTables.length === referencedTables.length ? 'high' : 'medium'
      });
    }
  });

  return findings;
}

function extractTableReferences(viewDefinition, projectId) {
  const references = [];

  const tablePattern = new RegExp(`\`${projectId}\\.(\\w+)\\.(\\w+)\``, 'g');
  let match;

  while ((match = tablePattern.exec(viewDefinition)) !== null) {
    references.push(`${match[1]}.${match[2]}`);
  }

  const unqualifiedPattern = /FROM\s+`(\w+)\.(\w+)`/gi;
  while ((match = unqualifiedPattern.exec(viewDefinition)) !== null) {
    references.push(`${match[1]}.${match[2]}`);
  }

  return [...new Set(references)];
}
