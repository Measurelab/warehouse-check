// Cache for dataset metadata (populated once per run)
let _datasetCache = null;

/**
 * List datasets with their locations (cached after first call).
 * Returns [{ id, location }] filtered by user selection if provided.
 */
export async function listDatasets(bigquery, datasets) {
  if (!_datasetCache) {
    const [allDatasets] = await bigquery.getDatasets();
    _datasetCache = [];

    for (const ds of allDatasets) {
      try {
        const [metadata] = await ds.getMetadata();
        _datasetCache.push({ id: ds.id, location: metadata.location || 'US' });
      } catch {
        _datasetCache.push({ id: ds.id, location: 'US' });
      }
    }
  }

  if (datasets && datasets.length > 0) {
    return _datasetCache.filter(d => datasets.includes(d.id));
  }
  return _datasetCache;
}

/**
 * Reset the dataset cache (for testing or between runs).
 */
export function resetDatasetCache() {
  _datasetCache = null;
}

/**
 * Detect unique regions across all datasets in the project.
 */
export async function detectRegions(bigquery, datasets) {
  const datasetList = await listDatasets(bigquery, datasets);
  const regions = [...new Set(datasetList.map(d => d.location))];
  return regions.length > 0 ? regions : ['US'];
}

/**
 * Detect the region of the first dataset (legacy compat).
 */
export async function detectRegion(bigquery, projectId) {
  try {
    const [datasets] = await bigquery.getDatasets();
    if (datasets.length > 0) {
      const [metadata] = await datasets[0].getMetadata();
      return metadata.location || 'US';
    }
  } catch (error) {
    console.warn(`Could not detect region: ${error.message}`);
  }
  return 'US';
}

/**
 * Build a dataset WHERE clause for INFORMATION_SCHEMA queries.
 */
export function datasetWhereClause(datasets, { prefix = 'AND' } = {}) {
  if (!datasets || datasets.length === 0) return '';
  const escaped = datasets.map(d => d.replace(/[^a-zA-Z0-9_]/g, ''));
  return `${prefix} table_schema IN (${escaped.map(d => `'${d}'`).join(',')})`;
}

/**
 * Run a query template against each dataset's INFORMATION_SCHEMA and merge results.
 * The queryFn receives (datasetId) and returns a SQL string.
 * Automatically sets the correct location per dataset.
 * Silently skips datasets that don't support the queried view.
 */
export async function queryPerDataset(bigquery, projectId, datasets, queryFn) {
  const datasetList = await listDatasets(bigquery, datasets);
  const allRows = [];
  const skipped = [];

  for (const { id: dsId, location } of datasetList) {
    try {
      const sql = queryFn(dsId);
      const [rows] = await bigquery.query({ query: sql, location });
      rows.forEach(row => {
        if (!row.dataset) row.dataset = dsId;
        allRows.push(row);
      });
    } catch (error) {
      skipped.push(dsId);
    }
  }

  if (skipped.length > 0) {
    console.warn(`  Skipped ${skipped.length}/${datasetList.length} dataset(s)`);
  }

  return allRows;
}

/**
 * Get table metadata for all tables in the project using __TABLES__ (broadly accessible)
 * or INFORMATION_SCHEMA.TABLE_STORAGE (more detail but needs extra permissions).
 * Returns normalised rows: { dataset, table_name, size_bytes, size_gb, row_count, last_modified, table_type }
 */
export async function getTableMetadata(bigquery, projectId, datasets) {
  const datasetList = await listDatasets(bigquery, datasets);
  const allRows = [];

  // Try TABLE_STORAGE first for one dataset to see if it's available
  const testDs = datasetList[0];
  let useTableStorage = false;

  if (testDs) {
    try {
      await bigquery.query({
        query: `SELECT 1 FROM \`${projectId}.${testDs.id}.INFORMATION_SCHEMA.TABLE_STORAGE\` LIMIT 1`,
        location: testDs.location
      });
      useTableStorage = true;
    } catch {
      // Fall back to __TABLES__
    }
  }

  for (const { id: dsId, location } of datasetList) {
    try {
      let rows;
      if (useTableStorage) {
        const sql = `
          SELECT
            '${dsId}' as dataset,
            table_name,
            size_bytes,
            ROUND(size_bytes / POW(1024, 3), 2) as size_gb,
            row_count,
            TIMESTAMP_MILLIS(last_modified_time) as last_modified,
            table_type,
            total_billable_bytes
          FROM \`${projectId}.${dsId}.INFORMATION_SCHEMA.TABLE_STORAGE\`
        `;
        [rows] = await bigquery.query({ query: sql, location });
      } else {
        const sql = `
          SELECT
            '${dsId}' as dataset,
            table_id as table_name,
            size_bytes,
            ROUND(size_bytes / POW(1024, 3), 2) as size_gb,
            row_count,
            TIMESTAMP_MILLIS(last_modified_time) as last_modified,
            CASE type WHEN 1 THEN 'BASE TABLE' WHEN 2 THEN 'VIEW' ELSE 'OTHER' END as table_type,
            size_bytes as total_billable_bytes
          FROM \`${projectId}.${dsId}.__TABLES__\`
        `;
        [rows] = await bigquery.query({ query: sql, location });
      }

      rows.forEach(row => allRows.push(row));
    } catch {
      // Skip datasets that can't be queried (external, etc.)
    }
  }

  return allRows;
}
