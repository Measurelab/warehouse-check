import { detectRegions, getTableMetadata } from '../src/utils.js';

export async function checkStaleTables(bigquery, projectId, datasets) {
  const findings = [];

  const allTables = await getTableMetadata(bigquery, projectId, datasets);
  const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);

  // Filter to stale tables with data
  const staleTables = allTables.filter(row =>
    row.size_bytes > 0 &&
    row.table_type !== 'VIEW' &&
    new Date(row.last_modified.value || row.last_modified) < ninetyDaysAgo
  );

  // Get last query times across all regions
  const lastQueryTimes = await getLastQueryTimes(bigquery, projectId, datasets);

  staleTables
    .sort((a, b) => b.size_bytes - a.size_bytes)
    .slice(0, 100)
    .forEach(row => {
      const tableKey = `${row.dataset}.${row.table_name}`;
      const lastQueried = lastQueryTimes[tableKey];

      let classification = 'GENUINELY_DEAD';

      if (lastQueried && lastQueried > new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)) {
        classification = 'ACTIVE';
      } else if (lastQueried && lastQueried > ninetyDaysAgo) {
        classification = 'PERIODIC';
      } else if (row.table_name.match(/_(v2|old|backup|temp|staging)$/i)) {
        classification = 'ITERATIVE_COPIES';
      }

      findings.push({
        dataset: row.dataset,
        tableName: row.table_name,
        lastModified: row.last_modified,
        lastQueried,
        sizeGb: row.size_gb,
        rowCount: parseInt(row.row_count),
        classification,
        severity: classification === 'GENUINELY_DEAD' ? 'high' : 'medium'
      });
    });

  return findings;
}

async function getLastQueryTimes(bigquery, projectId, datasets) {
  const regions = await detectRegions(bigquery, datasets);
  const times = {};

  for (const region of regions) {
    try {
      const [rows] = await bigquery.query({
        query: `
          SELECT
            CONCAT(referenced_tables.dataset_id, '.', referenced_tables.table_id) as table_key,
            MAX(creation_time) as last_queried
          FROM \`${projectId}.region-${region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT\`,
          UNNEST(referenced_tables) as referenced_tables
          WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            AND referenced_tables.project_id = '${projectId}'
          GROUP BY referenced_tables.dataset_id, referenced_tables.table_id
        `,
        location: region
      });
      rows.forEach(row => {
        const existing = times[row.table_key];
        const queried = new Date(row.last_queried.value);
        if (!existing || queried > existing) {
          times[row.table_key] = queried;
        }
      });
    } catch (error) {
      console.warn(`  Could not fetch job history for region ${region}: ${error.message}`);
    }
  }

  return times;
}
