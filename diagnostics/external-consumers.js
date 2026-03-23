import { detectRegions } from '../src/utils.js';

export async function checkExternalConsumers(bigquery, projectId, datasets) {
  const findings = [];

  const regions = await detectRegions(bigquery, datasets);
  const allJobRows = [];

  for (const region of regions) {
    try {
      const [rows] = await bigquery.query({
        query: `
          SELECT
            referenced_tables.dataset_id,
            referenced_tables.table_id,
            job_type,
            user_email,
            creation_time,
            query
          FROM \`${projectId}.region-${region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT\`,
          UNNEST(referenced_tables) as referenced_tables
          WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND referenced_tables.project_id = '${projectId}'
            AND (user_email LIKE '%.iam.gserviceaccount.com'
                 OR job_type IN ('LOAD', 'EXTRACT', 'COPY'))
          ORDER BY creation_time DESC
          LIMIT 200
        `,
        location: region
      });
      allJobRows.push(...rows);
    } catch (error) {
      console.warn(`  Could not fetch consumer data for region ${region}: ${error.message}`);
    }
  }

  // Group by table and consumer
  const tableConsumers = new Map();

  allJobRows.forEach(row => {
    const tableKey = `${row.dataset_id}.${row.table_id}`;
    const consumerKey = `${row.user_email}:${row.job_type}`;

    if (!tableConsumers.has(tableKey)) {
      tableConsumers.set(tableKey, {
        dataset: row.dataset_id,
        tableName: row.table_id,
        consumers: new Map()
      });
    }

    const table = tableConsumers.get(tableKey);

    if (!table.consumers.has(consumerKey)) {
      table.consumers.set(consumerKey, {
        userEmail: row.user_email,
        jobType: row.job_type,
        accessCount: 0,
        firstSeen: row.creation_time,
        lastSeen: row.creation_time,
        isServiceAccount: row.user_email.includes('.iam.gserviceaccount.com')
      });
    }

    const consumer = table.consumers.get(consumerKey);
    consumer.accessCount++;
    if (row.creation_time > consumer.lastSeen) {
      consumer.lastSeen = row.creation_time;
    }
    if (row.creation_time < consumer.firstSeen) {
      consumer.firstSeen = row.creation_time;
    }
  });

  // Create findings
  tableConsumers.forEach((table, tableKey) => {
    const consumers = Array.from(table.consumers.values());
    const serviceAccountConsumers = consumers.filter(c => c.isServiceAccount);
    const extractConsumers = consumers.filter(c => c.jobType === 'EXTRACT');

    if (consumers.length > 0) {
      let accessPattern = 'unknown';
      let severity = 'medium';

      if (serviceAccountConsumers.length > 0) {
        accessPattern = 'service_account_access';
        severity = 'high';
      } else if (extractConsumers.length > 0) {
        accessPattern = 'data_extraction';
        severity = 'medium';
      } else {
        accessPattern = 'external_load';
        severity = 'low';
      }

      findings.push({
        dataset: table.dataset,
        tableName: table.tableName,
        consumerCount: consumers.length,
        serviceAccountCount: serviceAccountConsumers.length,
        extractJobCount: extractConsumers.length,
        accessPattern,
        severity,
        topConsumers: consumers
          .sort((a, b) => b.accessCount - a.accessCount)
          .slice(0, 3)
          .map(c => ({
            email: c.userEmail,
            jobType: c.jobType,
            accessCount: c.accessCount,
            lastSeen: c.lastSeen
          })),
        totalAccessCount: consumers.reduce((sum, c) => sum + c.accessCount, 0)
      });
    }
  });

  findings.sort((a, b) => {
    if (a.severity !== b.severity) {
      const severityOrder = { high: 3, medium: 2, low: 1 };
      return severityOrder[b.severity] - severityOrder[a.severity];
    }
    return b.totalAccessCount - a.totalAccessCount;
  });

  return findings;
}
