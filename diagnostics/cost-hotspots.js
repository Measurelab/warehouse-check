import { detectRegions } from '../src/utils.js';

export async function checkCostHotspots(bigquery, projectId, datasets) {
  const findings = [];

  const regions = await detectRegions(bigquery, datasets);
  const allJobRows = [];

  for (const region of regions) {
    try {
      const [rows] = await bigquery.query({
        query: `
          SELECT
            user_email,
            query,
            total_bytes_processed,
            total_slot_ms,
            creation_time,
            ROUND(total_bytes_processed / POW(1024, 4) * 5.00, 2) as estimated_cost_gbp
          FROM \`${projectId}.region-${region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT\`
          WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND total_bytes_processed > 0
          ORDER BY total_bytes_processed DESC
          LIMIT 50
        `,
        location: region
      });
      allJobRows.push(...rows);
    } catch (error) {
      console.warn(`  Could not fetch cost data for region ${region}: ${error.message}`);
    }
  }

  // Group by query pattern
  const queryPatterns = new Map();

  allJobRows.forEach(row => {
    const normalizedQuery = normalizeQuery(row.query);
    const key = `${normalizedQuery.substring(0, 100)}...`;

    if (!queryPatterns.has(key)) {
      queryPatterns.set(key, {
        pattern: key,
        normalizedQuery,
        executions: [],
        totalCostGBP: 0,
        totalBytesProcessed: 0
      });
    }

    const pattern = queryPatterns.get(key);
    pattern.executions.push({
      userEmail: row.user_email,
      creationTime: row.creation_time,
      bytesProcessed: row.total_bytes_processed,
      costGBP: row.estimated_cost_gbp
    });

    pattern.totalCostGBP += row.estimated_cost_gbp;
    pattern.totalBytesProcessed += row.total_bytes_processed;
  });

  // Analyze patterns and create findings
  queryPatterns.forEach((pattern, key) => {
    const executionCount = pattern.executions.length;
    const avgCostGBP = pattern.totalCostGBP / executionCount;

    if (pattern.totalCostGBP > 10) {
      let category = 'unknown';
      let severity = 'medium';

      if (executionCount === 1 && avgCostGBP > 5) {
        category = 'expensive_rare';
        severity = 'high';
      } else if (executionCount > 10 && avgCostGBP > 1) {
        category = 'expensive_frequent';
        severity = 'high';
      } else if (executionCount > 20) {
        category = 'frequent_unknown_consumer';
        severity = 'medium';
      }

      const uniqueUsers = [...new Set(pattern.executions.map(e => e.userEmail))];

      findings.push({
        queryPattern: pattern.pattern,
        category,
        executionCount,
        totalCostGBP: Math.round(pattern.totalCostGBP * 100) / 100,
        avgCostGBP: Math.round(avgCostGBP * 100) / 100,
        uniqueUsers: uniqueUsers.length,
        topUsers: uniqueUsers.slice(0, 3),
        severity,
        monthlyCostProjectionGBP: Math.round(pattern.totalCostGBP * 100) / 100
      });
    }
  });

  findings.sort((a, b) => b.totalCostGBP - a.totalCostGBP);

  return findings;
}

function normalizeQuery(query) {
  return query
    .replace(/\s+/g, ' ')
    .replace(/'[^']*'/g, "'<string>'")
    .replace(/\b\d{4}-\d{2}-\d{2}\b/g, '<date>')
    .replace(/\d+/g, '<number>')
    .trim()
    .toLowerCase();
}
