import { BigQuery } from '@google-cloud/bigquery';
import ora from 'ora';
import { calculateScores } from './scoring.js';
import { generateNarrative } from './narrative.js';
import { resetDatasetCache } from './utils.js';

import { checkStaleTables } from '../diagnostics/stale-tables.js';
import { checkNullColumns } from '../diagnostics/null-columns.js';
import { checkUnpartitioned } from '../diagnostics/unpartitioned.js';
import { checkDocumentation } from '../diagnostics/documentation.js';
import { checkDuplicates } from '../diagnostics/duplicates.js';
import { checkOrphanedViews } from '../diagnostics/orphaned-views.js';
import { checkPrimaryKeys } from '../diagnostics/primary-keys.js';
import { checkSchemaDrift } from '../diagnostics/schema-drift.js';
import { checkCostHotspots } from '../diagnostics/cost-hotspots.js';
import { checkExternalConsumers } from '../diagnostics/external-consumers.js';

// Diagnostics grouped by shared dependencies for parallel execution.
// Group A: uses getTableMetadata (shares __TABLES__ cache)
// Group B: uses COLUMNS / VIEWS (independent)
// Group C: uses JOBS_BY_PROJECT (independent)
const groups = [
  {
    label: 'A',
    diagnostics: [
      { name: 'Stale Tables', key: 'stale_tables', fn: checkStaleTables },
      { name: 'Unpartitioned Tables', key: 'unpartitioned', fn: checkUnpartitioned },
      { name: 'Documentation', key: 'documentation', fn: checkDocumentation },
      { name: 'Primary Keys', key: 'primary_keys', fn: checkPrimaryKeys },
      { name: 'Null Columns', key: 'null_columns', fn: checkNullColumns },
    ]
  },
  {
    label: 'B',
    diagnostics: [
      { name: 'Duplicate Tables', key: 'duplicates', fn: checkDuplicates },
      { name: 'Orphaned Views', key: 'orphaned_views', fn: checkOrphanedViews },
      { name: 'Schema Drift', key: 'schema_drift', fn: checkSchemaDrift },
    ]
  },
  {
    label: 'C',
    diagnostics: [
      { name: 'Cost Hotspots', key: 'cost_hotspots', fn: checkCostHotspots },
      { name: 'External Consumers', key: 'external_consumers', fn: checkExternalConsumers },
    ]
  }
];

export async function scanWarehouse(projectId, datasets, { quiet = false } = {}) {
  const bigquery = new BigQuery({ projectId });
  resetDatasetCache();

  const results = {
    projectId,
    timestamp: new Date().toISOString(),
    datasets: datasets || ['all'],
    categories: [],
    overallScore: 0,
    overallStatus: 'unknown',
    narrative: ''
  };

  // Run groups in parallel, diagnostics within each group sequentially
  // (they share API calls / caches within a group)
  const groupPromises = groups.map(group =>
    runGroup(group.diagnostics, bigquery, projectId, datasets, quiet)
  );

  const groupResults = await Promise.all(groupPromises);
  results.categories = groupResults.flat();

  // Sort back to canonical order
  const keyOrder = groups.flatMap(g => g.diagnostics.map(d => d.key));
  results.categories.sort((a, b) => keyOrder.indexOf(a.key) - keyOrder.indexOf(b.key));

  // Calculate scores
  const scoring = calculateScores(results.categories);
  results.overallScore = scoring.overallScore;
  results.overallStatus = scoring.overallStatus;
  results.categories = scoring.categories;

  // Generate narrative
  results.narrative = generateNarrative(results);

  return results;
}

async function runGroup(diagnostics, bigquery, projectId, datasets, quiet) {
  const results = [];

  for (const diagnostic of diagnostics) {
    const spinner = quiet ? null : ora(`Running ${diagnostic.name} check...`).start();

    try {
      const findings = await diagnostic.fn(bigquery, projectId, datasets);

      if (spinner) spinner.succeed(`${diagnostic.name} complete — ${findings.length} findings`);

      results.push({
        key: diagnostic.key,
        name: diagnostic.name,
        findings,
        score: 0,
        status: 'unknown',
        details: {}
      });

    } catch (error) {
      if (spinner) spinner.fail(`${diagnostic.name} failed: ${error.message}`);

      results.push({
        key: diagnostic.key,
        name: diagnostic.name,
        findings: [],
        score: 0,
        status: 'unknown',
        details: { error: error.message },
        skipped: true
      });
    }
  }

  return results;
}
