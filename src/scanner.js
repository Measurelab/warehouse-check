import { BigQuery } from '@google-cloud/bigquery';
import ora from 'ora';
import { calculateScores } from './scoring.js';
import { generateNarrative } from './narrative.js';

// Import all diagnostics
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

const diagnostics = [
  { name: 'Stale Tables', key: 'stale_tables', fn: checkStaleTables },
  { name: 'Null Columns', key: 'null_columns', fn: checkNullColumns },
  { name: 'Unpartitioned Tables', key: 'unpartitioned', fn: checkUnpartitioned },
  { name: 'Documentation', key: 'documentation', fn: checkDocumentation },
  { name: 'Duplicate Tables', key: 'duplicates', fn: checkDuplicates },
  { name: 'Orphaned Views', key: 'orphaned_views', fn: checkOrphanedViews },
  { name: 'Primary Keys', key: 'primary_keys', fn: checkPrimaryKeys },
  { name: 'Schema Drift', key: 'schema_drift', fn: checkSchemaDrift },
  { name: 'Cost Hotspots', key: 'cost_hotspots', fn: checkCostHotspots },
  { name: 'External Consumers', key: 'external_consumers', fn: checkExternalConsumers }
];

export async function scanWarehouse(projectId, datasets) {
  const bigquery = new BigQuery({ projectId });
  
  const results = {
    projectId,
    timestamp: new Date().toISOString(),
    datasets: datasets || ['all'],
    categories: [],
    overallScore: 0,
    overallStatus: 'unknown',
    narrative: ''
  };

  // Run each diagnostic
  for (const diagnostic of diagnostics) {
    const spinner = ora(`Running ${diagnostic.name} check...`).start();
    
    try {
      const findings = await diagnostic.fn(bigquery, projectId, datasets);
      
      spinner.succeed(`${diagnostic.name} check complete`);
      
      results.categories.push({
        key: diagnostic.key,
        name: diagnostic.name,
        findings,
        score: 0, // Will be calculated in scoring.js
        status: 'unknown',
        details: {}
      });
      
      // Rate limiting delay
      await new Promise(resolve => setTimeout(resolve, 500));
      
    } catch (error) {
      spinner.fail(`${diagnostic.name} check failed: ${error.message}`);
      
      // Add empty result for failed diagnostic
      results.categories.push({
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

  // Calculate scores
  const scoring = calculateScores(results.categories);
  results.overallScore = scoring.overallScore;
  results.overallStatus = scoring.overallStatus;
  results.categories = scoring.categories;

  // Generate narrative
  results.narrative = generateNarrative(results);

  return results;
}