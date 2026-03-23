import { readFileSync, writeFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export async function generateReport(results, outputPath) {
  const template = readFileSync(join(__dirname, '../templates/report.html'), 'utf8');

  const issues = results.categories.filter(c => !c.skipped && (c.status === 'red' || c.status === 'amber'));
  const passing = results.categories.filter(c => !c.skipped && c.status === 'green');
  const skipped = results.categories.filter(c => c.skipped);

  // Sort issues: red first, then amber, then by finding count
  issues.sort((a, b) => {
    if (a.status !== b.status) return a.status === 'red' ? -1 : 1;
    return b.findings.length - a.findings.length;
  });

  // Build issues section
  let issuesSection = '';
  if (issues.length > 0) {
    issuesSection = `
      <div class="issues">
        <div class="section-heading">Needs attention</div>
        ${issues.map(c => buildIssueCard(c)).join('')}
      </div>`;
  }

  // Build passing section
  let passingSection = '';
  if (passing.length > 0) {
    passingSection = `
      <div class="passing">
        <div class="section-heading">Passing</div>
        <div class="passing-list">
          ${passing.map(c => `<div class="passing-item"><span class="tick">✓</span> ${c.name}</div>`).join('')}
        </div>
      </div>`;
  }

  // Build skipped section
  let skippedSection = '';
  if (skipped.length > 0) {
    skippedSection = `
      <div class="section-heading">Skipped</div>
      <div class="skipped-list">
        ${skipped.map(c => `<div class="skipped-item">— ${c.name}</div>`).join('')}
      </div>`;
  }

  let html = template;
  html = html.replace(/{{overallScore}}/g, results.overallScore);
  html = html.replace(/{{overallStatus}}/g, results.overallStatus.toUpperCase());
  html = html.replace(/{{overallStatusLower}}/g, results.overallStatus);
  html = html.replace(/{{projectId}}/g, results.projectId);
  html = html.replace(/{{timestamp}}/g, new Date(results.timestamp).toLocaleString('en-GB'));
  html = html.replace(/{{narrative}}/g, results.narrative);
  html = html.replace(/{{issuesSection}}/g, issuesSection);
  html = html.replace(/{{passingSection}}/g, passingSection);
  html = html.replace(/{{skippedSection}}/g, skippedSection);

  writeFileSync(outputPath, html, 'utf8');
}

function buildIssueCard(category) {
  const previews = category.findings.slice(0, 5);
  const remaining = category.findings.length - previews.length;

  const statLine = buildStatLine(category);

  return `
    <div class="issue-card ${category.status}">
      <div class="issue-header">
        <span class="issue-title">${category.name}</span>
        <span class="issue-badge ${category.status}">${category.status === 'red' ? 'Action needed' : 'Review'}</span>
      </div>
      ${statLine ? `<div class="issue-stat">${statLine}</div>` : ''}
      <ul class="issue-findings">
        ${previews.map(f => `<li>${formatFinding(category.key, f)}</li>`).join('')}
      </ul>
      ${remaining > 0 ? `<div class="issue-more">+ ${remaining} more</div>` : ''}
    </div>`;
}

function buildStatLine(category) {
  const n = category.findings.length;
  switch (category.key) {
    case 'stale_tables': {
      const dead = category.findings.filter(f => f.classification === 'GENUINELY_DEAD').length;
      const totalGb = category.findings.reduce((s, f) => s + (f.sizeGb || 0), 0).toFixed(1);
      return `<strong>${n}</strong> stale tables · <strong>${dead}</strong> genuinely dead · <strong>${totalGb} GB</strong> total`;
    }
    case 'null_columns':
      return `<strong>${n}</strong> columns over 80% null across sampled tables`;
    case 'unpartitioned': {
      const totalCost = category.findings.reduce((s, f) => s + (f.estimatedAnnualCostGBP || 0), 0);
      return `<strong>${n}</strong> large unpartitioned tables · est. <strong>£${totalCost.toLocaleString()}/year</strong> in unnecessary scans`;
    }
    case 'documentation': {
      const undoc = category.findings.filter(f => !f.hasDescription).length;
      const pct = n > 0 ? Math.round((undoc / n) * 100) : 0;
      return `<strong>${undoc}/${n}</strong> tables undocumented (<strong>${pct}%</strong>)`;
    }
    case 'duplicates': {
      const groups = new Set(category.findings.map(f => f.duplicateGroup)).size;
      return `<strong>${n}</strong> tables with duplicate schemas across <strong>${groups}</strong> groups`;
    }
    case 'orphaned_views':
      return `<strong>${n}</strong> views referencing missing tables`;
    case 'primary_keys': {
      const totalGb = category.findings.reduce((s, f) => s + (f.sizeGb || 0), 0).toFixed(1);
      return `<strong>${n}</strong> large tables without primary keys · <strong>${totalGb} GB</strong> total`;
    }
    case 'schema_drift':
      return `<strong>${n}</strong> tables with conflicting schemas across datasets`;
    case 'cost_hotspots': {
      const totalCost = category.findings.reduce((s, f) => s + (f.totalCostGBP || 0), 0);
      return `<strong>${n}</strong> expensive query patterns · <strong>£${totalCost.toFixed(0)}</strong> in last 30 days`;
    }
    case 'external_consumers': {
      const sas = category.findings.reduce((s, f) => s + (f.serviceAccountCount || 0), 0);
      return `<strong>${n}</strong> tables accessed by external systems · <strong>${sas}</strong> service accounts`;
    }
    default:
      return `<strong>${n}</strong> findings`;
  }
}

function formatFinding(key, finding) {
  switch (key) {
    case 'stale_tables':
      return `${finding.dataset}.${finding.tableName} — ${finding.sizeGb} GB, ${finding.classification.toLowerCase().replace(/_/g, ' ')}`;
    case 'null_columns':
      return `${finding.dataset}.${finding.tableName}.${finding.columnName} — ${finding.nullRatio}% null`;
    case 'unpartitioned':
      return `${finding.dataset}.${finding.tableName} — ${finding.sizeGb} GB, ~£${finding.estimatedAnnualCostGBP}/yr`;
    case 'documentation':
      return `${finding.dataset}.${finding.tableName}`;
    case 'duplicates':
      return `${finding.dataset}.${finding.tableName} — ${finding.duplicateCount} identical schemas`;
    case 'orphaned_views':
      return `${finding.dataset}.${finding.viewName} — ${finding.missingTables.length} missing ref(s)`;
    case 'primary_keys':
      return `${finding.dataset}.${finding.tableName} — ${finding.sizeGb} GB, no PK`;
    case 'schema_drift':
      return `${finding.tableName} — ${finding.differences.length} diffs between ${finding.dataset1} / ${finding.dataset2}`;
    case 'cost_hotspots':
      return `£${finding.totalCostGBP} — ${finding.executionCount} runs, ${finding.category.replace(/_/g, ' ')}`;
    case 'external_consumers':
      return `${finding.dataset}.${finding.tableName} — ${finding.consumerCount} consumers, ${finding.accessPattern.replace(/_/g, ' ')}`;
    default:
      return JSON.stringify(finding).substring(0, 120);
  }
}
