export function generateNarrative(results) {
  const { overallScore, categories } = results;

  const cat = {};
  categories.forEach(c => { cat[c.key] = c; });

  const lines = [];

  // Opening assessment
  if (overallScore >= 75) {
    lines.push('Your warehouse is in good shape. A few areas to tighten, but the foundations are solid.');
  } else if (overallScore >= 50) {
    lines.push('Your warehouse needs attention. The foundations are sound but governance hasn\'t kept pace with growth.');
  } else {
    lines.push('Your warehouse has significant structural issues that are likely affecting data reliability and costs.');
  }

  // Stale tables — quote size
  if (cat.stale_tables && cat.stale_tables.score < 50) {
    const n = cat.stale_tables.findings.length;
    const dead = cat.stale_tables.findings.filter(f => f.classification === 'GENUINELY_DEAD').length;
    const totalGb = cat.stale_tables.findings.reduce((s, f) => s + (f.sizeGb || 0), 0);
    if (dead > 0) {
      lines.push(`We found ${n} stale tables (${dead} genuinely dead) totalling ${totalGb.toFixed(0)} GB of storage you're paying for but not using.`);
    } else {
      lines.push(`${n} tables haven't been modified in over 90 days, totalling ${totalGb.toFixed(0)} GB.`);
    }
  }

  // Unpartitioned — quote cost
  if (cat.unpartitioned && cat.unpartitioned.score < 50) {
    const n = cat.unpartitioned.findings.length;
    const totalCost = cat.unpartitioned.findings.reduce((s, f) => s + (f.estimatedAnnualCostGBP || 0), 0);
    lines.push(`${n} large tables lack partitioning — that's an estimated £${totalCost.toLocaleString()} per year in avoidable scan costs.`);
  }

  // Documentation — quote percentage
  if (cat.documentation && cat.documentation.score < 50) {
    const total = cat.documentation.findings.length;
    const undoc = cat.documentation.findings.filter(f => !f.hasDescription).length;
    const pct = total > 0 ? Math.round((undoc / total) * 100) : 0;
    lines.push(`${pct}% of your tables have no description — ${undoc} out of ${total}. This makes onboarding and incident response significantly harder.`);
  }

  // Primary keys
  if (cat.primary_keys && cat.primary_keys.score < 50) {
    const n = cat.primary_keys.findings.length;
    const totalGb = cat.primary_keys.findings.reduce((s, f) => s + (f.sizeGb || 0), 0);
    lines.push(`${n} large tables (${totalGb.toFixed(0)} GB) have no primary key constraints, making data integrity difficult to verify.`);
  }

  // External consumers
  if (cat.external_consumers && cat.external_consumers.score < 50) {
    const n = cat.external_consumers.findings.length;
    const sas = cat.external_consumers.findings.reduce((s, f) => s + (f.serviceAccountCount || 0), 0);
    lines.push(`${n} tables are being accessed by external systems (${sas} service accounts). Without documented dependencies, any schema change risks breaking downstream pipelines.`);
  }

  // Duplicates
  if (cat.duplicates && cat.duplicates.score < 50) {
    const groups = new Set(cat.duplicates.findings.map(f => f.duplicateGroup)).size;
    lines.push(`We found ${groups} groups of tables with identical schemas — likely copies that have drifted from a single source of truth.`);
  }

  // Orphaned views
  if (cat.orphaned_views && cat.orphaned_views.score < 50) {
    const n = cat.orphaned_views.findings.length;
    lines.push(`${n} views reference tables that no longer exist — leftover from incomplete migrations or cleanup.`);
  }

  // Schema drift
  if (cat.schema_drift && cat.schema_drift.score < 50) {
    const n = cat.schema_drift.findings.length;
    lines.push(`${n} tables have conflicting schemas across datasets, indicating a lack of standardised data modelling.`);
  }

  // Null columns
  if (cat.null_columns && cat.null_columns.score < 50) {
    const n = cat.null_columns.findings.length;
    lines.push(`${n} columns are over 80% null — schema bloat that adds complexity without value.`);
  }

  // Positive callouts
  const goods = [];
  if (cat.cost_hotspots && cat.cost_hotspots.score >= 75) goods.push('query costs are well-managed');
  if (cat.orphaned_views && cat.orphaned_views.score >= 75) goods.push('no orphaned views');
  if (cat.schema_drift && cat.schema_drift.score >= 75) goods.push('consistent schemas across datasets');
  if (cat.documentation && cat.documentation.score >= 75) goods.push('strong documentation practices');

  if (goods.length > 0) {
    lines.push(`On the positive side: ${goods.join(', ')}.`);
  }

  // Closing recommendation
  if (overallScore < 50) {
    lines.push('We\'d recommend starting with partitioning the largest tables and establishing primary keys — the cost savings alone typically justify the effort.');
  } else if (overallScore < 75) {
    lines.push('Focus on documentation and removing stale objects to improve maintainability.');
  } else {
    lines.push('Consider implementing automated monitoring to maintain this level of warehouse health.');
  }

  return lines.join(' ');
}
