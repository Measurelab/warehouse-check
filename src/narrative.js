export function generateNarrative(results) {
  const { overallScore, categories } = results;
  
  // Get category scores for pattern matching
  const scores = {};
  categories.forEach(cat => {
    scores[cat.key] = cat.score;
  });

  // Pattern matching for narrative generation
  const patterns = [];

  // Documentation & cost patterns
  if (scores.documentation < 50 && scores.cost_hotspots < 50) {
    patterns.push("Your warehouse has grown faster than your team's ability to document it, and you're paying for the confusion.");
  }

  // Stale tables & primary keys
  if (scores.stale_tables < 50 && scores.primary_keys < 50) {
    patterns.push("You have a debris problem compounding a data integrity problem.");
  }

  // High-level assessments
  if (overallScore > 75) {
    patterns.push("Your warehouse is in good shape. A few areas to tighten, but the foundations are solid.");
  } else if (overallScore >= 50) {
    patterns.push("Your warehouse needs attention. The foundations are sound but governance hasn't kept pace with growth.");
  } else {
    patterns.push("Your warehouse has significant structural issues that are likely affecting data reliability and costs.");
  }

  // Specific issues
  if (scores.unpartitioned < 50) {
    patterns.push("Unpartitioned large tables are creating unnecessary scan costs.");
  }

  if (scores.orphaned_views < 50) {
    patterns.push("Broken views suggest incomplete cleanup processes after table changes.");
  }

  if (scores.external_consumers < 50) {
    patterns.push("External systems are accessing your data without clear documentation of dependencies.");
  }

  if (scores.schema_drift < 50) {
    patterns.push("Schema inconsistencies across datasets indicate a lack of standardised data modelling.");
  }

  // Positive patterns
  if (scores.documentation >= 75) {
    patterns.push("Strong documentation practices are evident across your warehouse.");
  }

  if (scores.cost_hotspots >= 75) {
    patterns.push("Query costs appear well-managed with no obvious inefficiencies.");
  }

  // Build narrative
  let narrative = patterns.slice(0, 3).join(' ');
  
  // Add recommendation
  if (overallScore < 50) {
    narrative += " We recommend starting with partitioning large tables and establishing primary keys for frequently-joined tables.";
  } else if (overallScore < 75) {
    narrative += " Focus on documentation and removing stale objects to improve maintainability.";
  } else {
    narrative += " Consider implementing automated monitoring to maintain this level of warehouse health.";
  }

  return narrative || "Warehouse analysis complete. Review individual categories for specific recommendations.";
}