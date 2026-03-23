import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const thresholds = JSON.parse(readFileSync(join(__dirname, '../config/thresholds.json'), 'utf8'));
const weights = JSON.parse(readFileSync(join(__dirname, '../config/weights.json'), 'utf8'));

export function calculateScores(categories) {
  const scoredCategories = categories.map(category => {
    if (category.skipped) {
      return { ...category, score: 0, status: 'unknown' };
    }

    const score = calculateCategoryScore(category);
    const status = getStatus(score);

    return {
      ...category,
      score,
      status
    };
  });

  // Calculate overall score
  let weightedSum = 0;
  let totalWeight = 0;

  scoredCategories.forEach(category => {
    if (!category.skipped && weights[category.key]) {
      weightedSum += category.score * weights[category.key];
      totalWeight += weights[category.key];
    }
  });

  const overallScore = totalWeight > 0 ? Math.round(weightedSum / totalWeight) : 0;
  const overallStatus = getStatus(overallScore);

  return {
    categories: scoredCategories,
    overallScore,
    overallStatus
  };
}

function calculateCategoryScore(category) {
  const threshold = thresholds[category.key];
  const findingCount = category.findings.length;

  if (!threshold) {
    return 0;
  }

  // Handle percentage-based thresholds (like documentation)
  if (threshold.green_pct !== undefined) {
    const documented = category.findings.filter(f => f.hasDescription).length;
    const total = category.findings.length;
    const percentage = total > 0 ? (documented / total) * 100 : 100;
    
    if (percentage >= threshold.green_pct) return 90;
    if (percentage >= threshold.amber_pct) return 65;
    return 30;
  }

  // Handle count-based thresholds
  if (findingCount >= threshold.red[0] && findingCount <= threshold.red[1]) {
    return 30;
  }
  if (findingCount >= threshold.amber[0] && findingCount <= threshold.amber[1]) {
    return 65;
  }
  if (findingCount >= threshold.green[0] && findingCount <= threshold.green[1]) {
    return 90;
  }

  return 0;
}

function getStatus(score) {
  if (score >= 75) return 'green';
  if (score >= 50) return 'amber';
  return 'red';
}