import { resolve } from 'path';
import inquirer from 'inquirer';
import chalk from 'chalk';
import ora from 'ora';
import open from 'open';
import { validateAuth, getDefaultProject } from './auth.js';
import { scanWarehouse } from './scanner.js';
import { generateReport } from './report.js';

export async function run() {
  // Show Measurelab branding header
  console.log('');
  console.log(chalk.hex('#6dd750').bold('   ╭─────────────────────────────────────╮'));
  console.log(chalk.hex('#6dd750').bold('   │') + chalk.hex('#081f30').bgHex('#6dd750').bold('      MEASURELAB WAREHOUSE CHECK      ') + chalk.hex('#6dd750').bold('│'));
  console.log(chalk.hex('#6dd750').bold('   ╰─────────────────────────────────────╯'));
  console.log('');
  console.log('   🔍 Find ghost tables, cost hotspots, and undocumented pipelines');
  console.log('');

  // Validate authentication
  await validateAuth();
  const defaultProject = await getDefaultProject();

  // Get project ID
  const { projectId } = await inquirer.prompt([
    {
      type: 'input',
      name: 'projectId',
      message: 'Google Cloud project ID:',
      default: defaultProject,
      validate: input => input.trim() !== '' || 'Project ID is required'
    }
  ]);

  // Get datasets to scan
  const { datasetChoice } = await inquirer.prompt([
    {
      type: 'list',
      name: 'datasetChoice',
      message: 'Which datasets would you like to scan?',
      choices: [
        { name: 'All datasets in the project', value: 'all' },
        { name: 'Let me choose specific datasets', value: 'select' }
      ]
    }
  ]);

  let datasets = null;
  if (datasetChoice === 'select') {
    // For now, we'll ask for manual input. A full implementation would query BigQuery for available datasets
    const { datasetList } = await inquirer.prompt([
      {
        type: 'input',
        name: 'datasetList',
        message: 'Enter dataset names (comma-separated):',
        validate: input => input.trim() !== '' || 'At least one dataset is required'
      }
    ]);
    datasets = datasetList.split(',').map(d => d.trim());
  }

  console.log('\\n🚀 Starting warehouse health check...\\n');

  // Run the scan
  const results = await scanWarehouse(projectId, datasets);

  // Display results table
  displayResults(results);

  // Display overall score
  displayOverallScore(results.overallScore, results.overallStatus);

  // Generate and open HTML report
  const reportPath = resolve('warehouse-health-report.html');
  await generateReport(results, reportPath);
  
  console.log(`\\n📄 Report generated: ${chalk.cyan(reportPath)}`);
  
  const { openReport } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'openReport',
      message: 'Open report in browser?',
      default: true
    }
  ]);

  if (openReport) {
    await open(reportPath);
  }

  // Ask about uploading to Measurelab
  const { uploadToMeasurelab } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'uploadToMeasurelab',
      message: 'Upload to Measurelab for expert review?',
      default: false
    }
  ]);

  if (uploadToMeasurelab) {
    console.log('📤 Upload functionality coming soon! For now, please share the report manually.');
    console.log('   Contact: hello@measurelab.co.uk');
  }

  console.log('\\n✅ Warehouse health check complete!\\n');
}

function displayResults(results) {
  console.log('📊 RESULTS SUMMARY\\n');
  
  results.categories.forEach(category => {
    const statusIcon = getStatusIcon(category.status);
    const statusColor = getStatusColor(category.status);
    console.log(`${statusIcon} ${chalk[statusColor](category.name.padEnd(20))} ${chalk[statusColor](category.score.toString().padStart(3))} ${chalk.gray(`(${category.findings.length} findings)`)}`);
  });
  
  console.log('');
}

function displayOverallScore(score, status) {
  const color = getStatusColor(status);
  console.log('🎯 OVERALL WAREHOUSE HEALTH\\n');
  
  // Simple ASCII gauge
  const gauge = createGauge(score);
  console.log(gauge);
  console.log(`\\n   ${chalk[color].bold(score + '/100')} - ${chalk[color](status.toUpperCase())}\\n`);
}

function createGauge(score) {
  const width = 40;
  const filled = Math.round((score / 100) * width);
  const empty = width - filled;
  
  let gauge = '   ';
  for (let i = 0; i < filled; i++) {
    if (score >= 75) gauge += chalk.green('█');
    else if (score >= 50) gauge += chalk.yellow('█');
    else gauge += chalk.red('█');
  }
  for (let i = 0; i < empty; i++) {
    gauge += chalk.gray('░');
  }
  
  return gauge;
}

function getStatusIcon(status) {
  switch (status) {
    case 'green': return '🟢';
    case 'amber': return '🟡';
    case 'red': return '🔴';
    default: return '⚪';
  }
}

function getStatusColor(status) {
  switch (status) {
    case 'green': return 'green';
    case 'amber': return 'yellow';
    case 'red': return 'red';
    default: return 'gray';
  }
}