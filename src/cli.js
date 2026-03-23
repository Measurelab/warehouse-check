import { resolve } from 'path';
import { writeFileSync } from 'fs';
import inquirer from 'inquirer';
import chalk from 'chalk';
import open from 'open';
import { validateAuth, getDefaultProject } from './auth.js';
import { scanWarehouse } from './scanner.js';
import { generateReport } from './report.js';

export async function run(options = {}) {
  const quiet = options.quiet || false;
  const jsonMode = options.json || false;

  // Validate authentication
  await validateAuth();

  // Resolve project ID
  let projectId = options.project;
  if (!projectId) {
    const defaultProject = await getDefaultProject();

    if (quiet) {
      projectId = defaultProject;
      if (!projectId) {
        console.error('No project specified. Use --project or set gcloud default.');
        process.exit(1);
      }
    } else {
      showHeader();
      const answer = await inquirer.prompt([{
        type: 'input',
        name: 'projectId',
        message: 'Google Cloud project ID:',
        default: defaultProject,
        validate: input => input.trim() !== '' || 'Project ID is required'
      }]);
      projectId = answer.projectId;
    }
  } else if (!quiet) {
    showHeader();
  }

  // Resolve datasets
  let datasets = options.datasets || null;
  if (!datasets && !quiet) {
    const { datasetChoice } = await inquirer.prompt([{
      type: 'list',
      name: 'datasetChoice',
      message: 'Which datasets would you like to scan?',
      choices: [
        { name: 'All datasets in the project', value: 'all' },
        { name: 'Let me choose specific datasets', value: 'select' }
      ]
    }]);

    if (datasetChoice === 'select') {
      const { datasetList } = await inquirer.prompt([{
        type: 'input',
        name: 'datasetList',
        message: 'Enter dataset names (comma-separated):',
        validate: input => input.trim() !== '' || 'At least one dataset is required'
      }]);
      datasets = datasetList.split(',').map(d => d.trim());
    }
  }

  if (!quiet) {
    console.log('\n' + chalk.gray('Scanning ' + projectId + '...') + '\n');
  }

  // Run the scan
  const results = await scanWarehouse(projectId, datasets, { quiet });

  // JSON mode — output and exit
  if (jsonMode) {
    const jsonPath = resolve('warehouse-health-report.json');
    writeFileSync(jsonPath, JSON.stringify(results, null, 2), 'utf8');
    console.log(JSON.stringify({
      score: results.overallScore,
      status: results.overallStatus,
      project: results.projectId,
      reportPath: jsonPath
    }));
    return;
  }

  // Quiet mode — just the score
  if (quiet) {
    console.log(`${results.overallScore}/100 (${results.overallStatus}) — ${projectId}`);
    return;
  }

  // Interactive mode — full output
  console.log('');
  displayResults(results);
  displayOverallScore(results.overallScore, results.overallStatus);

  // Generate reports
  const htmlPath = resolve('warehouse-health-report.html');
  const jsonPath = resolve('warehouse-health-report.json');
  await generateReport(results, htmlPath);
  writeFileSync(jsonPath, JSON.stringify(results, null, 2), 'utf8');

  console.log(`\n${chalk.cyan('Reports generated:')}`);
  console.log(`  HTML: ${htmlPath}`);
  console.log(`  JSON: ${jsonPath}`);

  const { openReport } = await inquirer.prompt([{
    type: 'confirm',
    name: 'openReport',
    message: 'Open report in browser?',
    default: true
  }]);

  if (openReport) {
    await open(htmlPath);
  }

  console.log('\n' + chalk.green('Done.') + '\n');
}

function showHeader() {
  console.log('');
  console.log(chalk.hex('#6dd750').bold('  Measurelab Warehouse Check'));
  console.log(chalk.gray('  Find ghost tables, cost hotspots, and undocumented pipelines'));
  console.log('');
}

function displayResults(results) {
  const issues = results.categories.filter(c => !c.skipped && c.status !== 'green');
  const passing = results.categories.filter(c => !c.skipped && c.status === 'green');
  const skipped = results.categories.filter(c => c.skipped);

  if (issues.length > 0) {
    console.log(chalk.gray('  NEEDS ATTENTION'));
    issues.forEach(c => {
      const icon = c.status === 'red' ? chalk.red('●') : chalk.yellow('●');
      const count = chalk.white.bold(c.findings.length.toString());
      console.log(`  ${icon} ${c.name.padEnd(22)} ${count} findings`);
    });
    console.log('');
  }

  if (passing.length > 0) {
    console.log(chalk.gray('  PASSING'));
    passing.forEach(c => {
      console.log(`  ${chalk.green('✓')} ${c.name}`);
    });
    console.log('');
  }

  if (skipped.length > 0) {
    console.log(chalk.gray('  SKIPPED'));
    skipped.forEach(c => {
      console.log(`  ${chalk.gray('—')} ${c.name}`);
    });
    console.log('');
  }
}

function displayOverallScore(score, status) {
  const color = status === 'green' ? 'green' : status === 'amber' ? 'yellow' : 'red';
  const width = 30;
  const filled = Math.round((score / 100) * width);
  const bar = chalk[color]('█'.repeat(filled)) + chalk.gray('░'.repeat(width - filled));
  console.log(`  ${bar}  ${chalk[color].bold(score + '/100')}`);
}
