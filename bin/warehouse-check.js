#!/usr/bin/env node

import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { run } from '../src/cli.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const pkg = JSON.parse(readFileSync(join(__dirname, '../package.json'), 'utf8'));

const args = process.argv.slice(2);

if (args.includes('--version') || args.includes('-v')) {
  console.log(`${pkg.name} v${pkg.version}`);
  process.exit(0);
}

if (args.includes('--help') || args.includes('-h')) {
  console.log(`
${pkg.name} v${pkg.version}
${pkg.description}

Usage:
  warehouse-check                          Interactive mode
  warehouse-check --project my-project     Scan a specific project
  warehouse-check --project p --dataset d  Scan specific dataset(s)
  warehouse-check --quiet                  Score only, no interactive prompts
  warehouse-check --json                   Output results as JSON

Options:
  --project, -p    Google Cloud project ID
  --dataset, -d    Dataset to scan (comma-separated, or repeat flag)
  --quiet, -q      Minimal output — just the score
  --json           Output full results as JSON (implies --quiet)
  --help, -h       Show this help
  --version, -v    Show version

Prerequisites:
  gcloud auth application-default login
  gcloud auth application-default set-quota-project YOUR_PROJECT_ID

Permissions needed:
  bigquery.metadataViewer, bigquery.user

More info: https://github.com/Measurelab/warehouse-check
`);
  process.exit(0);
}

// Parse flags
function getFlag(names) {
  for (const name of names) {
    const idx = args.indexOf(name);
    if (idx !== -1 && idx + 1 < args.length) return args[idx + 1];
  }
  return null;
}

function hasFlag(names) {
  return names.some(n => args.includes(n));
}

const options = {
  project: getFlag(['--project', '-p']),
  dataset: getFlag(['--dataset', '-d']),
  quiet: hasFlag(['--quiet', '-q']),
  json: hasFlag(['--json']),
};

// --json implies --quiet
if (options.json) options.quiet = true;

// Parse comma-separated datasets
if (options.dataset) {
  options.datasets = options.dataset.split(',').map(d => d.trim());
} else {
  options.datasets = null;
}

run(options);
