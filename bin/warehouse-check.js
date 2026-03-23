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
  warehouse-check              Run interactive warehouse health check
  warehouse-check --help       Show this help
  warehouse-check --version    Show version

Prerequisites:
  gcloud auth application-default login
  gcloud auth application-default set-quota-project YOUR_PROJECT_ID

Permissions needed:
  bigquery.metadataViewer, bigquery.user

More info: https://github.com/Measurelab/warehouse-check
`);
  process.exit(0);
}

run();
