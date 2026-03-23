import { exec } from 'child_process';
import { promisify } from 'util';
import chalk from 'chalk';

const execAsync = promisify(exec);

export async function validateAuth() {
  try {
    const { stdout } = await execAsync('gcloud auth application-default print-access-token');
    return stdout.trim();
  } catch (error) {
    console.log(chalk.red('\n❌ Google Cloud authentication required\n'));
    console.log('Please authenticate with Google Cloud first:');
    console.log(chalk.cyan('  gcloud auth application-default login'));
    console.log(chalk.cyan('  gcloud auth application-default set-quota-project YOUR_PROJECT_ID\n'));
    process.exit(1);
  }
}

export async function getDefaultProject() {
  try {
    const { stdout } = await execAsync('gcloud config get-value project');
    return stdout.trim();
  } catch (error) {
    return null;
  }
}