#!/usr/bin/env node

const { execSync } = require('child_process');
const path = require('path');

const packageDir = path.resolve(__dirname, '..');

console.log('Setting up Snowflake MCP Server...');

// Check for uv
let hasUv = false;
try {
  execSync('uv --version', { stdio: 'ignore' });
  hasUv = true;
  console.log('✓ Found uv');
} catch {
  console.log('✗ uv not found');
}

// Check for Python
let hasPython = false;
let pythonCmd = 'python3';
try {
  execSync('python3 --version', { stdio: 'ignore' });
  hasPython = true;
  console.log('✓ Found Python 3');
} catch {
  try {
    execSync('python --version', { stdio: 'ignore' });
    pythonCmd = 'python';
    hasPython = true;
    console.log('✓ Found Python');
  } catch {
    console.log('✗ Python not found');
  }
}

if (!hasPython && !hasUv) {
  console.error('\n❌ Error: Neither Python nor uv is installed.');
  console.error('\nPlease install one of the following:');
  console.error('  1. uv (recommended): curl -LsSf https://astral.sh/uv/install.sh | sh');
  console.error('  2. Python 3.12+: https://www.python.org/downloads/');
  process.exit(1);
}

// Install Python dependencies
if (hasUv) {
  console.log('\nInstalling Python dependencies with uv...');
  try {
    execSync('uv sync --frozen', {
      cwd: packageDir,
      stdio: 'inherit'
    });
    console.log('✓ Python dependencies installed');
  } catch (err) {
    console.error('❌ Failed to install Python dependencies');
    process.exit(1);
  }
} else {
  console.log('\n⚠️  Warning: uv not found. You will need to manually set up a virtual environment.');
  console.log('Run the following commands:');
  console.log(`  cd ${packageDir}`);
  console.log(`  ${pythonCmd} -m venv .venv`);
  console.log('  source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate');
  console.log('  pip install -e .');
}

if (hasUv) {
  console.log('\n✓ Snowflake MCP Server setup complete!');
} else {
  console.log('\n⚠️  Setup incomplete — manual steps required above.');
}

console.log('\nTo use the server, set the following environment variables:');
console.log('  export SNOWFLAKE_ACCOUNT="your-account-id"');
console.log('  export SNOWFLAKE_USERNAME="your-email@company.com"');
console.log('  export SNOWFLAKE_WAREHOUSE="YOUR_WAREHOUSE"');
console.log('\nThen run: npx snowflake-readonly-mcp');
