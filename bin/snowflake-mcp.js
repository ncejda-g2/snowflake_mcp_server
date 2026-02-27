#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// Validate required environment variables
const requiredVars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USERNAME', 'SNOWFLAKE_WAREHOUSE'];
const missingVars = requiredVars.filter(v => !process.env[v]);

if (missingVars.length > 0) {
  console.error('Error: Missing required environment variables:');
  missingVars.forEach(v => console.error(`  - ${v}`));
  console.error('\nPlease set these environment variables before starting the server.');
  console.error('\nExample:');
  console.error('  export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"');
  console.error('  export SNOWFLAKE_USERNAME="user@company.com"');
  console.error('  export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"');
  process.exit(1);
}

// Get the package directory
const packageDir = path.resolve(__dirname, '..');

// Check for uv first, fall back to direct python execution
const uvCheck = spawn('uv', ['--version']);
let useUv = false;
let started = false;

function maybeStart() {
  if (started) return;
  started = true;
  startServer();
}

uvCheck.on('close', (code) => {
  if (code === 0) {
    useUv = true;
  }
  maybeStart();
});

uvCheck.on('error', () => {
  maybeStart();
});

function startServer() {
  let child;

  if (useUv) {
    // Use uv to run the server
    child = spawn('uv', ['--directory', packageDir, 'run', 'snowflake-mcp'], {
      stdio: 'inherit',
      env: process.env
    });
  } else {
    // Check if venv exists
    const venvPath = path.join(packageDir, '.venv');
    const venvExists = fs.existsSync(venvPath);

    if (!venvExists) {
      console.error('Error: Virtual environment not found and uv is not installed.');
      console.error('Please install uv: curl -LsSf https://astral.sh/uv/install.sh | sh');
      console.error('Or manually set up a Python virtual environment in:', packageDir);
      process.exit(1);
    }

    // Use the venv python
    const venvPython = path.join(venvPath, process.platform === 'win32' ? 'Scripts' : 'bin', 'python');
    child = spawn(venvPython, ['-m', 'server.app'], {
      stdio: 'inherit',
      env: process.env,
      cwd: packageDir
    });
  }

  // Handle termination
  child.on('error', (err) => {
    console.error('Failed to start server:', err);
    process.exit(1);
  });

  child.on('close', (code) => {
    process.exit(code || 0);
  });

  // Forward signals
  process.on('SIGINT', () => {
    child.kill('SIGINT');
  });

  process.on('SIGTERM', () => {
    child.kill('SIGTERM');
  });
}
