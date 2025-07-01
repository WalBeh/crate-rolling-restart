# CrateDB Kubernetes Cluster Manager with Temporal

A robust, production-ready tool for managing CrateDB cluster restarts in Kubernetes using Temporal workflows for reliability, observability, and fault tolerance.

## Features

- **Temporal Workflows**: Reliable, fault-tolerant cluster restart operations with built-in retry logic
- **Maintenance Windows**: Configurable time windows for automated restart scheduling with operator override capability
- **Sequential Pod Restarts**: Safely restart pods one at a time with health checks between each restart
- **Prestop Hook Detection**: Automatically detects and respects decommissioning utilities in prestop hooks
- **Health Monitoring**: Continuous cluster health monitoring during restart operations
- **Multiple Output Formats**: Support for text, JSON, and YAML output formats
- **Async Execution**: Start long-running workflows asynchronously and monitor progress
- **Comprehensive Logging**: Detailed logging with configurable levels
- **Dry Run Mode**: Preview what would be done without making actual changes

## Architecture

The application is built using Temporal workflows to ensure reliability and observability:

### Components

- **CLI Client** (`cli.py`): Command-line interface that starts workflows
- **Temporal Worker** (`worker.py`): Processes workflows and activities
- **Activities** (`activities.py`): Atomic operations (discover clusters, restart pods, check health)
- **Workflows** (`workflows.py`): Orchestrate activities with proper error handling
- **Models** (`models.py`): Data structures for workflow inputs/outputs
- **Temporal Client** (`temporal_client.py`): Client for executing workflows

### Workflow Architecture

```
MultiClusterRestartWorkflow
├── ClusterDiscoveryActivity
├── For each cluster:
│   └── ClusterRestartWorkflow
│       ├── ClusterValidationActivity
│       ├── For each pod:
│       │   ├── RestartPodActivity
│       │   └── HealthCheckActivity
│       └── FinalHealthCheckActivity
```

## Quick Start

Get up and running in 5 minutes:

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone and setup the project
git clone <repository-url>
cd rr
./scripts/setup.sh

# 3. Start Temporal development server
temporal server start-dev

# 4. Start the worker (in another terminal)
uv run python -m rr.worker

# 5. Run your first restart (in another terminal)
uv run rr restart --context your-k8s-context cluster-name
```

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) - Fast Python package manager
- [Temporal CLI](https://docs.temporal.io/cli) - For running `temporal server start-dev`
- Access to a Kubernetes cluster with CrateDB operator installed

## Installation

### 1. Install Prerequisites

Install uv and Temporal CLI:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Temporal CLI
# On macOS
brew install temporal

# On Linux
curl -sSf https://temporal.download/cli.sh | sh

# On Windows
scoop install temporal
```

### 2. Set up the project

Clone and set up the project:

```bash
cd rr

# Create virtual environment and install dependencies
uv sync

# Install the package in development mode
uv pip install -e .
```

### 3. Start Temporal development server

```bash
# Start Temporal in development mode (simple single-process server)
temporal server start-dev

# This will start:
# - Temporal server on localhost:7233
# - Web UI on localhost:8233
# - Uses local SQLite database
```

### 4. Start the worker

```bash
# In another terminal, start the worker
uv run python -m rr.worker
```

## Setup Verification

Before using the application, verify your setup is working correctly:

```bash
# Run the setup verification script
uv run python tests/test_setup.py
```

This script will:
- Test module imports
- Verify Temporal connection
- Test worker functionality
- Execute a sample workflow

If all tests pass, your setup is ready for use.

## Usage

### Basic Commands

All commands require the `--context` flag to specify the Kubernetes context:

```bash
# Restart specific clusters
uv run rr restart --context prod cluster1 cluster2

# Restart all clusters (with confirmation)
uv run rr restart --context prod all

# Dry run to see what would be done
uv run rr restart --context prod --dry-run cluster1

# Start restart asynchronously
uv run rr restart --context prod --async cluster1
```

### Command Options

```bash
uv run rr [OPTIONS] CLUSTER_NAMES...

Options:
  --kubeconfig PATH          Path to kubeconfig file
  --context TEXT             Kubernetes context to use (REQUIRED)
  --dry-run                  Only show what would be done
  --skip-hook-warning        Skip warning about missing prestop hook
  --output-format [text|json|yaml]  Output format for the report
  --log-level [DEBUG|INFO|WARNING|ERROR|CRITICAL]  Log level
  --temporal-address TEXT    Temporal server address [default: localhost:7233]
  --task-queue TEXT          Temporal task queue name [default: cratedb-operations]
  --async                    Start workflow asynchronously
```

### Workflow Management

```bash
# Check workflow status
uv run rr status <workflow-id>

# List recent workflows
uv run rr list-workflows --limit 20

# Cancel a running workflow
uv run rr cancel <workflow-id>

# Force restart a workflow waiting for maintenance window
uv run rr force-restart <workflow-id>
uv run rr force-restart <workflow-id> --reason "Emergency maintenance required"
```

### Environment Variables

Create a `.env` file in your project root:

```bash
# .env
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_TASK_QUEUE=cratedb-operations
K8S_CONTEXT=prod
KUBECONFIG=/path/to/your/kubeconfig
```

Load environment variables:
```bash
# uv automatically loads .env files
uv run rr restart --context prod cluster1
```

## Development Setup

### 1. Development Environment

```bash
# Clone the repository
git clone <repository-url>
cd rr

# Create development environment with all dependencies
uv sync --dev

# Install pre-commit hooks
uv run pre-commit install
```

### 2. Project Structure

```
rr/
├── pyproject.toml          # Project configuration with uv
├── uv.lock                 # Lockfile for reproducible builds
├── tests/                  # Test files
│   ├── test_setup.py       # Setup verification script
│   ├── test_worker.py      # Simple test worker
│   ├── test_discovery.py   # Cluster discovery tests
│   └── test_restart_workflow.py # Restart workflow tests
├── .env                    # Environment variables (gitignored)
├── rr/
│   ├── __init__.py
│   ├── cli.py              # Click CLI commands
│   ├── main.py             # Entry point
│   ├── models.py           # Data models
│   ├── activities.py       # Temporal activities
│   ├── workflows.py        # Temporal workflows
│   ├── worker.py           # Temporal worker
│   ├── temporal_client.py  # Temporal client
│   └── kubeconfig.py       # Kubernetes config
├── tests/                  # Test files
└── README.md
```

### 3. Running Tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=rr --cov-report=html

# Run specific test file
uv run pytest tests/test_activities.py

# Run with verbose output
uv run pytest -v
```

### 4. Code Quality

```bash
# Format code
uv run ruff format .

# Lint code
uv run ruff check .

# Fix linting issues automatically
uv run ruff check --fix .

# Type checking
uv run mypy rr/
```

### 5. Development Commands

```bash
# Start worker in development mode
uv run python -m rr.worker --log-level DEBUG

# Run CLI in development mode
uv run python -m rr.cli --context dev --dry-run cluster1

# Access Temporal UI (built into temporal server start-dev)
# Visit http://localhost:8233 in your browser
```

## Configuration

### Temporal Configuration

For development, the defaults work with `temporal server start-dev`:

```bash
# Development (default settings)
temporal server start-dev  # Starts on localhost:7233

# Using environment variables for different setups
export TEMPORAL_ADDRESS=localhost:7233  # Default for dev server
export TEMPORAL_TASK_QUEUE=cratedb-operations

# Or using CLI flags for remote servers
uv run rr --temporal-address your-server:7233 --context prod cluster1
```

### Kubernetes Configuration

The tool uses standard Kubernetes configuration resolution:
1. `--kubeconfig` parameter
2. `KUBECONFIG` environment variable (note: if multiple configs, use first one)
3. Default `~/.kube/config`

### Worker Configuration

```bash
# Start worker with custom configuration
uv run python -m rr.worker \
  --temporal-address localhost:7233 \
  --task-queue cratedb-operations \
  --log-level INFO

# For development with temporal server start-dev
uv run python -m rr.worker  # Uses defaults
```

## Development vs Production

### Development Setup

For development, use `temporal server start-dev`:

```bash
# Terminal 1: Start Temporal development server
temporal server start-dev

# Terminal 2: Start worker
uv run python -m rr.worker

# Terminal 3: Test the CLI
uv run rr restart --context dev --dry-run test-cluster
```

### Production Deployment

For production, use a proper Temporal cluster:

```bash
# Connect to existing Temporal cluster
export TEMPORAL_ADDRESS=your-temporal-cluster:7233
uv run python -m rr.worker
```

### Production Build

```bash
# Create production build
uv build

# Install from wheel
uv pip install dist/*.whl
```

### Production Installation

Install the package for production use:

```bash
# Create production build
uv build

# Install from wheel
uv pip install dist/*.whl

# Run worker with production Temporal
TEMPORAL_ADDRESS=temporal:7233 python -m rr.worker
```

## Maintenance Windows

The maintenance window feature allows you to control when cluster restarts can occur, ensuring they only happen during designated maintenance periods. This provides better control over when disruptive operations occur in production environments.

### Key Features

- **Flexible Scheduling**: Define maintenance windows using weekdays, specific dates, or ordinal patterns (e.g., "2nd Tuesday", "last Friday")
- **Multiple Windows**: Configure multiple maintenance windows per cluster for different maintenance schedules
- **Time Zones**: All times are specified in UTC for consistency across environments
- **Minimum Duration**: Configure minimum time requirements to prevent restarts when insufficient time remains
- **Operator Override**: Emergency restarts can be triggered outside maintenance windows via Temporal UI signals
- **Wait Logic**: Automatically waits for the next maintenance window or proceeds if already in one

### Configuration Format

Maintenance windows are configured using TOML files with a clear, readable format:

```toml
# Maintenance Windows Configuration
# All times are in UTC

[cluster-name]
timezone = "UTC"
min_window_duration = 30  # Minimum minutes needed for maintenance

# Regular weekly windows
[[cluster-name.windows]]
time = "18:00-24:00"
weekdays = ["mon", "tue", "wed"]
description = "Evening maintenance window"

# Ordinal day patterns
[[cluster-name.windows]]
time = "02:00-04:00"
ordinal_days = ["2nd tue", "last fri"]
description = "Monthly maintenance slots"
```

### Supported Schedule Patterns

**Weekdays**: Standard day-of-week patterns
- `weekdays = ["mon", "tue", "wed", "thu", "fri"]`
- `weekdays = ["sat", "sun"]` (weekends only)

**Ordinal Days**: Complex scheduling patterns
- `"1st mon"`, `"2nd tue"`, `"3rd wed"`, `"4th thu"`, `"5th fri"`
- `"last mon"`, `"last fri"` (last occurrence in month)

**Time Ranges**: 24-hour format in UTC
- `"09:00-17:00"` (standard business hours)
- `"23:00-01:00"` (crosses midnight)
- `"18:00-24:00"` (evening until end of day)

### Management Commands

```bash
# Create a sample configuration file
rr maintenance create-config -o maintenance-windows.toml

# Check if a cluster is in its maintenance window
rr maintenance check maintenance-windows.toml cluster-name

# Check maintenance window at specific time
rr maintenance check maintenance-windows.toml cluster-name --time "2024-01-15T19:30:00"

# List all configured maintenance windows
rr maintenance list-windows maintenance-windows.toml
```

### Using Maintenance Windows

```bash
# Restart with maintenance window enforcement
rr restart --context prod --maintenance-config maintenance-windows.toml cluster-name

# Override maintenance windows for emergency restarts
rr restart --context prod --ignore-maintenance-windows cluster-name

# Dry run with maintenance window check
rr restart --context prod --maintenance-config maintenance-windows.toml --dry-run cluster-name
```

### Operator Override

When a restart is waiting for a maintenance window, operators can force the restart to proceed in two ways:

#### Option 1: CLI Command (Recommended)
```bash
# Force restart with default reason
rr force-restart <workflow-id>

# Force restart with custom reason
rr force-restart <workflow-id> --reason "Emergency maintenance required"

# Example with actual workflow ID
rr force-restart restart-cluster-name-2024-01-15T10:00:00+00:00 --reason "Critical security patch"
```

#### Option 2: Temporal UI
1. Navigate to the running workflow in Temporal Web UI
2. Send a signal named `force_restart` with an optional reason
3. The workflow will immediately proceed with the restart operation

#### Finding Workflow IDs
Use the list command to find running workflows:
```bash
rr list-workflows
```
Copy the full workflow ID from the output to use with the force-restart command.

### Workflow Behavior

**Inside Maintenance Window**: 
- ✅ Restart proceeds immediately
- Logs: "Proceeding with restart: Current time is within maintenance window"

**Outside Maintenance Window**:
- ⏳ Workflow waits for next maintenance window
- Checks every 5 minutes for window availability
- Logs: "Waiting for maintenance window: Next window starts at 2024-01-15 18:00 UTC"

**Approaching Maintenance Window**:
- ⏳ Waits if less than `min_window_duration` minutes remaining
- Prevents restarts that might not complete within the window
- Logs: "Less than 30 minutes until window opens (15.0 minutes remaining)"

### Example Configuration

```toml
# Production cluster - conservative schedule
[production-cluster]
timezone = "UTC"
min_window_duration = 60

[[production-cluster.windows]]
time = "02:00-04:00"
weekdays = ["sat", "sun"]
description = "Weekend early morning maintenance"

[[production-cluster.windows]]
time = "23:00-01:00"
ordinal_days = ["last fri"]
description = "End of month maintenance"

# Development cluster - more flexible
[dev-cluster]
timezone = "UTC"
min_window_duration = 15

[[dev-cluster.windows]]
time = "12:00-13:00"
weekdays = ["mon", "tue", "wed", "thu", "fri"]
description = "Lunch break maintenance"

# Staging cluster - bi-weekly
[staging-cluster]
timezone = "UTC"
min_window_duration = 45

[[staging-cluster.windows]]
time = "22:00-23:30"
ordinal_days = ["2nd thu", "4th thu"]
description = "Bi-weekly Thursday evening"
```

## How It Works

### Cluster Discovery

1. Searches for CrateDB Custom Resources in Kubernetes
2. Finds corresponding StatefulSets using multiple naming patterns
3. Analyzes prestop hooks for decommissioning utilities
4. Discovers pods using label selectors and owner references

### Restart Process

1. **Validation**: Checks cluster health and configuration
2. **Sequential Restart**: Restarts pods one at a time
3. **Health Monitoring**: Waits for each pod to be ready
4. **Cluster Health**: Verifies cluster health between pod restarts
5. **Final Verification**: Ensures cluster is healthy after all restarts

### Prestop Hook Handling

The tool automatically detects and respects prestop hooks:
- Detects decommissioning utilities (`dc_util`, `dc-util`)
- Extracts timeout values from command arguments
- Adjusts pod restart timeouts accordingly
- Warns about clusters without proper prestop hooks

## Output Formats

### Text Format (Default)
Human-readable tables with summary and details.

### JSON Format
```bash
uv run rr restart --context prod --output-format json cluster1
```

### YAML Format
```bash
uv run rr restart --context prod --output-format yaml cluster1
```

## Error Handling and Reliability

### Temporal Benefits

- **Durability**: Workflows survive worker restarts
- **Retries**: Automatic retry of failed activities
- **Visibility**: Full execution history and current state
- **Timeouts**: Configurable timeouts for all operations

### Failure Recovery

- Individual pod restart failures don't fail the entire workflow
- Health check failures stop the restart process safely
- Partial restart states are preserved and recoverable
- Comprehensive error reporting with context

### Monitoring

- Real-time workflow status through Temporal UI
- Structured logging with correlation IDs
- Progress tracking for long-running operations
- Detailed error messages with troubleshooting context

## Examples

### Basic Usage

```bash
# Restart a single cluster
uv run rr restart --context prod my-cluster

# Restart multiple clusters
uv run rr restart --context prod cluster1 cluster2 cluster3

# Restart all clusters with confirmation
uv run rr restart --context prod all

# Dry run to preview changes
uv run rr restart --context prod --dry-run my-cluster

# Use maintenance windows
uv run rr restart --context prod --maintenance-config maintenance-windows.toml my-cluster

# Emergency restart (ignore maintenance windows)
uv run rr restart --context prod --ignore-maintenance-windows my-cluster
```

### Advanced Usage

```bash
# Use specific kubeconfig and context
uv run rr restart --kubeconfig ~/.kube/prod-config --context prod-cluster my-cluster

# Start async with JSON output
uv run rr restart --context prod --async --output-format json my-cluster

# Debug mode with detailed logging
uv run rr restart --context prod --log-level DEBUG my-cluster

# Skip prestop hook warnings
uv run rr restart --context prod --skip-hook-warning legacy-cluster

# Combine maintenance windows with other options
uv run rr restart --context prod --maintenance-config config.toml --async --output-format json my-cluster
```

### Workflow Management

```bash
# Start async restart
uv run rr restart --context prod --async my-cluster
# Output: Workflow ID: restart-clusters-2024-01-15T10:30:00

# Check status
uv run rr status restart-clusters-2024-01-15T10:30:00

# List recent workflows
uv run rr list-workflows --limit 5

# Cancel if needed
uv run rr cancel restart-clusters-2024-01-15T10:30:00

# Force restart if waiting for maintenance window
uv run rr force-restart restart-clusters-2024-01-15T10:30:00 --reason "Emergency fix required"
```

### Maintenance Window Override Scenario

```bash
# 1. Start restart with maintenance windows configured
uv run rr restart --context prod --maintenance-config maintenance-windows.toml my-cluster
# Output: Workflow started, but waiting for maintenance window...

# 2. Find the waiting workflow
uv run rr list-workflows
# Copy the workflow ID from the output (shows "Running" status)

# 3. Check why it's waiting
uv run rr status restart-my-cluster-2024-01-15T10:30:00
# Shows it's waiting for next maintenance window

# 4. Force the restart due to emergency
uv run rr force-restart restart-my-cluster-2024-01-15T10:30:00 --reason "Critical security patch"
# Output: Force restart signal sent! Workflow should proceed shortly.

# 5. Monitor progress
uv run rr status restart-my-cluster-2024-01-15T10:30:00
# Should now show the restart is proceeding
```

### Using Scripts

Create convenience scripts:

```bash
# scripts/restart-prod.sh
#!/bin/bash
export K8S_CONTEXT=prod
uv run rr restart --context prod "$@"

# scripts/restart-staging.sh
#!/bin/bash
export K8S_CONTEXT=staging
uv run rr restart --context staging "$@"
```

Make executable and use:
```bash
chmod +x scripts/*.sh
./scripts/restart-prod.sh cluster1 cluster2
./scripts/restart-staging.sh --dry-run all
```

## Troubleshooting

### Common Issues

1. **No clusters found**
   - Check if CrateDB operator is installed
   - Verify cluster names and namespaces
   - Use `--log-level DEBUG` for detailed discovery information

2. **Temporal connection failed**
   - Ensure Temporal server is running
   - Check `--temporal-address` parameter
   - Verify network connectivity

3. **Pod restart timeouts**
   - Check if prestop hooks are taking too long
   - Review pod resource limits
   - Verify cluster health before restart

4. **Maintenance window issues**
   - **Workflow stuck waiting**: Use `rr force-restart <workflow-id>` to override
   - **Wrong timezone**: Ensure maintenance config uses UTC times
   - **Config not found**: Check `--maintenance-config` path is correct
   - **Window not triggering**: Use `rr maintenance check config.toml cluster-name` to debug

4. **Health check failures**
   - Check CrateDB cluster status
   - Review cluster logs for errors
   - Ensure sufficient cluster resources

5. **Worker fails with "datetime.now restricted" error**
   - This indicates a module is calling `datetime.now()` during workflow validation
   - Check for imports that might load modules with datetime calls (like loguru)
   - Ensure CLI or other non-workflow modules aren't imported in `__init__.py`
   - Use `workflow.unsafe.imports_passed_through()` for necessary imports

### Debug Mode

Enable debug logging for detailed troubleshooting:
```bash
uv run rr restart --context prod --log-level DEBUG my-cluster
```

This provides:
- Detailed cluster discovery process
- Pod restart step-by-step logging
- Health check details
- Prestop hook analysis
- Kubernetes API call traces

### Development Debugging

```bash
# First, always verify your setup
uv run python tests/test_setup.py

# Test minimal worker
uv run python tests/test_worker.py

# Run with Python debugger
uv run python -m pdb -m rr.cli --context dev cluster1

# Enable asyncio debug mode
uv run python -X dev -m rr.cli --context dev cluster1

# Check Temporal Web UI
open http://localhost:8233

# View workflow history and debugging info
temporal workflow list --limit 10
temporal workflow show --workflow-id your-workflow-id
```

## Contributing

### Development Workflow

1. **Setup development environment**:
   ```bash
   uv sync --dev
   uv run pre-commit install
   ```

2. **Make changes and test**:
   ```bash
   uv run pytest
   uv run ruff check .
   uv run mypy rr/
   ```

3. **Submit changes**:
   ```bash
   git add .
   git commit -m "feat: add new feature"
   git push
   ```

### Adding Dependencies

```bash
# Add runtime dependency
uv add requests

# Add development dependency
uv add --dev pytest

# Add optional dependency
uv add --optional monitoring prometheus-client
```

### Updating Dependencies

```bash
# Update all dependencies
uv sync --upgrade

# Update specific dependency
uv sync --upgrade requests
```

## Performance Tuning

### Worker Configuration

```bash
# Adjust worker concurrency
uv run python -m rr.worker \
  --max-concurrent-activities 20 \
  --max-concurrent-workflows 50
```

### Temporal Optimization

```python
# In worker.py, adjust worker settings
worker = Worker(
    client,
    task_queue=task_queue,
    max_concurrent_activities=50,
    max_concurrent_workflow_tasks=100,
    max_concurrent_local_activities=20,
)
```

## Security

### Production Considerations

1. **Secrets Management**:
   ```bash
   # Use environment variables for sensitive data
   export TEMPORAL_ADDRESS=secure-temporal:7233
   export KUBECONFIG=/secure/path/to/kubeconfig
   ```

2. **Network Security**:
   - Use TLS for Temporal connections
   - Restrict network access to Temporal server
   - Use Kubernetes RBAC for cluster access

3. **Authentication**:
   - Configure Temporal authentication
   - Use service accounts for Kubernetes access
   - Rotate credentials regularly

## Development Tips

### Quick Development Setup

```bash
# Complete setup for new development environment
curl -LsSf https://astral.sh/uv/install.sh | sh  # Install uv
brew install temporal  # Install Temporal CLI (macOS)
git clone <repo> && cd rr  # Clone project
uv sync && uv pip install -e .  # Setup Python environment
temporal server start-dev  # Start Temporal (Terminal 1)
uv run python -m rr.worker  # Start worker (Terminal 2)
```

### Why uv?

- **Speed**: 10-100x faster than pip for package resolution and installation
- **Reliability**: Deterministic dependency resolution with lockfiles
- **Compatibility**: Works with existing Python packaging standards
- **Modern**: Built with Rust for performance and reliability

### Why temporal server start-dev?

- **Simplicity**: Single command to start Temporal server
- **No Docker**: No need for Docker containers or complex setup during development
- **Built-in UI**: Web UI available at http://localhost:8233
- **Easy Reset**: Just restart the command to reset state
- **Fast Development**: Optimized for development workflows with immediate feedback

## License

Apache License 2.0 - See LICENSE file for details
