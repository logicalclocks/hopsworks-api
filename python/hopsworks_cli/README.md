# Hopsworks CLI

Command-line interface for managing Hopsworks resources including models, deployments, feature groups, and more.

## Installation

Install the Hopsworks CLI with the CLI extra dependencies:

```bash
pip install "hopsworks[cli]"
```

Or install in development mode:

```bash
cd python
uv sync --extra cli
source .venv/bin/activate
```

## Quick Start

1. **Get your API key** from the Hopsworks UI
2. **Save it to a file** (recommended):
   ```bash
   echo "your-api-key-here" > ~/.hopsworks/api_key
   ```

3. **Run your first command**:
   ```bash
   hopsworks-cli --project my_project --api-key-file ~/.hopsworks/api_key models list --name my_model
   ```

## Authentication

The CLI supports multiple authentication methods (in order of priority):

1. **Command-line arguments**: `--api-key-file` or `--api-key`
2. **Environment variables**: `HOPSWORKS_API_KEY` or `HOPSWORKS_API_KEY_FILE`
3. **Configuration file**: `~/.hopsworks/config.yaml`

### Using Environment Variables

```bash
export HOPSWORKS_HOST="app.hopsworks.ai"
export HOPSWORKS_PROJECT="my_project"
export HOPSWORKS_API_KEY_FILE=~/.hopsworks/api_key

hopsworks-cli models list
```

### Using Configuration Profiles

Create `~/.hopsworks/config.yaml`:

```yaml
default_profile: production

profiles:
  production:
    host: app.hopsworks.ai
    port: 443
    project: prod_project
    api_key_file: ~/.hopsworks/prod_api_key

  development:
    host: dev.hopsworks.ai
    port: 443
    project: dev_project
    api_key_file: ~/.hopsworks/dev_api_key
```

Then use profiles:

```bash
hopsworks-cli --profile production models list
hopsworks-cli --profile development deployments list
```

## Commands

### Global Options

```bash
hopsworks-cli [OPTIONS] COMMAND [ARGS]...

Global Options:
  --host TEXT                  Hopsworks hostname [default: app.hopsworks.ai]
  --port INTEGER               Hopsworks port [default: 443]
  --project TEXT               Project name
  --api-key TEXT               API key value
  --api-key-file PATH          Path to API key file
  --profile TEXT               Configuration profile name
  --output FORMAT              Output format: json|table|yaml [default: table]
  --verbose                    Enable verbose output
  --no-verify                  Disable SSL verification
  --help                       Show help message
```

### Models

Manage models in the model registry.

#### List Models

```bash
# List all versions of a specific model
hopsworks-cli models list --name my_model

# Output as JSON
hopsworks-cli --output json models list --name my_model
```

#### Get Model Details

```bash
hopsworks-cli models get my_model --version 1

# With JSON output
hopsworks-cli --output json models get my_model --version 1
```

#### Get Best Model by Metric

```bash
# Get model with highest accuracy
hopsworks-cli models best my_model --metric accuracy --direction max

# Get model with lowest loss
hopsworks-cli models best my_model --metric loss --direction min
```

#### Download Model

```bash
# Download to current directory
hopsworks-cli models download my_model --version 1

# Download to specific directory
hopsworks-cli models download my_model --version 1 --output-dir ./models
```

#### Get Comprehensive Model Info

```bash
hopsworks-cli models info my_model --version 1
```

### Deployments

Manage model deployments for serving.

#### List Deployments

```bash
hopsworks-cli deployments list

# With JSON output
hopsworks-cli --output json deployments list
```

#### Get Deployment Details

```bash
hopsworks-cli deployments get my_deployment
```

#### Start Deployment

```bash
# Start and return immediately
hopsworks-cli deployments start my_deployment

# Start and wait for it to be running (with 5-minute timeout)
hopsworks-cli deployments start my_deployment --wait --timeout 300
```

#### Stop Deployment

```bash
# Stop and return immediately
hopsworks-cli deployments stop my_deployment

# Stop and wait for it to be stopped
hopsworks-cli deployments stop my_deployment --wait
```

#### Check Deployment Status

```bash
hopsworks-cli deployments status my_deployment
```

## Output Formats

The CLI supports three output formats:

### Table (default)

```bash
hopsworks-cli models list --name my_model
```

Output:
```
+------------+---------+------------+---------------------+
| Name       | Version | Framework  | Created             |
+------------+---------+------------+---------------------+
| my_model   | 1       | tensorflow | 2024-01-15 10:30:00 |
+------------+---------+------------+---------------------+
```

### JSON

```bash
hopsworks-cli --output json models list --name my_model
```

Output:
```json
[
  {
    "name": "my_model",
    "version": 1,
    "framework": "tensorflow",
    "created": "2024-01-15T10:30:00"
  }
]
```

### YAML

```bash
hopsworks-cli --output yaml models list --name my_model
```

Output:
```yaml
- name: my_model
  version: 1
  framework: tensorflow
  created: '2024-01-15T10:30:00'
```

## Examples

### Complete Workflow

```bash
# Set up environment
export HOPSWORKS_PROJECT="my_project"
export HOPSWORKS_API_KEY_FILE=~/.hopsworks/api_key

# List all versions of a model
hopsworks-cli models list --name fraud_detection

# Get the best performing model
hopsworks-cli models best fraud_detection --metric f1_score --direction max

# Download the best model
hopsworks-cli models download fraud_detection --version 3 --output-dir ./models

# Check deployments
hopsworks-cli deployments list

# Start a deployment
hopsworks-cli deployments start fraud_detection_v3 --wait

# Check deployment status
hopsworks-cli deployments status fraud_detection_v3
```

### CI/CD Integration

```bash
#!/bin/bash
# deploy.sh - Deploy best model to production

set -e

# Get best model version
BEST_MODEL=$(hopsworks-cli --output json models best my_model \
  --metric accuracy --direction max | jq -r '.[0].version')

echo "Best model version: $BEST_MODEL"

# Check if deployment exists and start it
if hopsworks-cli deployments get my_model_prod &> /dev/null; then
  echo "Starting existing deployment..."
  hopsworks-cli deployments start my_model_prod --wait
else
  echo "Deployment not found, please create it first"
  exit 1
fi

echo "Deployment successful!"
```

### Monitoring Script

```bash
#!/bin/bash
# monitor_deployments.sh - Monitor all deployments

while true; do
  clear
  echo "=== Deployment Status ==="
  hopsworks-cli deployments list
  sleep 30
done
```

## Error Handling

The CLI provides clear error messages:

```bash
# Missing authentication
$ hopsworks-cli models list
✗ Error: Authentication required: provide --api-key-file or --api-key

# Model not found
$ hopsworks-cli models get nonexistent --version 1
✗ Error: Model 'nonexistent' version 1 not found

# Enable verbose mode for full stack traces
$ hopsworks-cli --verbose models get my_model --version 1
```

## Development

### Running Tests

```bash
cd python
pytest tests/cli/ -v
```

### Adding New Commands

1. Create a new command module in `hopsworks_cli/commands/`
2. Define Click command group and commands
3. Register the command group in `main.py`
4. Add tests in `tests/cli/`

Example:

```python
# hopsworks_cli/commands/mycommand.py
import click

@click.group()
def mycommand():
    """My custom command"""
    pass

@mycommand.command("list")
def list_items():
    """List items"""
    click.echo("Listing items...")
```

Register in `main.py`:

```python
from hopsworks_cli.commands.mycommand import mycommand

cli.add_command(mycommand)
```

## Troubleshooting

### SSL Certificate Errors

If you encounter SSL verification errors:

```bash
hopsworks-cli --no-verify models list
```

### Connection Timeouts

Increase timeout or check your network connection:

```bash
hopsworks-cli --verbose models list
```

### Permission Errors

Ensure your API key has the necessary permissions in the Hopsworks UI.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - See [LICENSE](../../LICENSE) for details.
