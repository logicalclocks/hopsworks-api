# Hopsworks CLI Tool - Implementation Plan

## Executive Summary

Create a Python CLI tool (`hopsworks-cli`) that provides command-line access to Hopsworks resources including projects, feature groups, feature views, models, deployments, jobs, and secrets. The CLI will leverage the existing `hopsworks` Python SDK and follow patterns established in the existing MCP server implementation.

---

## 1. Architecture & Technology Stack

### CLI Framework: Click
**Rationale:** The codebase already uses Click for the MCP server (`hopsworks/mcp/main.py`), ensuring consistency and leveraging existing patterns.

### Project Structure
```
python/hopsworks_cli/
├── __init__.py
├── main.py              # Entry point with main command group
├── config.py            # Configuration management
├── auth.py              # Authentication helpers
├── output.py            # Output formatting (JSON, table, etc.)
├── commands/
│   ├── __init__.py
│   ├── projects.py      # Project commands
│   ├── feature_groups.py
│   ├── feature_views.py
│   ├── models.py
│   ├── deployments.py
│   ├── jobs.py
│   └── secrets.py
└── utils/
    ├── __init__.py
    ├── exceptions.py
    └── helpers.py
```

### Entry Point
```python
# In pyproject.toml
[project.scripts]
hopsworks-cli = "hopsworks_cli.main:cli"
```

---

## 2. Authentication & Configuration

### Default Connection Parameters
```python
DEFAULT_HOST = "app.hopsworks.ai"
DEFAULT_PORT = 443
DEFAULT_HOSTNAME_VERIFICATION = False
```

### Configuration File Support
Create `~/.hopsworks/config.yaml` for persistent configuration:
```yaml
default_profile: production

profiles:
  production:
    host: app.hopsworks.ai
    port: 443
    api_key_file: ~/.hopsworks/prod_api_key
    project: my_project

  development:
    host: dev.hopsworks.ai
    port: 443
    api_key_file: ~/.hopsworks/dev_api_key
    project: dev_project
```

### Environment Variable Support
Priority order (highest to lowest):
1. Command-line arguments
2. Environment variables (`HOPSWORKS_*`)
3. Profile in config file
4. Default values

### Authentication Flow
```python
hopsworks-cli --host app.hopsworks.ai --api-key-file ~/.api_key projects list
# Or using profile
hopsworks-cli --profile production projects list
# Or using environment variables
export HOPSWORKS_HOST=app.hopsworks.ai
export HOPSWORKS_API_KEY=<key>
hopsworks-cli projects list
```

---

## 3. Command Structure

### Main Command Group
```bash
hopsworks-cli [GLOBAL_OPTIONS] COMMAND [SUBCOMMAND] [OPTIONS] [ARGS]
```

### Global Options
```
--host TEXT                  Hopsworks hostname [default: app.hopsworks.ai]
--port INTEGER               Hopsworks port [default: 443]
--project TEXT               Project name
--api-key TEXT               API key value (not recommended, use file)
--api-key-file PATH          Path to API key file
--profile TEXT               Configuration profile name
--output FORMAT              Output format: json|table|yaml [default: table]
--verbose                    Enable verbose output
--no-verify                  Disable SSL verification
--help                       Show help message
```

---

## 4. Core Commands - Detailed Specification

### 4.1 Projects

#### List Projects
```bash
hopsworks-cli projects list [--output json|table]
```
**Implementation:**
- Call `hopsworks.login()` without project
- Use connection to list all accessible projects
- Display: project name, description, creation date, owner

#### Get Project Details
```bash
hopsworks-cli projects get PROJECT_NAME [--output json|table]
```
**Implementation:**
- Call `hopsworks.login(project=PROJECT_NAME)`
- Display detailed project information

#### Create Project
```bash
hopsworks-cli projects create PROJECT_NAME [--description TEXT]
```
**Implementation:**
- Call `hopsworks.create_project(name, description)`
- Display created project details

#### Switch Project (Update Config)
```bash
hopsworks-cli projects use PROJECT_NAME [--profile PROFILE]
```
**Implementation:**
- Update config file's default project for the specified profile
- Display confirmation message

---

### 4.2 Feature Groups

#### List Feature Groups
```bash
hopsworks-cli feature-groups list [--feature-store NAME] [--output json|table]
```
**Implementation:**
- Call `project.get_feature_store()`
- Call `fs.get_feature_groups()`
- Display: name, version, online_enabled, created, features count

#### Get Feature Group
```bash
hopsworks-cli feature-groups get NAME --version INTEGER [--output json|table]
```
**Implementation:**
- Call `fs.get_feature_group(name, version)`
- Display: full feature group details including schema

#### Create Feature Group
```bash
hopsworks-cli feature-groups create NAME \
  --version INTEGER \
  --primary-key KEY1,KEY2 \
  [--description TEXT] \
  [--online] \
  [--event-time COLUMN]
```
**Implementation:**
- Call `fs.create_feature_group()`
- Display created feature group summary

#### Describe Feature Group Schema
```bash
hopsworks-cli feature-groups schema NAME --version INTEGER [--output json|table]
```
**Implementation:**
- Get feature group and display feature schema
- Show: feature name, type, description

---

### 4.3 Feature Views

#### List Feature Views
```bash
hopsworks-cli feature-views list [--output json|table]
```
**Implementation:**
- Call `fs.get_feature_views()`
- Display: name, version, features count, created

#### Get Feature View
```bash
hopsworks-cli feature-views get NAME --version INTEGER [--output json|table]
```
**Implementation:**
- Call `fs.get_feature_view(name, version)`
- Display full feature view details including query

#### Get Feature View Query
```bash
hopsworks-cli feature-views query NAME --version INTEGER
```
**Implementation:**
- Get feature view and display the underlying query as SQL

---

### 4.4 Models

#### List Models
```bash
hopsworks-cli models list [--name NAME] [--output json|table]
```
**Implementation:**
- Call `project.get_model_registry()`
- If name provided: `mr.get_models(name)` (all versions)
- If not: iterate and collect all models
- Display: name, version, framework, metrics, created

#### Get Model
```bash
hopsworks-cli models get NAME --version INTEGER [--output json|table]
```
**Implementation:**
- Call `mr.get_model(name, version)`
- Display full model details

#### Get Best Model
```bash
hopsworks-cli models best NAME --metric METRIC --direction max|min [--output json|table]
```
**Implementation:**
- Call `mr.get_best_model(name, metric, direction)`
- Display best model details

#### Download Model
```bash
hopsworks-cli models download NAME --version INTEGER [--output-dir PATH]
```
**Implementation:**
- Call `model.download()`
- Save to specified directory or current directory
- Display download path

---

### 4.5 Deployments

#### List Deployments
```bash
hopsworks-cli deployments list [--output json|table]
```
**Implementation:**
- Call `project.get_model_serving()`
- Call `ms.get_deployments()`
- Display: name, model, status, created

#### Get Deployment
```bash
hopsworks-cli deployments get NAME [--output json|table]
```
**Implementation:**
- Call `ms.get_deployment(name)`
- Display deployment details including predictor, status, resources

#### Start Deployment
```bash
hopsworks-cli deployments start NAME
```
**Implementation:**
- Get deployment and call `deployment.start()`
- Display status update

#### Stop Deployment
```bash
hopsworks-cli deployments stop NAME
```
**Implementation:**
- Get deployment and call `deployment.stop()`
- Display status update

---

### 4.6 Jobs

#### List Jobs
```bash
hopsworks-cli jobs list [--output json|table]
```
**Implementation:**
- Call `project.get_job_api()`
- Call `job_api.get_jobs()`
- Display: name, type, status, last execution

#### Get Job
```bash
hopsworks-cli jobs get NAME [--output json|table]
```
**Implementation:**
- Call `job_api.get_job(name)`
- Display job configuration and details

#### Create/Update Job
```bash
hopsworks-cli jobs create NAME --config CONFIG_FILE
```
**Implementation:**
- Load config from JSON/YAML file
- Call `job_api.create_job(name, config)`
- Display created/updated job details

#### Run Job
```bash
hopsworks-cli jobs run NAME [--wait]
```
**Implementation:**
- Get job and call `job.run()`
- If `--wait`, poll until completion and display result
- Display execution ID and status

---

### 4.7 Secrets

#### List Secrets
```bash
hopsworks-cli secrets list [--output json|table]
```
**Implementation:**
- Call `project.get_secrets_api()`
- Call `secrets_api.get_secrets()`
- Display: name, owner, scope (don't show values)

#### Get Secret
```bash
hopsworks-cli secrets get NAME [--owner OWNER]
```
**Implementation:**
- Call `secrets_api.get_secret(name, owner)`
- Display secret metadata (not value for security)

#### Create Secret
```bash
hopsworks-cli secrets create NAME --value VALUE [--project PROJECT]
# Or from file
hopsworks-cli secrets create NAME --value-file PATH [--project PROJECT]
```
**Implementation:**
- Read value from argument or file
- Call `secrets_api.create_secret(name, value, project)`
- Display confirmation (don't echo value)

#### Delete Secret
```bash
hopsworks-cli secrets delete NAME [--owner OWNER]
```
**Implementation:**
- Call `secrets_api.delete(name, owner)`
- Display confirmation

---

## 5. Output Formatting

### Format Options

#### Table Format (Default)
```
┌─────────────┬─────────┬──────────────┬────────────────────┐
│ Name        │ Version │ Online       │ Created            │
├─────────────┼─────────┼──────────────┼────────────────────┤
│ transactions│ 1       │ True         │ 2024-01-15 10:30   │
│ users       │ 2       │ False        │ 2024-01-14 09:20   │
└─────────────┴─────────┴──────────────┴────────────────────┘
```
**Library:** `tabulate` or `rich` for better formatting

#### JSON Format
```json
[
  {
    "name": "transactions",
    "version": 1,
    "online_enabled": true,
    "created": "2024-01-15T10:30:00"
  }
]
```

#### YAML Format
```yaml
- name: transactions
  version: 1
  online_enabled: true
  created: 2024-01-15T10:30:00
```

### Implementation
```python
# output.py
class OutputFormatter:
    @staticmethod
    def format(data, format_type='table'):
        if format_type == 'json':
            return json.dumps(data, indent=2, cls=util.Encoder)
        elif format_type == 'yaml':
            return yaml.dump(data)
        elif format_type == 'table':
            return tabulate(data, headers='keys', tablefmt='grid')
```

---

## 6. Error Handling

### Exception Mapping
```python
# utils/exceptions.py
class CLIException(Exception):
    """Base exception for CLI errors"""
    pass

class AuthenticationError(CLIException):
    """Authentication failed"""
    pass

class ResourceNotFoundError(CLIException):
    """Resource not found"""
    pass

class ConfigurationError(CLIException):
    """Invalid configuration"""
    pass
```

### Error Display
```python
def handle_exception(ctx, exception):
    """Global exception handler"""
    if isinstance(exception, RestAPIError):
        click.secho(f"API Error: {exception}", fg='red', err=True)
        ctx.exit(1)
    elif isinstance(exception, ProjectException):
        click.secho(f"Project Error: {exception}", fg='red', err=True)
        ctx.exit(1)
    else:
        if ctx.obj.get('verbose'):
            raise  # Show full traceback in verbose mode
        click.secho(f"Error: {exception}", fg='red', err=True)
        ctx.exit(1)
```

---

## 7. Configuration Management

### Config File Location
```python
import os
from pathlib import Path

CONFIG_DIR = Path.home() / ".hopsworks"
CONFIG_FILE = CONFIG_DIR / "config.yaml"
```

### Config Structure
```python
# config.py
import yaml
from dataclasses import dataclass
from typing import Optional

@dataclass
class Profile:
    host: str
    port: int = 443
    project: Optional[str] = None
    api_key_file: Optional[str] = None
    hostname_verification: bool = False
    trust_store_path: Optional[str] = None

class Config:
    def __init__(self):
        self.config_file = CONFIG_FILE
        self.profiles = {}
        self.default_profile = "default"
        self.load()

    def load(self):
        """Load configuration from file"""
        if self.config_file.exists():
            with open(self.config_file) as f:
                data = yaml.safe_load(f)
                self.default_profile = data.get('default_profile', 'default')
                for name, profile_data in data.get('profiles', {}).items():
                    self.profiles[name] = Profile(**profile_data)

    def save(self):
        """Save configuration to file"""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            'default_profile': self.default_profile,
            'profiles': {
                name: vars(profile)
                for name, profile in self.profiles.items()
            }
        }
        with open(self.config_file, 'w') as f:
            yaml.dump(data, f)

    def get_profile(self, name: Optional[str] = None) -> Profile:
        """Get profile by name or default"""
        profile_name = name or self.default_profile
        return self.profiles.get(profile_name, Profile(host=DEFAULT_HOST))
```

---

## 8. Testing Strategy

### Unit Tests
```python
# tests/test_cli_projects.py
from click.testing import CliRunner
from hopsworks_cli.main import cli

def test_projects_list():
    runner = CliRunner()
    result = runner.invoke(cli, ['projects', 'list', '--output', 'json'])
    assert result.exit_code == 0
    # Mock API calls with pytest-mock or responses
```

### Integration Tests
- Test against actual Hopsworks instance (optional, CI only)
- Use test project with known data
- Verify CRUD operations complete successfully

### Test Structure
```
python/tests/cli/
├── __init__.py
├── conftest.py              # Fixtures and mocks
├── test_auth.py
├── test_projects.py
├── test_feature_groups.py
├── test_feature_views.py
├── test_models.py
├── test_deployments.py
├── test_jobs.py
└── test_secrets.py
```

---

## 9. Implementation Phases

### Phase 1: Foundation (Week 1)
- [ ] Set up CLI project structure
- [ ] Implement main entry point with Click
- [ ] Implement configuration management (config.py)
- [ ] Implement authentication helper (auth.py)
- [ ] Implement output formatting (output.py)
- [ ] Add global options handling
- [ ] Basic error handling

### Phase 2: Core Commands (Week 2)
- [ ] Implement `projects` commands
  - [ ] list
  - [ ] get
  - [ ] create
  - [ ] use
- [ ] Implement `feature-groups` commands
  - [ ] list
  - [ ] get
  - [ ] schema
- [ ] Write unit tests for Phase 2

### Phase 3: Feature Store (Week 3)
- [ ] Complete `feature-groups` commands
  - [ ] create
- [ ] Implement `feature-views` commands
  - [ ] list
  - [ ] get
  - [ ] query
- [ ] Write unit tests for Phase 3

### Phase 4: Model Management (Week 4)
- [ ] Implement `models` commands
  - [ ] list
  - [ ] get
  - [ ] best
  - [ ] download
- [ ] Implement `deployments` commands
  - [ ] list
  - [ ] get
  - [ ] start
  - [ ] stop
- [ ] Write unit tests for Phase 4

### Phase 5: Jobs & Secrets (Week 5)
- [ ] Implement `jobs` commands
  - [ ] list
  - [ ] get
  - [ ] create
  - [ ] run
- [ ] Implement `secrets` commands
  - [ ] list
  - [ ] get
  - [ ] create
  - [ ] delete
- [ ] Write unit tests for Phase 5

### Phase 6: Polish & Documentation (Week 6)
- [ ] Add bash/zsh completion
- [ ] Improve error messages
- [ ] Add progress indicators for long operations
- [ ] Write comprehensive documentation
- [ ] Add example workflows
- [ ] Integration testing
- [ ] Performance optimization

---

## 10. Example Implementation - Projects Command

### File: `python/hopsworks_cli/commands/projects.py`

```python
import click
from hopsworks_cli.auth import get_connection
from hopsworks_cli.output import OutputFormatter
import hopsworks

@click.group()
def projects():
    """Manage Hopsworks projects"""
    pass

@projects.command('list')
@click.pass_context
def list_projects(ctx):
    """List all accessible projects"""
    try:
        # Login without specifying project to see all projects
        connection = get_connection(ctx, project=None)

        # Get project list (implementation depends on API availability)
        # This may require enhancement to the SDK

        projects_data = [
            {
                'name': project.name,
                'description': project.description,
                'created': project.created
            }
            for project in projects_list
        ]

        formatter = OutputFormatter()
        output = formatter.format(projects_data, ctx.obj['output_format'])
        click.echo(output)

    except Exception as e:
        ctx.obj['error_handler'](ctx, e)

@projects.command('get')
@click.argument('name')
@click.pass_context
def get_project(ctx, name):
    """Get details of a specific project"""
    try:
        project = hopsworks.login(
            host=ctx.obj['host'],
            port=ctx.obj['port'],
            project=name,
            api_key_file=ctx.obj['api_key_file']
        )

        project_data = {
            'name': project.name,
            'description': project.description,
            'created': project.created,
            # Add more fields as needed
        }

        formatter = OutputFormatter()
        output = formatter.format(project_data, ctx.obj['output_format'])
        click.echo(output)

    except Exception as e:
        ctx.obj['error_handler'](ctx, e)

@projects.command('create')
@click.argument('name')
@click.option('--description', help='Project description')
@click.pass_context
def create_project(ctx, name, description):
    """Create a new project"""
    try:
        project = hopsworks.create_project(name, description=description)

        click.secho(f"✓ Project '{name}' created successfully", fg='green')

        project_data = {
            'name': project.name,
            'description': project.description,
        }

        formatter = OutputFormatter()
        output = formatter.format(project_data, ctx.obj['output_format'])
        click.echo(output)

    except Exception as e:
        ctx.obj['error_handler'](ctx, e)

@projects.command('use')
@click.argument('name')
@click.option('--profile', default='default', help='Profile to update')
@click.pass_context
def use_project(ctx, name, profile):
    """Set default project for a profile"""
    try:
        config = ctx.obj['config']
        profile_obj = config.get_profile(profile)
        profile_obj.project = name
        config.save()

        click.secho(f"✓ Default project set to '{name}' for profile '{profile}'", fg='green')

    except Exception as e:
        ctx.obj['error_handler'](ctx, e)
```

### File: `python/hopsworks_cli/main.py`

```python
import click
from hopsworks_cli.config import Config, DEFAULT_HOST
from hopsworks_cli.commands import projects, feature_groups, models, deployments, jobs, secrets
from hopsworks_cli.utils.exceptions import handle_exception

@click.group()
@click.option('--host', envvar='HOPSWORKS_HOST', default=DEFAULT_HOST,
              help='Hopsworks hostname')
@click.option('--port', envvar='HOPSWORKS_PORT', default=443, type=int,
              help='Hopsworks port')
@click.option('--project', envvar='HOPSWORKS_PROJECT',
              help='Project name')
@click.option('--api-key', envvar='HOPSWORKS_API_KEY',
              help='API key value')
@click.option('--api-key-file', envvar='HOPSWORKS_API_KEY_FILE', type=click.Path(),
              help='Path to API key file')
@click.option('--profile', help='Configuration profile name')
@click.option('--output', 'output_format',
              type=click.Choice(['json', 'table', 'yaml']),
              default='table',
              help='Output format')
@click.option('--verbose', is_flag=True,
              help='Enable verbose output')
@click.option('--no-verify', is_flag=True,
              help='Disable SSL verification')
@click.pass_context
def cli(ctx, host, port, project, api_key, api_key_file, profile,
        output_format, verbose, no_verify):
    """Hopsworks CLI - Manage Hopsworks resources from command line"""

    # Initialize context object
    ctx.ensure_object(dict)

    # Load configuration
    config = Config()

    # Use profile if specified, otherwise use CLI args
    if profile:
        profile_obj = config.get_profile(profile)
        host = host or profile_obj.host
        port = port or profile_obj.port
        project = project or profile_obj.project
        api_key_file = api_key_file or profile_obj.api_key_file

    # Store in context
    ctx.obj['config'] = config
    ctx.obj['host'] = host
    ctx.obj['port'] = port
    ctx.obj['project'] = project
    ctx.obj['api_key'] = api_key
    ctx.obj['api_key_file'] = api_key_file
    ctx.obj['output_format'] = output_format
    ctx.obj['verbose'] = verbose
    ctx.obj['hostname_verification'] = not no_verify
    ctx.obj['error_handler'] = handle_exception

# Register command groups
cli.add_command(projects.projects)
cli.add_command(feature_groups.feature_groups, name='feature-groups')
cli.add_command(models.models)
cli.add_command(deployments.deployments)
cli.add_command(jobs.jobs)
cli.add_command(secrets.secrets)

if __name__ == '__main__':
    cli()
```

---

## 11. Dependencies

Add to `pyproject.toml`:
```toml
[project.optional-dependencies]
cli = [
    "click>=8.0",
    "rich>=13.0",      # For beautiful terminal formatting
    "pyyaml>=6.0",     # For config file parsing
    "tabulate>=0.9",   # For table formatting
]
```

Install with:
```bash
pip install "hopsworks[cli]"
```

---

## 12. Documentation

### User Guide Structure

```markdown
# Hopsworks CLI User Guide

## Installation
pip install "hopsworks[cli]"

## Quick Start
1. Get your API key from Hopsworks UI
2. Save it to a file: ~/.hopsworks/api_key
3. Run your first command:
   hopsworks-cli --api-key-file ~/.hopsworks/api_key projects list

## Configuration
Create ~/.hopsworks/config.yaml with your connection details...

## Commands Reference
- Projects: Manage projects
- Feature Groups: Work with feature groups
- Feature Views: Manage feature views
- Models: Interact with model registry
- Deployments: Manage model deployments
- Jobs: Execute and monitor jobs
- Secrets: Manage secrets

## Examples
### List all feature groups
hopsworks-cli feature-groups list --output json

### Create a new job
hopsworks-cli jobs create my_job --config job_config.json

### Deploy a model
hopsworks-cli deployments start my_deployment
```

---

## 13. Future Enhancements

### Phase 7+ (Optional)
- [ ] Interactive mode with prompts
- [ ] Batch operations support
- [ ] Export/import configurations
- [ ] Shell completion (bash, zsh, fish)
- [ ] Colored output with syntax highlighting
- [ ] Watch mode for monitoring
- [ ] Diff commands to compare resources
- [ ] Template system for common operations
- [ ] Plugin system for custom commands

---

## Success Criteria

1. ✅ Users can authenticate to Hopsworks via CLI
2. ✅ All core CRUD operations are supported
3. ✅ Multiple output formats (JSON, table, YAML)
4. ✅ Configuration profiles work correctly
5. ✅ Error messages are clear and actionable
6. ✅ Unit test coverage >80%
7. ✅ Documentation is complete and clear
8. ✅ Performance is acceptable (<2s for most operations)

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| API limitations for listing all resources | Medium | Document limitations, provide workarounds |
| Authentication complexity | High | Provide clear examples, good error messages |
| Breaking changes in SDK | High | Pin SDK version, test with each release |
| Performance issues with large datasets | Medium | Implement pagination, add filters |
| Cross-platform compatibility | Low | Test on Linux, macOS, Windows |

---

## Conclusion

This plan provides a comprehensive roadmap for implementing a robust, user-friendly CLI tool for Hopsworks. The phased approach allows for iterative development and testing, while the clear structure ensures maintainability and extensibility.

The CLI will significantly improve the developer experience by enabling:
- Scriptable operations for CI/CD pipelines
- Quick inspection of resources without opening the UI
- Automation of common workflows
- Integration with other command-line tools
