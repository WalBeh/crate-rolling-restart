# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2025-01-30

### Added
- `tests/test_setup.py` - Comprehensive setup verification script
- `CHANGELOG.md` - This changelog file
- Setup verification section in README.md
- Enhanced debugging commands in documentation

### Changed
- **BREAKING**: Fixed `workflow.utc_now()` to `workflow.now()` for Temporal 1.13.0 compatibility
- Updated all workflow time handling to use correct Temporal API
- Made datetime fields optional in models to avoid initialization issues
- Enhanced README.md with better development workflow documentation
- Improved project structure documentation

### Fixed
- Fixed Temporal development server compatibility issues
- Resolved `workflow.unsafe.imports_passed_through()` usage in workflows and activities
- Fixed datetime.now() usage in activities for development server compatibility
- Corrected import handling for Temporal workflows and activities
- Made HealthCheckResult.checked_at optional to prevent datetime issues
- Made MultiClusterRestartResult datetime fields optional

### Removed
- **BREAKING**: Removed Docker-related files and configurations:
  - `docker-compose.yml`
  - `docker-compose.minimal.yml` 
  - `Dockerfile.dev`
- Removed unnecessary test files:
  - `clean_worker.py`
  - `simple_worker.py`
- Removed Docker deployment sections from README.md
- Cleaned up Docker references throughout documentation

### Security
- Removed loguru import issues that could cause problems with Temporal's deterministic execution
- Fixed module import patterns for better compatibility with temporal server start-dev

## [0.1.0] - 2025-01-29

### Added
- Initial release of CrateDB Kubernetes Cluster Manager with Temporal workflows
- Support for single and multi-cluster restart operations
- Kubernetes integration with CRD discovery
- Temporal workflow orchestration
- CLI interface with rich output formatting
- Activity-based architecture for cluster operations
- Health checking and validation
- Pod restart with graceful decommissioning support
- Configuration management for different Kubernetes contexts
- Comprehensive error handling and retry policies

### Features
- **Workflows**:
  - `ClusterRestartWorkflow` - Single cluster restart with health checks
  - `MultiClusterRestartWorkflow` - Multiple cluster coordination
  - `ClusterDiscoveryWorkflow` - Kubernetes cluster discovery

- **Activities**:
  - Cluster discovery and validation
  - Pod restart with readiness waiting
  - Health checking with CrateDB-specific logic
  - Prestop hook analysis and decommission utility detection

- **CLI**:
  - Rich terminal output with progress indicators
  - Multiple output formats (text, JSON, YAML)
  - Dry-run mode for safe testing
  - Context-aware Kubernetes configuration

- **Development**:
  - Full compatibility with `temporal server start-dev`
  - Comprehensive test suite
  - Type-safe implementation with Pydantic models
  - Async/await throughout for better performance