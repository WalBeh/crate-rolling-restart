# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2025-01-03

### Added
- State machine workflows for cluster restart operations
- Maintenance window management with signal-based overrides
- Dictionary access fixes for Temporal activity results
- Comprehensive legacy code cleanup
- **Enhanced Cluster Routing Allocation Reset**: Automatic reset of `cluster.routing.allocation.enable` setting to "all" after pod restarts when using manual decommission
  - Reset executes independently of cluster health state (GREEN/YELLOW/RED) 
  - Reset occurs even if pod restart operations fail
  - Improved timing: waits for CrateDB startup and pod readiness before attempting reset
  - Retry mechanism: up to 5 attempts with exponential backoff (15s, 30s, 45s, 60s intervals)
  - Fallback mechanism tries alternative pods if target pod is unavailable
  - Critical error logging with manual intervention instructions when all attempts fail

### Changed
- Fixed AttributeError issues with activity results being accessed as objects instead of dictionaries
- Updated state machine workflows to properly handle Temporal serialization
- Improved error handling in MaintenanceWindowStateMachine, HealthCheckStateMachine, and ClusterRestartStateMachine

### Removed
- **BREAKING**: Removed legacy debug and test scripts:
  - `debug_cluster.py` - Cluster inspection debug script
  - `decommission_example.py` - Example decommission script
  - `snippet-decommssion.py` - Code snippet file
  - `test_health_retry_logic.py` - Legacy health check retry test
  - `test_health_validation_fix.py` - Legacy health validation test
  - `test_maintenance_config_integration.py` - Legacy maintenance config test
  - `test_state_machine_implementation.py` - Legacy state machine test
  - `validate_worker.py` - Worker validation script
  - `verify_health_validation_fix.py` - Health validation verification script
- Removed excessive documentation files (8 files consolidated):
  - `HEALTH_CHECK_REGRESSION_FIX.md`
  - `HEALTH_CHECK_RETRY_ENHANCEMENT.md`
  - `HEALTH_VALIDATION_REGRESSION_FIX.md`
  - `REGRESSION_FIX_SUMMARY.md`
  - `FIXES_APPLIED_SUMMARY.md`
  - `CLEANUP_SUMMARY.md`
  - `COMPLETE_SOLUTION_SUMMARY.md`
  - `UPDATED_STATE_BASED_RETRIES.md`

### Fixed
- Fixed 'dict' object has no attribute 'reason' error in maintenance window checks
- Fixed 'dict' object has no attribute 'is_valid' error in cluster validation
- Fixed 'dict' object has no attribute 'health_status' error in health checks
- Fixed 'PodRestartResult' object is not subscriptable error in pod restart workflows
- Fixed cluster routing allocation setting not being reset after manual decommission and pod restart
- Enhanced cluster routing reset to work independently of cluster health and operation success/failure
- Improved cluster routing reset timing to wait for CrateDB readiness before attempting reset
- Added retry mechanism with exponential backoff for cluster routing reset operations
- Added fallback mechanism for routing reset when target pod is unavailable
- Fixed random.uniform usage in HealthCheckStateMachine workflow (replaced with deterministic jitter)

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