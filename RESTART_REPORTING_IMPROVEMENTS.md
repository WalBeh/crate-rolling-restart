# Pod Restart Reporting Improvements

## Overview

This document describes the improvements made to the pod restart reporting system to provide more accurate and informative feedback when cluster restarts are partially completed.

## Problem Statement

Previously, the "Cluster Details" report showed inaccurate pod restart counts. For example:
- `aqua-darth-vader` cluster with 3 pods showed "Pods Restarted: 1" when a health check failed after the first pod
- Users couldn't distinguish between "1 pod successfully restarted out of 3" vs "1 pod cluster fully restarted"
- Error messages lacked context about restart progress

## Solution

### Enhanced RestartResult Model

Added `total_pods` field to the `RestartResult` model to track the total number of pods in the cluster:

```python
class RestartResult(BaseModel):
    cluster: CrateDBCluster
    success: bool
    duration: float
    restarted_pods: List[str] = Field(default_factory=list)
    total_pods: int = 0  # NEW FIELD
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
```

### Improved CLI Display

The "Pods Restarted" column now shows progress in `"X/Y"` format:
- `1/3` - 1 pod restarted out of 3 total pods (partial restart)
- `3/3` - All 3 pods successfully restarted (complete restart)
- `1/1` - Single pod cluster fully restarted

### Enhanced Error Messages

Error messages now include restart progress context:

**Before:**
```
Cluster health check failed after restarting pod pod-1: YELLOW
```

**After:**
```
Cluster health check failed after restarting pod pod-1: YELLOW (restarted 1/3 pods)
```

### Updated JSON Export

JSON output now includes the `total_pods` field for programmatic consumption:

```json
{
  "cluster": {...},
  "success": false,
  "duration": 45.5,
  "restarted_pods": ["pod-1"],
  "total_pods": 3,
  "error": "Cluster health check failed after restarting pod pod-1: YELLOW (restarted 1/3 pods)"
}
```

## Example Output

### CLI Table Format
```
Cluster Details
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Cluster              ┃ Namespace      ┃ Success ┃ Duration (s) ┃ Pods Restarted ┃ Error                           ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ aqua-darth-vader     │ default        │ ✗       │ 45.30       │ 1/3            │ Health check failed: YELLOW     │
│ single-pod-cluster   │ default        │ ✓       │ 30.20       │ 1/1            │                                 │
└──────────────────────┴────────────────┴─────────┴─────────────┴────────────────┴─────────────────────────────────┘
```

### Progress Interpretation
- `1/3`: Health check failed after 1st pod restart (33% complete)
- `2/3`: Health check failed after 2nd pod restart (67% complete)  
- `3/3`: All pods successfully restarted (100% complete)
- `1/1`: Single pod cluster completed successfully (100% complete)

## Benefits

1. **Clear Progress Visibility**: Users can immediately see how much of the restart was completed
2. **Better Troubleshooting**: Error messages provide context about when the failure occurred
3. **Accurate Reporting**: Distinguishes between partial and complete restarts
4. **Consistent API**: JSON export includes all necessary fields for automation
5. **Health Check Integrity**: Maintains strict health check standards (YELLOW ≠ GREEN)

## Workflow Behavior

The restart workflow remains unchanged in terms of health check requirements:
- **Intermediate Health Checks**: Must be GREEN to proceed to next pod
- **Final Health Check**: Must be GREEN for overall success
- **Early Termination**: Stops on first health check failure, preserving restart progress in results

This ensures cluster safety while providing accurate progress reporting when issues occur.

## Project Organization Note

The test files referenced in this document have been organized into a `tests/` directory for better project structure:
- `tests/test_restart_workflow.py` - Contains tests for the restart workflow functionality
- `tests/test_discovery.py` - Contains cluster discovery tests
- `tests/test_setup.py` - Contains setup verification tests
- `tests/test_worker.py` - Contains worker functionality tests

When running tests, use the correct paths:
```bash
# Run setup verification
uv run python tests/test_setup.py

# Run restart workflow tests
uv run python tests/test_restart_workflow.py
```