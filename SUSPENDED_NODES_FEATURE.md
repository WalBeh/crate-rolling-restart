# Suspended Nodes Feature Summary

## Overview

The `--only-on-suspended-nodes` feature allows selective restart of CrateDB pods that are running on suspended Kubernetes nodes. This feature provides a robust, lightweight solution for handling node maintenance, spot instance terminations, and other scenarios where specific nodes need to be drained gracefully.

## Key Features

- **Selective Pod Restart**: Only restarts pods on suspended nodes, leaving others untouched
- **Comprehensive Detection**: Detects suspended nodes via multiple criteria (taints, annotations, unschedulable flag)
- **Temporal Integration**: Leverages Temporal's retry policies and state management
- **Robust Error Handling**: Gracefully handles failures with conservative defaults
- **Detailed Logging**: Provides clear visibility into which pods are processed vs skipped

## Usage

```bash
# Basic usage - restart pods on suspended nodes only
rr restart --context prod --only-on-suspended-nodes cluster1

# With dry run to preview actions
rr restart --context prod --only-on-suspended-nodes --dry-run cluster1

# For all clusters (with confirmation)
rr restart --context prod --only-on-suspended-nodes all

# Asynchronous execution for large clusters
rr restart --context prod --only-on-suspended-nodes --async cluster1
```

## Implementation Details

### Architecture
- **CLI Layer**: New `--only-on-suspended-nodes` flag in `cli.py`
- **Model Layer**: `RestartOptions.only_on_suspended_nodes` field
- **Activity Layer**: `is_pod_on_suspended_node()` activity for node status detection
- **State Machine**: Modified `ClusterRestartStateMachine` to skip pods on active nodes

### Node Suspension Detection
A node is considered suspended if it has any of:
- `spec.unschedulable: true`
- Suspension taints: `node.kubernetes.io/unschedulable`, `aws.amazon.com/spot-instance-terminating`, etc.
- Suspension annotations: `node.kubernetes.io/suspend`, `cluster-autoscaler.kubernetes.io/scale-down-disabled`, etc.

### Workflow Integration
1. **Pod Discovery**: Normal cluster discovery process
2. **Node Check**: For each pod, check if its node is suspended
3. **Selective Processing**: Skip pods on active nodes, restart pods on suspended nodes
4. **Health Checks**: Only perform final health check if pods were actually restarted

## Files Modified

| File | Changes |
|------|---------|
| `rr/cli.py` | Added CLI argument and updated function signatures |
| `rr/models.py` | Added `only_on_suspended_nodes` field to `RestartOptions` |
| `rr/activities.py` | Added `is_pod_on_suspended_node()` activity |
| `rr/state_machines.py` | Modified pod restart logic to check node status |

## Testing

Comprehensive test suite includes:
- **Unit Tests**: Activity functionality and model validation
- **Integration Tests**: Real-world scenarios with mixed node states
- **Error Handling**: Graceful degradation on API failures
- **Edge Cases**: Pods without node assignment, permission errors

Run tests with:
```bash
python -m pytest tests/test_suspended_nodes*.py -v
```

## Security Considerations

The feature requires additional Kubernetes permissions:
- `nodes/get` - Read individual node status
- `nodes/list` - List nodes for validation

These are cluster-scoped permissions and should be granted appropriately.

## Best Practices

1. **Test First**: Always use `--dry-run` to preview actions
2. **Monitor Logs**: Watch for skipped vs restarted pods
3. **Permission Setup**: Ensure proper RBAC for node access
4. **Combine Features**: Use with maintenance windows and async execution as needed

## Example Output

```
[STATE: POD_RESTARTS] Checking pod 1/3: cratedb-0
[STATE: POD_RESTARTS] Pod cratedb-0 is running on active node worker-1
[STATE: POD_RESTARTS] Skipping pod cratedb-0 - not on suspended node

[STATE: POD_RESTARTS] Checking pod 2/3: cratedb-1  
[STATE: POD_RESTARTS] Pod cratedb-1 is running on suspended node worker-2
[STATE: POD_RESTARTS] Restarting pod 2/3: cratedb-1
...

[STATE: COMPLETE] Restarted 1 pods, skipped 2 pods
[STATE: COMPLETE] Skipped pods: cratedb-0, cratedb-2
```

## Performance Impact

- **Minimal Overhead**: Only adds node status checks when flag is enabled
- **Efficient API Usage**: Uses direct node/pod API calls rather than broad queries
- **Temporal Optimization**: Leverages existing retry policies and state management
- **Conservative Defaults**: Skips pods on API errors to avoid unnecessary operations

## Future Enhancements

Potential improvements could include:
- Custom node selector criteria
- Batch node status checks for better performance
- Integration with cluster autoscaler events
- Support for custom suspension indicators

## Worker Restart Required

⚠️ **Important**: After upgrading to use this feature, you must restart the Temporal worker to register the new `is_pod_on_suspended_node` activity. 

The worker will fail with `Activity function is_pod_on_suspended_node is not registered` if you try to use the feature without restarting the worker first.

---

This feature provides a production-ready solution for handling node maintenance scenarios while maintaining the existing robustness and reliability of the CrateDB restart workflow.