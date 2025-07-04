# Suspended Nodes Feature

This document describes the `--only-on-suspended-nodes` feature for the CrateDB cluster restart tool.

## Overview

The `--only-on-suspended-nodes` flag allows you to restart only the CrateDB pods that are running on suspended Kubernetes nodes. This is useful for scenarios where you want to gracefully handle node maintenance, spot instance terminations, or other situations where specific nodes are marked for removal.

## Usage

```bash
rr restart --context prod --only-on-suspended-nodes cluster1
```

## How It Works

When the `--only-on-suspended-nodes` flag is enabled, the restart workflow performs the following steps:

1. **Pod Discovery**: Discovers all pods in the specified CrateDB cluster(s)
2. **Node Status Check**: For each pod, checks if it's running on a suspended node
3. **Selective Restart**: Only restarts pods that are running on suspended nodes
4. **Logging**: Logs detailed information about skipped and restarted pods

## Node Suspension Detection

A node is considered "suspended" if it has any of the following characteristics:

### Unschedulable Flag
- Node has `spec.unschedulable: true`

### Suspension Taints
- `node.kubernetes.io/unschedulable`
- `node.kubernetes.io/not-ready`
- `node.kubernetes.io/unreachable`
- `aws.amazon.com/spot-instance-terminating`
- `cluster-autoscaler.kubernetes.io/scale-down-disabled`
- `node.kubernetes.io/suspend`

### Suspension Annotations
- `cluster-autoscaler.kubernetes.io/scale-down-disabled`
- `node.kubernetes.io/suspend`
- `node.kubernetes.io/suspended`

## Behavior

### When Pods Are on Suspended Nodes
- Pod is restarted following the normal restart workflow
- Decommissioning, deletion, and health checks are performed
- Logging indicates the pod was restarted

### When Pods Are on Active Nodes
- Pod is skipped entirely
- No decommissioning or restart operations are performed
- Logging indicates the pod was skipped with the reason

### Error Handling
- If node status cannot be determined, the pod is skipped for safety
- Errors are logged but don't fail the entire operation
- Conservative approach prioritizes cluster stability

## Example Output

```
[STATE: POD_RESTARTS] Checking pod 1/3: cratedb-cluster-0
[STATE: POD_RESTARTS] Pod cratedb-cluster-0 is running on active node worker-1
[STATE: POD_RESTARTS] Skipping pod cratedb-cluster-0 - not on suspended node

[STATE: POD_RESTARTS] Checking pod 2/3: cratedb-cluster-1
[STATE: POD_RESTARTS] Pod cratedb-cluster-1 is running on suspended node worker-2
[STATE: POD_RESTARTS] Pod cratedb-cluster-1 is on suspended node, proceeding with restart
[STATE: POD_RESTARTS] Restarting pod 2/3: cratedb-cluster-1
[STATE: DECOMMISSION] Decommissioning pod cratedb-cluster-1
...

[STATE: COMPLETE] Cluster restart completed for cratedb-cluster in 45.2s
[STATE: COMPLETE] Restarted 1 pods, skipped 2 pods
[STATE: COMPLETE] Skipped pods: cratedb-cluster-0, cratedb-cluster-2
```

## Use Cases

### Spot Instance Termination
Handle AWS spot instance termination gracefully:
```bash
rr restart --context prod --only-on-suspended-nodes --async all
```

### Planned Node Maintenance
Restart pods before planned node maintenance:
```bash
rr restart --context prod --only-on-suspended-nodes cluster1 cluster2
```

### Node Draining
Coordinate with cluster autoscaler node draining:
```bash
rr restart --context prod --only-on-suspended-nodes --dry-run all
```

## Integration with Temporal

The suspended nodes feature integrates seamlessly with Temporal workflows:

- **Reliability**: Uses Temporal's retry policies for node status checks
- **Observability**: All operations are logged and tracked through Temporal
- **State Management**: Maintains proper state transitions through the restart workflow
- **Error Recovery**: Temporal handles transient failures in node status detection

## Limitations

1. **Node Status Accuracy**: Depends on accurate node status in Kubernetes API
2. **Timing**: Node status is checked at the beginning of each pod restart
3. **Permissions**: Requires cluster-level permissions to read node status
4. **Static Check**: Node status is not re-checked during the restart process

## Best Practices

1. **Combine with Dry Run**: Use `--dry-run` first to see which pods would be affected
2. **Monitor Logs**: Watch the logs to ensure expected pods are being processed
3. **Use with Async**: For large clusters, consider using `--async` to avoid blocking
4. **Test Permissions**: Ensure the service account has node read permissions

## Security Considerations

The feature requires additional Kubernetes permissions:
- `nodes/get` - Read individual node status
- `nodes/list` - List nodes (for validation)

These permissions are cluster-scoped and should be granted carefully.

## Troubleshooting

### Common Issues

**Pod always skipped despite node being suspended:**
- Check if the node has the expected taints/annotations
- Verify the pod is actually scheduled on the suspected node
- Enable debug logging with `--log-level DEBUG`

**Permission errors:**
- Ensure the service account has `nodes/get` permissions
- Check if RBAC policies allow node access

**False positives:**
- Review the node suspension detection criteria
- Some cloud providers may use different taint keys

### Debug Commands

Check pod node assignment:
```bash
kubectl get pod -n <namespace> <pod-name> -o jsonpath='{.spec.nodeName}'
```

Check node status:
```bash
kubectl get node <node-name> -o yaml
```

Check node taints:
```bash
kubectl describe node <node-name> | grep -A 10 Taints
```

## Worker Restart Required

⚠️ **Important**: After upgrading to use this feature, you must restart the Temporal worker to register the new `is_pod_on_suspended_node` activity. 

The worker will fail with `Activity function is_pod_on_suspended_node is not registered` if you try to use the feature without restarting the worker first.

To restart the worker:
1. Stop the current worker process
2. Start the worker again with the updated code
3. Verify the new activity is registered in the worker logs
