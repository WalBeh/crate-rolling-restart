# CrateDB Decommission Integration

## Overview

This document describes the complete integration of intelligent CrateDB decommission capabilities into the Temporal workflow system. The implementation automatically detects whether a cluster has Kubernetes-managed decommission (preStop hook with dc_util) or requires manual decommission, then executes the appropriate strategy.

## Key Components

### 1. Enhanced Activities (`rr/activities.py`)

#### New Activity: `decommission_pod`

```python
@activity.defn
async def decommission_pod(self, input_data: DecommissionInput) -> DecommissionResult:
```

**Purpose**: Standalone decommission activity that can be used independently or as part of pod restart workflows.

**Features**:
- Automatic strategy detection based on StatefulSet configuration
- Support for both Kubernetes-managed and manual decommission
- Comprehensive logging and error handling
- Dry-run support

#### Enhanced Activity: `restart_pod`

The existing `restart_pod` activity now includes intelligent decommission:

1. **Strategy Detection**: Analyzes cluster configuration
2. **Decommission Execution**: Uses appropriate strategy
3. **Pod Deletion**: Handles pod deletion based on strategy
4. **Pod Recreation**: Waits for new pod to be ready

### 2. Decommission Strategies

#### Strategy 1: Kubernetes-Managed Decommission

**When Used**: `cluster.has_dc_util = true`

**Process**:
1. Delete pod with extended grace period (`dc_util_timeout + 60s`)
2. Kubernetes triggers preStop hook
3. dc_util handles decommission automatically
4. Pod is deleted and recreated
5. Wait for new pod to be ready

**Benefits**:
- Uses official CrateDB tooling (dc_util)
- Consistent with Kubernetes lifecycle management
- No race conditions
- Proper timeout handling

#### Strategy 2: Manual Decommission

**When Used**: `cluster.has_dc_util = false`

**Process**:
1. Execute decommission setup commands via CrateDB API
2. Send `ALTER CLUSTER DECOMMISSION` command
3. Wait for CrateDB process to exit
4. Delete pod with short grace period (30s)
5. Wait for new pod to be ready

**Commands Executed**:
```sql
-- Setup commands
set global transient "cluster.routing.allocation.enable" = "new_primaries"
set global transient "cluster.graceful_stop.timeout" = "720s"
set global transient "cluster.graceful_stop.force" = true
set global transient "cluster.graceful_stop.min_availability" = "PRIMARIES"

-- Decommission command
alter cluster decommission $$data-hot-{pod_suffix}$$
```

### 3. Data Models

#### `DecommissionInput`
```python
class DecommissionInput(BaseModel):
    pod_name: str
    namespace: str
    cluster: CrateDBCluster
    dry_run: bool = False
```

#### `DecommissionResult`
```python
class DecommissionResult(BaseModel):
    pod_name: str
    namespace: str
    strategy_used: str  # "kubernetes_managed" or "manual"
    success: bool
    duration: float
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    process_exited: bool = False
    decommission_timeout: int = 720
```

## Usage Examples

### 1. Standalone Decommission

```python
from rr.activities import CrateDBActivities
from rr.models import DecommissionInput, CrateDBCluster

# Create cluster with Kubernetes-managed decommission
cluster = CrateDBCluster(
    name="my-cluster",
    namespace="crate-system",
    statefulset_name="crate-data-hot-my-cluster",
    health="GREEN",
    has_dc_util=True,
    dc_util_timeout=720,
    # ... other fields
)

# Execute decommission
activities = CrateDBActivities()
result = await activities.decommission_pod(DecommissionInput(
    pod_name="crate-data-hot-my-cluster-2",
    namespace="crate-system",
    cluster=cluster
))

if result.success:
    print(f"✅ Decommission completed using {result.strategy_used}")
else:
    print(f"❌ Decommission failed: {result.error}")
```

### 2. Pod Restart with Decommission

```python
from rr.models import PodRestartInput

# Pod restart automatically includes intelligent decommission
result = await activities.restart_pod(PodRestartInput(
    pod_name="crate-data-hot-my-cluster-1",
    namespace="crate-system", 
    cluster=cluster,
    pod_ready_timeout=600
))
```

### 3. Workflow Integration

```python
@workflow.defn
class ClusterMaintenanceWorkflow:
    @workflow.run
    async def run(self, cluster: CrateDBCluster):
        # Restart all pods with intelligent decommission
        for pod_name in cluster.pods:
            result = await workflow.execute_activity(
                CrateDBActivities.restart_pod,
                PodRestartInput(
                    pod_name=pod_name,
                    namespace=cluster.namespace,
                    cluster=cluster,
                    pod_ready_timeout=cluster.dc_util_timeout + 300
                ),
                start_to_close_timeout=timedelta(minutes=30)
            )
            
            if not result.success:
                raise Exception(f"Failed to restart {pod_name}")
```

## Configuration Detection

The system automatically detects decommission configuration by analyzing the StatefulSet:

### Kubernetes-Managed Detection

```yaml
# StatefulSet with dc_util in preStop hook
spec:
  template:
    spec:
      containers:
      - name: crate
        lifecycle:
          preStop:
            exec:
              command:
              - /bin/sh
              - -c  
              - ./dc_util-linux-amd64 -min-availability PRIMARIES -timeout 720s
```

**Detection Result**: `has_dc_util=true, dc_util_timeout=720`

### Manual Decommission Detection

```yaml
# StatefulSet without lifecycle hooks
spec:
  template:
    spec:
      containers:
      - name: crate
        # No lifecycle configuration
```

**Detection Result**: `has_dc_util=false, dc_util_timeout=720` (default)

## Error Handling

### Common Scenarios

#### 1. Pod Not Found
```
DecommissionResult(
    success=False,
    error="Pod not found: crate-data-hot-cluster-0",
    strategy_used="unknown"
)
```

#### 2. Timeout During Manual Decommission
```
DecommissionResult(
    success=False,
    error="Manual decommission timed out after 1200s",
    strategy_used="manual"
)
```

#### 3. Kubernetes API Error
```
DecommissionResult(
    success=False,
    error="Failed to delete pod: Forbidden",
    strategy_used="kubernetes_managed"
)
```

### Error Recovery

1. **Timeout Errors**: Increase timeout or check cluster health
2. **Permission Errors**: Verify RBAC configuration
3. **Pod Not Found**: Verify pod name and namespace
4. **CrateDB API Errors**: Check cluster connectivity and health

## Logging

### Kubernetes-Managed Decommission Logs

```
INFO: Analyzing decommission strategy for pod crate-data-hot-cluster1-0
INFO: Cluster config: has_prestop_hook=True, has_dc_util=True
INFO: Using Kubernetes-managed decommission for pod crate-data-hot-cluster1-0
INFO: PreStop hook with dc_util will handle decommission automatically
INFO: Will delete pod crate-data-hot-cluster1-0 with grace period 780s for dc_util
INFO: Pod crate-data-hot-cluster1-0 has been deleted
INFO: Kubernetes-managed decommission completed for pod crate-data-hot-cluster1-0
INFO: Decommission completed for pod crate-data-hot-cluster1-0 using kubernetes_managed strategy
```

### Manual Decommission Logs

```
INFO: Analyzing decommission strategy for pod crate-data-hot-cluster2-0
INFO: Cluster config: has_prestop_hook=False, has_dc_util=False
INFO: Using manual decommission for pod crate-data-hot-cluster2-0
INFO: No dc_util configured - executing manual decommission via CrateDB API
DEBUG: Executing manual decommission SQL 1/5: set global transient "cluster.routing.allocation.enable" = "new_primaries"
DEBUG: Executing manual decommission SQL 2/5: set global transient "cluster.graceful_stop.timeout" = "720s"
DEBUG: Executing manual decommission SQL 3/5: set global transient "cluster.graceful_stop.force" = true
DEBUG: Executing manual decommission SQL 4/5: set global transient "cluster.graceful_stop.min_availability" = "PRIMARIES"
DEBUG: Executing manual decommission SQL 5/5: alter cluster decommission $$data-hot-0$$
INFO: Manual decommission completed - CrateDB process has exited
INFO: Pod crate-data-hot-cluster2-0 is ready for deletion and restart
INFO: Manual decommission strategy completed for pod crate-data-hot-cluster2-0
```

## Best Practices

### 1. Timeout Configuration

- **Kubernetes-managed**: Use detected `dc_util_timeout + 60s` for grace period
- **Manual**: Use `1200s` for decommission command, `60s` for setup commands
- **Pod ready**: Use `cluster.dc_util_timeout + 300s` for pod restart timeout

### 2. Error Handling

```python
try:
    result = await activities.decommission_pod(input_data)
    if not result.success:
        # Log error but don't necessarily fail the workflow
        workflow.logger.error(f"Decommission failed: {result.error}")
        # Decide whether to continue or abort based on error type
except Exception as e:
    # Unexpected errors should be handled
    workflow.logger.error(f"Unexpected decommission error: {e}")
    raise
```

### 3. Cluster Health Monitoring

Always check cluster health before and after decommission operations:

```python
# Check health before decommission
health_result = await workflow.execute_activity(
    CrateDBActivities.check_cluster_health,
    HealthCheckInput(cluster=cluster)
)

if health_result.health_status != "GREEN":
    raise Exception(f"Cluster unhealthy: {health_result.health_status}")

# Proceed with decommission...
```

### 4. Batch Operations

When decommissioning multiple pods:
- Process one pod at a time
- Wait for cluster stabilization between operations
- Stop on first failure to prevent cascading issues

## Troubleshooting

### Issue: Wrong Strategy Selected

**Symptoms**: Manual decommission runs when Kubernetes-managed expected

**Diagnosis**:
```python
# Check detection results
has_prestop, has_dc_util, timeout = activities._analyze_prestop_hook(sts, cluster_name)
print(f"Detection: prestop={has_prestop}, dc_util={has_dc_util}, timeout={timeout}")
```

**Solutions**:
- Verify StatefulSet has preStop hook configured
- Check dc_util patterns in command detection
- Review command formatting in preStop hook

### Issue: Timeout During Decommission

**Symptoms**: Operation times out waiting for process exit

**Diagnosis**:
- Check cluster health and shard distribution
- Verify data replication settings
- Monitor cluster logs during decommission

**Solutions**:
- Increase timeout values
- Ensure sufficient replicas for data availability
- Check disk space on remaining nodes

### Issue: Pod Stuck in Terminating

**Symptoms**: Pod remains in Terminating state

**Diagnosis**:
- Check pod events: `kubectl describe pod <pod-name>`
- Review StatefulSet controller logs
- Check resource constraints

**Solutions**:
- Verify grace period is sufficient
- Check for resource locks or persistent volumes
- Consider force deletion as last resort

## Performance Considerations

### Resource Usage

- **Kubernetes-managed**: Lower resource usage, handled by dc_util
- **Manual**: Higher resource usage due to API calls and monitoring

### Network Traffic

- **Kubernetes-managed**: Minimal additional network traffic  
- **Manual**: Multiple HTTP requests to CrateDB API

### Timing

- **Kubernetes-managed**: Timing controlled by dc_util and grace period
- **Manual**: Timing controlled by polling and timeout configuration

## Security Considerations

### RBAC Requirements

Ensure the service account has permissions for:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: crate-decommission
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "delete"]
- apiGroups: [""]
  resources: ["pods/exec"]
  verbs: ["create"]
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "list"]
```

### Network Policies

Ensure pods can communicate with CrateDB API (port 4200) for manual decommission.

## Monitoring and Metrics

### Key Metrics to Track

1. **Decommission Success Rate**: Percentage of successful decommissions
2. **Decommission Duration**: Time taken for each strategy
3. **Strategy Distribution**: Usage of Kubernetes-managed vs manual
4. **Error Rates**: Common failure scenarios

### Alerting

Set up alerts for:
- High decommission failure rates
- Unusually long decommission times
- Repeated timeout errors
- Pod termination issues

## Future Enhancements

### Planned Improvements

1. **Health-aware decommission**: Check cluster health before proceeding
2. **Custom timeout configuration**: Override detected timeouts when needed
3. **Retry mechanisms**: Automatic retry for transient failures
4. **Metrics collection**: Export decommission performance metrics
5. **Multiple dc_util versions**: Support for different decommission utilities

### Integration Opportunities

1. **Prometheus metrics**: Export strategy usage and performance
2. **Grafana dashboards**: Visualize decommission operations
3. **Alert manager**: Notify on decommission failures
4. **Jaeger tracing**: Trace decommission operations across activities