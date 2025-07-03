# to fix/improve
- [x] Make sure cratedb cluster health is checked to be GREEN before starting/deleting the first pods
- [x] The code should not work on  SUSPENDED
- [x] Log a message when a cluster is outside its maintenance window
- [x] Final health check needs to wait for GREEN, UNREACHABLE needs to wait
- [x] env values? YELLOW_MAX_ATTEMPTS
```
2025-07-01T11:49:33.998278+0200 | DEBUG | Loaded Kubernetes context: eks1-us-east-1-dev
Error checking maintenance window: can't compare offset-naive and offset-aware datetimes ({'activity_id': '1', 'activity_type': 'check_maintenance_window', 'attempt': 1, 'namespace': 'default', 'task_queue': 'cratedb-operations', 'workflow_id': 'restart-lime-wicket-systri-warrick-2025-07-01T09:49:40.451048+00:00', 'workflow_run_id': '0197c564-865b-7df6-be19-c65bf74ec396', 'workflow_type': 'ClusterRestartWorkflow'})
Final health check failed: UNREACHABLE (successfully restarted 1/1 pods) ({'attempt': 1, 'namespace': 'default', 'run_id': '0197c564-865b-7df6-be19-c65bf74ec396', 'task_queue': 'cratedb-operations', 'workflow_id': 'restart-lime-wicket-systri-warrick-2025-07-01T09:49:40.451048+00:00', 'workflow_type': 'ClusterRestartWorkflow'})
Failed to restart cluster lime-wicket-systri-warrick: Final health check failed: UNREACHABLE (successfully restarted 1/1 pods) ({'attempt': 1, 'namespace': 'default', 'run_id': '0197c564-64de-704a-ab5b-ed0fa2004196', 'task_queue': 'cratedb-operations', 'workflow_id': 'restart-clusters-81a32c5a', 'workflow_type': 'MultiClusterRestartWorkflow'})
```

```yaml
imagePullPolicy: IfNotPresent
lifecycle:
  preStop:
    exec:
      command:
      - /bin/sh
      - -c
      - curl -sLO https://github.com/crate/crate-operator/releases/download/dcutil-0.0.1/dc_util-linux-amd68
        && curl -sLO https://github.com/crate/crate-operator/releases/download/dcutil-0.0.1/dc_util-linux-amd64.sha256
        && sha256sum -c dc_util-linux-amd64.sha256 && chmod u+x dc_util-linux-amd64
        &&./dc_util-linux-amd64 -min-availability PRIMARIES -timeout 720s
name: crate



```
