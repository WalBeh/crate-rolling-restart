"""
Temporal state machine workflows for CrateDB operations.

This module implements state machine patterns using Temporal's workflow capabilities
to replace custom retry logic and state management with declarative workflows.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from .activities import CrateDBActivities
    from .models import (
        CrateDBCluster,
        HealthCheckInput,
        HealthCheckResult,
        PodRestartInput,
        PodRestartResult,
        MaintenanceWindowCheckInput,
        MaintenanceWindowCheckResult,
        DecommissionInput,
        DecommissionResult,
        ClusterValidationInput,
        ClusterRoutingResetInput,
        ClusterRoutingResetResult,
    )


class HealthNotGreenException(Exception):
    """Exception raised when cluster health is not GREEN."""

    def __init__(self, health_status: str, message: str | None = None):
        self.health_status = health_status
        super().__init__(message or f"Cluster health is {health_status}, expected GREEN")


class MaintenanceWindowBlockedException(Exception):
    """Exception raised when operation is blocked by maintenance window."""
    """Exception raised when operation is blocked by maintenance window."""


@workflow.defn
class HealthCheckStateMachine:
    """
    State machine for health check operations with intelligent retry logic.

    This replaces the custom retry logic in activities with Temporal's
    built-in state management and retry capabilities.
    """

    @workflow.run
    async def run(self, input_data: HealthCheckInput) -> HealthCheckResult:
        """
        Execute health check with state-based retry logic.

        States: UNKNOWN -> CHECKING -> (YELLOW|RED|UNKNOWN) -> GREEN
        """
        # Handle case where input_data might be a dict due to serialization issues
        if isinstance(input_data, dict):
            from .models import HealthCheckInput, CrateDBCluster
            # Handle nested cluster object conversion
            if isinstance(input_data.get('cluster'), dict):
                input_data['cluster'] = CrateDBCluster(**input_data['cluster'])
            input_data = HealthCheckInput(**input_data)
        
        workflow.logger.info(f"Starting health check state machine for cluster {input_data.cluster.name}")

        # State-based retry configuration
        retry_configs = {
            "YELLOW": {"max_attempts": 30, "wait_seconds": 10},
            "RED": {"max_attempts": 30, "wait_seconds": 15},
            "UNKNOWN": {"max_attempts": 20, "wait_seconds": 5},
            "default": {"max_attempts": 5, "wait_seconds": 30}
        }

        current_state = "UNKNOWN"
        attempts = 0
        max_total_attempts = 60  # Overall safety limit

        while attempts < max_total_attempts:
            try:
                # Execute simple health check (no retries at activity level)
                health_result = await workflow.execute_activity(
                    "check_cluster_health",
                    input_data,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        maximum_attempts=1,  # No retries - let state machine handle it
                    ),
                )

                new_state = health_result['health_status']
                workflow.logger.info(f"Health check result: {current_state} -> {new_state}")

                if new_state == "GREEN":
                    workflow.logger.info(f"Cluster {input_data.cluster.name} health is GREEN after {attempts + 1} attempts")
                    return HealthCheckResult(**health_result)

                # Update state
                current_state = new_state
                attempts += 1

                # Get retry configuration for this state
                config = retry_configs.get(current_state, retry_configs["default"])

                # Check if we've exceeded attempts for this state
                if attempts >= config["max_attempts"]:
                    error_msg = f"Health check failed: cluster {input_data.cluster.name} is {current_state} after {attempts} attempts"
                    workflow.logger.error(error_msg)
                    raise HealthNotGreenException(current_state, error_msg)

                # Wait before next attempt with exponential backoff and deterministic jitter
                base_wait = config["wait_seconds"]
                exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)  # Cap at 60 seconds
                # Use deterministic jitter based on attempt number to avoid random in workflows
                jitter_factor = 0.1 + ((attempts % 10) * 0.02)  # Range from 0.1 to 0.28
                jitter = jitter_factor * exponential_wait
                total_wait = exponential_wait + jitter

                workflow.logger.info(
                    f"Cluster {input_data.cluster.name} health is {current_state} "
                    f"(attempt {attempts}/{config['max_attempts']}). "
                    f"Waiting {total_wait:.1f}s before retry..."
                )

                await workflow.sleep(timedelta(seconds=total_wait))

            except Exception as e:
                if isinstance(e, HealthNotGreenException):
                    raise  # Re-raise our controlled exceptions

                # Handle API/network errors
                attempts += 1
                if attempts >= max_total_attempts:
                    error_msg = f"Health check failed after {attempts} attempts due to errors: {e}"
                    workflow.logger.error(error_msg)
                    raise HealthNotGreenException("UNKNOWN", error_msg)

                # Wait before retrying API call
                api_wait = min(5 * (2 ** min(attempts, 6)), 30)  # Shorter waits for API errors
                workflow.logger.warning(
                    f"Health check API error (attempt {attempts}/{max_total_attempts}): {e}. "
                    f"Retrying in {api_wait}s..."
                )
                await workflow.sleep(timedelta(seconds=api_wait))

        # Should not reach here due to max_total_attempts check above
        raise HealthNotGreenException("UNKNOWN", f"Health check exceeded maximum attempts ({max_total_attempts})")


@workflow.defn
class MaintenanceWindowStateMachine:
    """
    State machine for maintenance window management.

    This replaces manual polling loops with Temporal's condition-based waiting
    and signal handling for operator overrides.
    """

    def __init__(self):
        self.force_restart_signal = False
        self.force_restart_reason = ""

    @workflow.signal
    def force_restart(self, reason: str):
        """Signal to override maintenance window restrictions."""
        self.force_restart_signal = True
        self.force_restart_reason = reason
        workflow.logger.info(f"Received force restart signal: {reason}")

    @workflow.run
    async def run(self, input_data: MaintenanceWindowCheckInput) -> MaintenanceWindowCheckResult:
        """
        Execute maintenance window check with signal-based override capability.

        States: CHECKING -> (IN_WINDOW|OUT_OF_WINDOW) -> WAITING -> (OVERRIDE|IN_WINDOW)
        """
        # Import needed for later use in the method
        from .models import MaintenanceWindowCheckInput
        
        # Handle case where input_data might be a dict due to serialization issues
        if isinstance(input_data, dict):
            input_data = MaintenanceWindowCheckInput(**input_data)
        
        workflow.logger.info(f"Starting maintenance window state machine for cluster {input_data.cluster_name}")

        # Initial maintenance window check
        maintenance_result = await workflow.execute_activity(
            "check_maintenance_window",
            input_data,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3,
            ),
        )

        workflow.logger.info(f"Initial maintenance window check for {input_data.cluster_name}: {maintenance_result['reason']}")

        if not maintenance_result['should_wait']:
            workflow.logger.info(f"Cluster {input_data.cluster_name} is in maintenance window")
            return MaintenanceWindowCheckResult(**maintenance_result)

        # We're outside the maintenance window - enter waiting state
        workflow.logger.warning(f"Cluster {input_data.cluster_name} is OUTSIDE maintenance window - entering wait state")

        while True:
            # Wait for either maintenance window or operator override
            # Use Temporal's await_condition for efficient waiting
            try:
                await workflow.wait_condition(
                    lambda: self.force_restart_signal,
                    timeout=timedelta(seconds=300)  # Check every 5 minutes
                )

                if self.force_restart_signal:
                    workflow.logger.info(f"Maintenance window override activated: {self.force_restart_reason}")
                    return MaintenanceWindowCheckResult(
                        cluster_name=input_data.cluster_name,
                        should_wait=False,
                        reason=f"Operator override: {self.force_restart_reason}",
                        current_time=workflow.now(),
                        in_maintenance_window=False
                    )

            except TimeoutError:
                # Timeout reached - check maintenance window again
                workflow.logger.info(f"Rechecking maintenance window for {input_data.cluster_name}")

                updated_input = MaintenanceWindowCheckInput(
                    cluster_name=input_data.cluster_name,
                    current_time=workflow.now(),
                    config_path=input_data.config_path
                )

                maintenance_check = await workflow.execute_activity(
                    "check_maintenance_window",
                    updated_input,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=10),
                        maximum_attempts=3,
                    ),
                )

                if not maintenance_check['should_wait']:
                    workflow.logger.info(f"Maintenance window now open for {input_data.cluster_name}")
                    return MaintenanceWindowCheckResult(**maintenance_check)

                workflow.logger.info(f"Cluster {input_data.cluster_name} still outside maintenance window, continuing to wait...")


@workflow.defn
class PodRestartStateMachine:
    """
    State machine for pod restart operations.

    This breaks down the complex pod restart process into clear states:
    HEALTH_CHECK -> DECOMMISSION -> DELETE -> WAIT_READY -> RESET_ROUTING -> COMPLETE
    """

    @workflow.run
    async def run(self, input_data: PodRestartInput) -> PodRestartResult:
        """
        Execute pod restart with clear state transitions.

        States: HEALTH_CHECK -> DECOMMISSION -> DELETE -> WAIT_READY -> RESET_ROUTING -> COMPLETE
        """
        start_time = workflow.now()
        
        # Handle case where input_data might be a dict due to serialization issues
        if isinstance(input_data, dict):
            from .models import PodRestartInput, CrateDBCluster
            # Handle nested cluster object conversion
            if isinstance(input_data.get('cluster'), dict):
                input_data['cluster'] = CrateDBCluster(**input_data['cluster'])
            input_data = PodRestartInput(**input_data)
        
        workflow.logger.info(f"Starting pod restart state machine for {input_data.pod_name}")

        try:
            # STATE 1: HEALTH_CHECK - Ensure cluster is healthy before proceeding
            workflow.logger.info(f"[STATE: HEALTH_CHECK] Validating cluster health for {input_data.pod_name}")

            health_input = HealthCheckInput(
                cluster=input_data.cluster,
                dry_run=input_data.dry_run,
                timeout=30,
            )

            # Use health check state machine for robust health validation
            await workflow.execute_child_workflow(
                HealthCheckStateMachine.run,
                args=[health_input],
                id=f"health-check-{input_data.pod_name}-{workflow.now().timestamp()}",
                task_timeout=timedelta(seconds=600),  # 10 minutes max for health check
            )

            workflow.logger.info(f"[STATE: HEALTH_CHECK] Cluster health validated for {input_data.pod_name}")

            # STATE 2: DECOMMISSION - Safely decommission the pod
            workflow.logger.info(f"[STATE: DECOMMISSION] Decommissioning pod {input_data.pod_name}")

            decommission_input = DecommissionInput(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                cluster=input_data.cluster,
                dry_run=input_data.dry_run,
            )

            # Calculate timeout based on cluster configuration
            decommission_timeout = input_data.cluster.dc_util_timeout + 120

            decommission_result = await workflow.execute_activity(
                "decommission_pod",
                decommission_input,
                start_to_close_timeout=timedelta(seconds=decommission_timeout),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(seconds=60),
                    maximum_attempts=3 if not input_data.cluster.has_dc_util else 2,
                    non_retryable_error_types=["ActivityCancellationError"]
                ),
            )

            if not decommission_result['success']:
                raise Exception(f"Decommission failed: {decommission_result['error']}")

            workflow.logger.info(f"[STATE: DECOMMISSION] Pod {input_data.pod_name} decommissioned successfully")

            # STATE 3: DELETE - Delete the pod
            workflow.logger.info(f"[STATE: DELETE] Deleting pod {input_data.pod_name}")

            await workflow.execute_activity(
                "delete_pod",
                input_data,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=3,
                ),
            )

            workflow.logger.info(f"[STATE: DELETE] Pod {input_data.pod_name} deleted successfully")

            # STATE 4: WAIT_READY - Wait for pod to be ready after recreation
            workflow.logger.info(f"[STATE: WAIT_READY] Waiting for pod {input_data.pod_name} to be ready")

            await workflow.execute_activity(
                "wait_for_pod_ready",
                input_data,
                start_to_close_timeout=timedelta(seconds=input_data.pod_ready_timeout),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                ),
            )

            workflow.logger.info(f"[STATE: WAIT_READY] Pod {input_data.pod_name} is ready")

            # STATE 5: RESET_ROUTING - Reset cluster routing allocation (for manual decommission only)
            if not input_data.cluster.has_dc_util:
                workflow.logger.info(f"[STATE: RESET_ROUTING] Resetting cluster routing allocation for {input_data.pod_name}")
                
                reset_input = ClusterRoutingResetInput(
                    pod_name=input_data.pod_name,
                    namespace=input_data.namespace,
                    cluster=input_data.cluster,
                    dry_run=input_data.dry_run,
                )
                
                try:
                    reset_result = await workflow.execute_activity(
                        "reset_cluster_routing_allocation",
                        reset_input,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=RetryPolicy(
                            initial_interval=timedelta(seconds=15),
                            maximum_interval=timedelta(seconds=60),
                            maximum_attempts=5,
                            backoff_coefficient=2.0,
                        ),
                    )
                    
                    workflow.logger.info(f"[STATE: RESET_ROUTING] Successfully reset cluster routing allocation for {input_data.pod_name}")
                    
                except Exception as e:
                    workflow.logger.error(f"[STATE: RESET_ROUTING] Failed to reset cluster routing allocation: {e}")
                    # Don't fail the entire restart, but log the issue
                    workflow.logger.error(f"[STATE: RESET_ROUTING] Manual intervention may be required for cluster {input_data.cluster.name}")
                    workflow.logger.error(f"[STATE: RESET_ROUTING] Execute manually: kubectl exec -n {input_data.namespace} {input_data.pod_name} -c crate -- curl --insecure -sS -H 'Content-Type: application/json' -X POST https://127.0.0.1:4200/_sql -d '{{\"stmt\": \"set global transient \\\"cluster.routing.allocation.enable\\\" = \\\"all\\\"\"}}'")
            else:
                workflow.logger.info(f"[STATE: RESET_ROUTING] Skipping routing reset (Kubernetes-managed decommission)")

            # STATE 6: COMPLETE
            end_time = workflow.now()
            duration = (end_time - start_time).total_seconds()

            workflow.logger.info(f"[STATE: COMPLETE] Pod restart completed for {input_data.pod_name} in {duration:.2f}s")

            return PodRestartResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                success=True,
                duration=duration,
                started_at=start_time,
                completed_at=end_time,
            )

        except Exception as e:
            end_time = workflow.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Pod restart state machine failed for {input_data.pod_name}: {e}"
            workflow.logger.error(error_msg)

            return PodRestartResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                success=False,
                duration=duration,
                error=error_msg,
                started_at=start_time,
                completed_at=end_time,
            )


@workflow.defn
class ClusterRestartStateMachine:
    """
    State machine for cluster restart operations.

    This orchestrates the complete cluster restart process with proper
    state management and failure handling.
    """

    def __init__(self):
        self.force_restart_signal = False
        self.force_restart_reason = ""

    @workflow.signal
    def force_restart(self, reason: str):
        """Signal to override maintenance window restrictions."""
        self.force_restart_signal = True
        self.force_restart_reason = reason
        workflow.logger.info(f"Received force restart signal: {reason}")

    @workflow.run
    async def run(self, cluster: CrateDBCluster, options) -> dict:
        """
        Execute cluster restart with state machine orchestration.

        States: MAINTENANCE_CHECK -> VALIDATION -> INITIAL_HEALTH -> POD_RESTARTS -> FINAL_HEALTH -> COMPLETE
        """
        start_time = workflow.now()
        restarted_pods = []
        skipped_pods = []

        # Handle case where cluster might be a dict due to serialization issues
        if isinstance(cluster, dict):
            from .models import CrateDBCluster
            cluster = CrateDBCluster(**cluster)

        # Handle case where options might be a dict due to serialization issues
        if isinstance(options, dict):
            from .models import RestartOptions
            options = RestartOptions(**options)

        workflow.logger.info(f"Starting cluster restart state machine for {cluster.name}")

        try:
            # STATE 1: MAINTENANCE_CHECK - Check maintenance window
            if not options.ignore_maintenance_windows and options.maintenance_config_path:
                workflow.logger.info(f"[STATE: MAINTENANCE_CHECK] Checking maintenance window for {cluster.name}")

                maintenance_input = MaintenanceWindowCheckInput(
                    cluster_name=cluster.name,
                    current_time=workflow.now(),
                    config_path=options.maintenance_config_path
                )

                # Use maintenance window state machine
                await workflow.execute_child_workflow(
                    MaintenanceWindowStateMachine.run,
                    args=[maintenance_input],
                    id=f"maintenance-{cluster.name}-{workflow.now().timestamp()}",
                    task_timeout=timedelta(hours=2),  # Allow for long maintenance waits
                )

                workflow.logger.info(f"[STATE: MAINTENANCE_CHECK] Maintenance window validated for {cluster.name}")

            # STATE 2: VALIDATION - Validate cluster before restart
            workflow.logger.info(f"[STATE: VALIDATION] Validating cluster {cluster.name}")

            validation_input = ClusterValidationInput(
                cluster=cluster,
                skip_hook_warning=options.skip_hook_warning
            )

            validation_result = await workflow.execute_activity(
                "validate_cluster",
                validation_input,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=3,
                ),
            )

            if not validation_result['is_valid']:
                raise Exception(f"Cluster validation failed: {', '.join(validation_result['errors'])}")

            for warning in validation_result['warnings']:
                workflow.logger.warning(f"Cluster {cluster.name}: {warning}")

            workflow.logger.info(f"[STATE: VALIDATION] Cluster {cluster.name} validated successfully")

            # STATE 3: INITIAL_HEALTH - Check initial cluster health
            workflow.logger.info(f"[STATE: INITIAL_HEALTH] Checking initial health for {cluster.name}")

            health_input = HealthCheckInput(
                cluster=cluster,
                dry_run=options.dry_run,
                timeout=options.health_check_timeout,
            )

            # Use health check state machine with relaxed requirements for initial check
            try:
                await workflow.execute_child_workflow(
                    HealthCheckStateMachine.run,
                    args=[health_input],
                    id=f"initial-health-{cluster.name}-{workflow.now().timestamp()}",
                    task_timeout=timedelta(seconds=300),  # 5 minutes max for initial check
                )
                workflow.logger.info(f"[STATE: INITIAL_HEALTH] Initial health check passed for {cluster.name}")
            except Exception as e:
                # For initial health check, we can proceed with YELLOW/UNKNOWN but not RED
                if "RED" in str(e) or "UNREACHABLE" in str(e):
                    raise Exception(f"Cannot restart cluster in unhealthy state: {e}") from e
                workflow.logger.warning(f"[STATE: INITIAL_HEALTH] Initial health not GREEN but proceeding: {e}")

            # STATE 4: POD_RESTARTS - Restart pods sequentially
            workflow.logger.info(f"[STATE: POD_RESTARTS] Restarting {len(cluster.pods)} pods for {cluster.name}")

            for i, pod_name in enumerate(cluster.pods):
                workflow.logger.info(f"[STATE: POD_RESTARTS] Checking pod {i+1}/{len(cluster.pods)}: {pod_name}")

                # Check if we should only restart pods on suspended nodes
                if options.only_on_suspended_nodes:
                    workflow.logger.info(f"[STATE: POD_RESTARTS] Checking if pod {pod_name} is on suspended node")
                    
                    try:
                        is_on_suspended_node = await workflow.execute_activity(
                            "is_pod_on_suspended_node",
                            args=[pod_name, cluster.namespace],
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=1),
                                maximum_interval=timedelta(seconds=5),
                                maximum_attempts=3,
                            ),
                        )
                        
                        if not is_on_suspended_node:
                            workflow.logger.info(f"[STATE: POD_RESTARTS] Skipping pod {pod_name} - not on suspended node")
                            skipped_pods.append(pod_name)
                            continue
                            
                        workflow.logger.info(f"[STATE: POD_RESTARTS] Pod {pod_name} is on suspended node, proceeding with restart")
                    except Exception as e:
                        workflow.logger.error(f"[STATE: POD_RESTARTS] Failed to check node status for pod {pod_name}: {e}")
                        workflow.logger.info(f"[STATE: POD_RESTARTS] Skipping pod {pod_name} due to node check failure")
                        skipped_pods.append(pod_name)
                        continue

                workflow.logger.info(f"[STATE: POD_RESTARTS] Restarting pod {i+1}/{len(cluster.pods)}: {pod_name}")

                pod_input = PodRestartInput(
                    pod_name=pod_name,
                    namespace=cluster.namespace,
                    cluster=cluster,
                    dry_run=options.dry_run,
                    pod_ready_timeout=options.pod_ready_timeout,
                )

                # Use pod restart state machine
                pod_result = await workflow.execute_child_workflow(
                    PodRestartStateMachine.run,
                    args=[pod_input],
                    id=f"restart-{pod_name}-{workflow.now().timestamp()}",
                    task_timeout=timedelta(seconds=pod_input.pod_ready_timeout + 600),
                )

                if not pod_result.success:
                    raise Exception(f"Pod restart failed: {pod_result.error}")

                restarted_pods.append(pod_name)
                workflow.logger.info(f"[STATE: POD_RESTARTS] Successfully restarted pod {pod_name}")

                # Health check after each pod restart (except the last pod in the list)
                # We do this conservatively to ensure cluster stability
                if i < len(cluster.pods) - 1:
                    workflow.logger.info(f"[STATE: POD_RESTARTS] Health check after restarting {pod_name}")

                    # Brief stabilization wait
                    await workflow.sleep(timedelta(seconds=5))

                    # Health check with state machine
                    await workflow.execute_child_workflow(
                        HealthCheckStateMachine.run,
                        args=[health_input],
                        id=f"inter-health-{pod_name}-{workflow.now().timestamp()}",
                        task_timeout=timedelta(seconds=600),  # 10 minutes max
                    )

                    workflow.logger.info(f"[STATE: POD_RESTARTS] Health check passed after restarting {pod_name}")

            # STATE 5: FINAL_HEALTH - Final health check (only if pods were restarted)
            if restarted_pods:
                workflow.logger.info(f"[STATE: FINAL_HEALTH] Performing final health check for {cluster.name}")

                await workflow.execute_child_workflow(
                    HealthCheckStateMachine.run,
                    args=[health_input],
                    id=f"final-health-{cluster.name}-{workflow.now().timestamp()}",
                    task_timeout=timedelta(seconds=600),  # 10 minutes max
                )

                workflow.logger.info(f"[STATE: FINAL_HEALTH] Final health check passed for {cluster.name}")
            else:
                workflow.logger.info(f"[STATE: FINAL_HEALTH] Skipping final health check - no pods were restarted")

            # STATE 6: COMPLETE
            end_time = workflow.now()
            duration = (end_time - start_time).total_seconds()

            # Log summary of restart operation
            if skipped_pods:
                workflow.logger.info(f"[STATE: COMPLETE] Cluster restart completed for {cluster.name} in {duration:.2f}s")
                workflow.logger.info(f"[STATE: COMPLETE] Restarted {len(restarted_pods)} pods, skipped {len(skipped_pods)} pods")
                workflow.logger.info(f"[STATE: COMPLETE] Skipped pods: {', '.join(skipped_pods)}")
            else:
                workflow.logger.info(f"[STATE: COMPLETE] Cluster restart completed for {cluster.name} in {duration:.2f}s")

            return {
                "cluster": cluster,
                "success": True,
                "duration": duration,
                "restarted_pods": restarted_pods,
                "total_pods": len(cluster.pods),
                "started_at": start_time,
                "completed_at": end_time,
            }

        except Exception as e:
            end_time = workflow.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Cluster restart state machine failed for {cluster.name}: {e}"
            workflow.logger.error(error_msg)

            return {
                "cluster": cluster,
                "success": False,
                "duration": duration,
                "restarted_pods": restarted_pods,
                "total_pods": len(cluster.pods),
                "error": error_msg,
                "started_at": start_time,
                "completed_at": end_time,
            }


# Utility functions for state machine configuration
class StateMachineConfig:
    """Configuration utilities for state machines."""

    @staticmethod
    def get_health_check_retry_policy(health_state: str) -> RetryPolicy:
        """Get retry policy based on health state."""
        configs = {
            "YELLOW": RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=30,
                backoff_coefficient=2.0,
            ),
            "RED": RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=30,
                backoff_coefficient=2.0,
            ),
            "UNKNOWN": RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=20,
                backoff_coefficient=2.0,
            ),
        }
        return configs.get(health_state, RetryPolicy(maximum_attempts=5))

    @staticmethod
    def get_decommission_timeout(cluster: CrateDBCluster) -> int:
        """Get decommission timeout based on cluster configuration."""
        base_timeout = cluster.dc_util_timeout
        if cluster.has_dc_util:
            return base_timeout + 120  # Kubernetes-managed
        else:
            return base_timeout + 180  # Manual decommission needs more buffer

    @staticmethod
    def get_pod_restart_timeout(cluster: CrateDBCluster, base_timeout: int) -> int:
        """Get pod restart timeout based on cluster configuration."""
        if cluster.has_dc_util:
            return cluster.dc_util_timeout + base_timeout + 120
        else:
            return base_timeout + 60
