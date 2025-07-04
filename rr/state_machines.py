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


# Enhanced exception classes for better error handling
class NonRetryableError(Exception):
    """Base class for errors that should not be retried."""
    pass


class ConfigurationError(NonRetryableError):
    """Configuration or validation errors."""
    pass


class ResourceNotFoundError(NonRetryableError):
    """Resource not found errors."""
    pass


class ValidationError(NonRetryableError):
    """Validation errors."""
    pass


class PodNotFoundError(NonRetryableError):
    """Pod not found errors."""
    pass


class TransientError(Exception):
    """Base class for transient errors that should be retried."""
    pass


class NetworkError(TransientError):
    """Network-related errors."""
    pass


class APIError(TransientError):
    """Kubernetes API errors."""
    pass


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
    State machine for health check operations using Temporal's built-in retry capabilities.

    This leverages Temporal's RetryPolicy for robust health checking with
    exponential backoff and intelligent retry logic.
    """

    @workflow.run
    async def run(self, input_data: HealthCheckInput) -> HealthCheckResult:
        """
        Execute health check with Temporal's built-in retry capabilities.

        Uses RetryPolicy with exponential backoff to handle different health states.
        """
        # Handle case where input_data might be a dict due to serialization issues
        if isinstance(input_data, dict):
            from .models import HealthCheckInput, CrateDBCluster
            # Handle nested cluster object conversion
            if isinstance(input_data.get('cluster'), dict):
                input_data['cluster'] = CrateDBCluster(**input_data['cluster'])
            input_data = HealthCheckInput(**input_data)

        workflow.logger.info(f"Starting health check state machine for cluster {input_data.cluster.name}")

        # Use Temporal's RetryPolicy with intelligent configuration
        timeouts = StateMachineConfig.get_standard_timeouts("health_check")
        retry_policy = StateMachineConfig.get_standard_retry_policy("health_check")

        try:
            health_result = await workflow.execute_activity(
                "check_cluster_health",
                input_data,
                start_to_close_timeout=timeouts["start_to_close_timeout"],
                heartbeat_timeout=timeouts["heartbeat_timeout"],
                retry_policy=retry_policy,
            )

            workflow.logger.info(f"Health check completed for cluster {input_data.cluster.name}: {health_result['health_status']}")

            # Check if result is GREEN
            if health_result['health_status'] == "GREEN":
                return HealthCheckResult(**health_result)
            else:
                # Let the activity handle non-GREEN states with its own retry logic
                raise HealthNotGreenException(
                    health_result['health_status'],
                    f"Cluster {input_data.cluster.name} health is {health_result['health_status']}, expected GREEN"
                )

        except Exception as e:
            error_msg = f"Health check failed for cluster {input_data.cluster.name}: {e}"
            workflow.logger.error(error_msg)
            raise HealthNotGreenException("UNKNOWN", error_msg)


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

        # Initial maintenance window check using standardized configuration
        timeouts = StateMachineConfig.get_standard_timeouts("maintenance_check")
        retry_policy = StateMachineConfig.get_standard_retry_policy("maintenance_check")

        maintenance_result = await workflow.execute_activity(
            "check_maintenance_window",
            input_data,
            start_to_close_timeout=timeouts["start_to_close_timeout"],
            heartbeat_timeout=timeouts["heartbeat_timeout"],
            retry_policy=retry_policy,
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
                    start_to_close_timeout=timeouts["start_to_close_timeout"],
                    heartbeat_timeout=timeouts["heartbeat_timeout"],
                    retry_policy=retry_policy,
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

            # Use comprehensive configuration for decommission activity
            decommission_config = StateMachineConfig.create_activity_config("decommission", input_data.cluster)

            decommission_result = await workflow.execute_activity(
                "decommission_pod",
                decommission_input,
                **decommission_config,
            )

            if not decommission_result['success']:
                raise Exception(f"Decommission failed: {decommission_result['error']}")

            workflow.logger.info(f"[STATE: DECOMMISSION] Pod {input_data.pod_name} decommissioned successfully")

            # STATE 3: DELETE - Delete the pod
            workflow.logger.info(f"[STATE: DELETE] Deleting pod {input_data.pod_name}")

            # Use comprehensive configuration for pod delete with custom timeout
            delete_config = StateMachineConfig.create_activity_config("pod_operations", timeout=60)

            await workflow.execute_activity(
                "delete_pod",
                input_data,
                **delete_config,
            )

            workflow.logger.info(f"[STATE: DELETE] Pod {input_data.pod_name} deleted successfully")

            # STATE 4: WAIT_READY - Wait for pod to be ready after recreation
            workflow.logger.info(f"[STATE: WAIT_READY] Waiting for pod {input_data.pod_name} to be ready")

            # Use comprehensive configuration for pod wait with custom timeout
            wait_config = StateMachineConfig.create_activity_config("pod_operations", timeout=input_data.pod_ready_timeout)

            try:
                await workflow.execute_activity(
                    "wait_for_pod_ready",
                    input_data,
                    **wait_config,
                )

                workflow.logger.info(f"[STATE: WAIT_READY] Pod {input_data.pod_name} is ready")
            except ResourceNotFoundError as e:
                # Pod failed to start - this is a terminal condition
                workflow.logger.error(f"[STATE: WAIT_READY] Pod {input_data.pod_name} failed to start: {e}")
                workflow.logger.error(f"[STATE: WAIT_READY] This requires manual investigation - check pod logs and events")
                raise Exception(f"Pod startup failed: {e}")
            except ConfigurationError as e:
                # Pod configuration issue
                workflow.logger.error(f"[STATE: WAIT_READY] Pod {input_data.pod_name} configuration issue: {e}")
                raise Exception(f"Pod configuration error: {e}")

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
                    # Use very conservative configuration for routing reset to prevent workflow timeout issues
                    reset_config = {
                        "start_to_close_timeout": timedelta(seconds=60),
                        "heartbeat_timeout": timedelta(seconds=15),
                        "retry_policy": RetryPolicy(
                            initial_interval=timedelta(seconds=5),
                            maximum_interval=timedelta(seconds=15),
                            maximum_attempts=2,
                            backoff_coefficient=1.5,
                            non_retryable_error_types=["ActivityCancellationError", "ConfigurationError", "ValidationError"]
                        )
                    }

                    reset_result = await workflow.execute_activity(
                        "reset_cluster_routing_allocation",
                        reset_input,
                        **reset_config,
                    )

                    workflow.logger.info(f"[STATE: RESET_ROUTING] Successfully reset cluster routing allocation for {input_data.pod_name}")

                except Exception as e:
                    workflow.logger.error(f"[STATE: RESET_ROUTING] Failed to reset cluster routing allocation: {e}")
                    # Don't fail the entire restart, but log the issue with clearer instructions
                    workflow.logger.error(f"[STATE: RESET_ROUTING] Manual intervention may be required for cluster {input_data.cluster.name}")
                    workflow.logger.error(f"[STATE: RESET_ROUTING] Execute manually:")
                    workflow.logger.error(f"[STATE: RESET_ROUTING]   kubectl exec -n {input_data.namespace} {input_data.pod_name} -c crate -- \\")
                    workflow.logger.error(f"[STATE: RESET_ROUTING]     curl --insecure -sS -H 'Content-Type: application/json' -X POST \\")
                    workflow.logger.error(f"[STATE: RESET_ROUTING]     https://127.0.0.1:4200/_sql \\")
                    workflow.logger.error(f"[STATE: RESET_ROUTING]     -d '{{\"stmt\": \"set global transient \\\"cluster.routing.allocation.enable\\\" = \\\"all\\\"\"}}'")
                    workflow.logger.warning(f"[STATE: RESET_ROUTING] Continuing with restart despite routing reset failure")
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
        self.pause_signal = False
        self.cancel_signal = False
        self.current_pod = ""
        self.pods_completed = []
        self.skipped_pods = []

    @workflow.signal
    def force_restart(self, reason: str):
        """Signal to override maintenance window restrictions."""
        self.force_restart_signal = True
        self.force_restart_reason = reason
        workflow.logger.info(f"Received force restart signal: {reason}")

    @workflow.signal
    def pause_restart(self, reason: str):
        """Pause the restart process."""
        self.pause_signal = True
        workflow.logger.info(f"Restart paused: {reason}")

    @workflow.signal
    def resume_restart(self):
        """Resume the restart process."""
        self.pause_signal = False
        workflow.logger.info("Restart resumed")

    @workflow.signal
    def cancel_restart(self, reason: str):
        """Cancel the restart process."""
        self.cancel_signal = True
        workflow.logger.info(f"Restart cancelled: {reason}")

    @workflow.query
    def get_status(self) -> dict:
        """Get current restart status."""
        return {
            "current_pod": self.current_pod,
            "pods_completed": self.pods_completed,
            "skipped_pods": self.skipped_pods,
            "paused": self.pause_signal,
            "cancelled": self.cancel_signal,
            "force_restart_active": self.force_restart_signal
        }

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

            # Use comprehensive configuration for validation
            validation_config = StateMachineConfig.create_activity_config("api_calls")

            validation_result = await workflow.execute_activity(
                "validate_cluster",
                validation_input,
                **validation_config,
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
                # Check for cancellation signal
                if self.cancel_signal:
                    workflow.logger.info(f"[STATE: POD_RESTARTS] Restart cancelled, stopping at pod {pod_name}")
                    break

                # Check for pause signal
                if self.pause_signal:
                    workflow.logger.info(f"[STATE: POD_RESTARTS] Restart paused at pod {pod_name}")
                    # Wait for resume signal
                    await workflow.wait_condition(
                        lambda: not self.pause_signal or self.cancel_signal,
                        timeout=timedelta(hours=24)  # Wait up to 24 hours
                    )

                    if self.cancel_signal:
                        workflow.logger.info(f"[STATE: POD_RESTARTS] Restart cancelled during pause")
                        break

                    workflow.logger.info(f"[STATE: POD_RESTARTS] Restart resumed at pod {pod_name}")

                # Update current pod for status queries
                self.current_pod = pod_name

                workflow.logger.info(f"[STATE: POD_RESTARTS] Checking pod {i+1}/{len(cluster.pods)}: {pod_name}")

                # Check if we should only restart pods on suspended nodes
                if options.only_on_suspended_nodes:
                    workflow.logger.info(f"[STATE: POD_RESTARTS] Checking if pod {pod_name} is on suspended node")

                    try:
                        # Use comprehensive configuration for node status check
                        node_check_config = StateMachineConfig.create_activity_config("api_calls")

                        is_on_suspended_node = await workflow.execute_activity(
                            "is_pod_on_suspended_node",
                            args=[pod_name, cluster.namespace],
                            **node_check_config,
                        )

                        if not is_on_suspended_node:
                            workflow.logger.info(f"[STATE: POD_RESTARTS] Skipping pod {pod_name} - not on suspended node")
                            skipped_pods.append(pod_name)
                            self.skipped_pods.append(pod_name)
                            continue

                        workflow.logger.info(f"[STATE: POD_RESTARTS] Pod {pod_name} is on suspended node, proceeding with restart")
                    except Exception as e:
                        workflow.logger.error(f"[STATE: POD_RESTARTS] Failed to check node status for pod {pod_name}: {e}")
                        workflow.logger.info(f"[STATE: POD_RESTARTS] Skipping pod {pod_name} due to node check failure")
                        skipped_pods.append(pod_name)
                        self.skipped_pods.append(pod_name)
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
                self.pods_completed.append(pod_name)
                workflow.logger.info(f"[STATE: POD_RESTARTS] Successfully restarted pod {pod_name}")

                # Health check after each pod restart (except the last pod in the list)
                # We do this conservatively to ensure cluster stability
                if i < len(cluster.pods) - 1:
                    workflow.logger.info(f"[STATE: POD_RESTARTS] Health check after restarting {pod_name}")

                    # Brief stabilization wait
                    await workflow.sleep(timedelta(seconds=5))

                    # Health check with state machine
                    inter_health_input = HealthCheckInput(
                        cluster=cluster,
                        dry_run=options.dry_run,
                        timeout=options.health_check_timeout,
                    )
                    await workflow.execute_child_workflow(
                        HealthCheckStateMachine.run,
                        args=[inter_health_input],
                        id=f"inter-health-{pod_name}-{workflow.now().timestamp()}",
                        task_timeout=timedelta(seconds=600),  # 10 minutes max
                    )

                    workflow.logger.info(f"[STATE: POD_RESTARTS] Health check passed after restarting {pod_name}")

            # STATE 5: FINAL_HEALTH - Final health check (only if pods were restarted)
            if restarted_pods:
                workflow.logger.info(f"[STATE: FINAL_HEALTH] Performing final health check for {cluster.name}")

                # Use health check state machine for robust health validation
                final_health_input = HealthCheckInput(
                    cluster=cluster,
                    dry_run=options.dry_run,
                    timeout=options.health_check_timeout,
                )
                await workflow.execute_child_workflow(
                    HealthCheckStateMachine.run,
                    args=[final_health_input],
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
            if self.cancel_signal:
                workflow.logger.info(f"[STATE: COMPLETE] Cluster restart CANCELLED for {cluster.name} after {duration:.2f}s")
                workflow.logger.info(f"[STATE: COMPLETE] Completed {len(restarted_pods)} pods, skipped {len(skipped_pods)} pods before cancellation")
            elif skipped_pods:
                workflow.logger.info(f"[STATE: COMPLETE] Cluster restart completed for {cluster.name} in {duration:.2f}s")
                workflow.logger.info(f"[STATE: COMPLETE] Restarted {len(restarted_pods)} pods, skipped {len(skipped_pods)} pods")
                workflow.logger.info(f"[STATE: COMPLETE] Skipped pods: {', '.join(skipped_pods)}")
            else:
                workflow.logger.info(f"[STATE: COMPLETE] Cluster restart completed for {cluster.name} in {duration:.2f}s")

            return {
                "cluster": cluster,
                "success": True if not self.cancel_signal else False,
                "duration": duration,
                "restarted_pods": restarted_pods,
                "skipped_pods": skipped_pods,
                "total_pods": len(cluster.pods),
                "started_at": start_time,
                "completed_at": end_time,
                "cancelled": self.cancel_signal,
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
                "skipped_pods": skipped_pods,
                "total_pods": len(cluster.pods),
                "error": error_msg,
                "started_at": start_time,
                "completed_at": end_time,
                "cancelled": self.cancel_signal,
            }


# Utility functions for state machine configuration
class StateMachineConfig:
    """Enhanced configuration utilities for state machines."""

    @staticmethod
    def get_comprehensive_config(operation_type: str, cluster: CrateDBCluster = None) -> dict:
        """
        Get comprehensive configuration for a specific operation type.

        Args:
            operation_type: Type of operation (health_check, decommission, etc.)
            cluster: Optional cluster object for cluster-specific configurations

        Returns:
            Dictionary containing retry_policy, timeouts, and other configurations
        """
        base_config = {
            "retry_policy": StateMachineConfig.get_standard_retry_policy(operation_type),
            "timeouts": StateMachineConfig.get_standard_timeouts(operation_type),
        }

        # Add cluster-specific configurations
        if cluster and operation_type == "decommission":
            base_config["timeouts"]["start_to_close_timeout"] = timedelta(
                seconds=cluster.dc_util_timeout + 120
            )
            # Adjust retry attempts based on cluster type
            if cluster.has_dc_util:
                base_config["retry_policy"] = RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(seconds=60),
                    maximum_attempts=2,  # Fewer retries for K8s-managed
                    backoff_coefficient=2.0,
                    non_retryable_error_types=["ConfigurationError", "ValidationError", "PodNotFoundError"]
                )

        return base_config

    @staticmethod
    def get_standard_retry_policy(operation_type: str) -> RetryPolicy:
        """Get standardized retry policies by operation type."""
        policies = {
            "health_check": RetryPolicy(
                initial_interval=timedelta(seconds=5),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=30,
                backoff_coefficient=2.0,
                non_retryable_error_types=["ConfigurationError", "ValidationError"]
            ),
            "decommission": RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
                backoff_coefficient=2.0,
                non_retryable_error_types=["ConfigurationError", "ValidationError", "PodNotFoundError", "ActivityCancellationError"]
            ),
            "pod_operations": RetryPolicy(
                initial_interval=timedelta(seconds=5),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=5,
                backoff_coefficient=2.0,
                non_retryable_error_types=["ResourceNotFoundError", "ValidationError", "ActivityCancellationError"]
            ),
            "api_calls": RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3,
                backoff_coefficient=2.0,
                non_retryable_error_types=["ConfigurationError", "ValidationError", "ActivityCancellationError"]
            ),
            "maintenance_check": RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3,
                backoff_coefficient=2.0,
                non_retryable_error_types=["ConfigurationError", "ActivityCancellationError"]
            )
        }
        return policies.get(operation_type, RetryPolicy(maximum_attempts=3))

    @staticmethod
    def get_standard_timeouts(operation_type: str) -> dict:
        """Get standardized timeout configurations."""
        timeouts = {
            "health_check": {
                "start_to_close_timeout": timedelta(minutes=10),
                "heartbeat_timeout": timedelta(seconds=30),
            },
            "decommission": {
                "start_to_close_timeout": timedelta(minutes=15),
                "heartbeat_timeout": timedelta(seconds=30),
            },
            "pod_operations": {
                "start_to_close_timeout": timedelta(minutes=5),
                "heartbeat_timeout": timedelta(seconds=30),
            },
            "api_calls": {
                "start_to_close_timeout": timedelta(seconds=30),
                "heartbeat_timeout": timedelta(seconds=10),
            },
            "maintenance_check": {
                "start_to_close_timeout": timedelta(seconds=30),
                "heartbeat_timeout": timedelta(seconds=10),
            }
        }
        return timeouts.get(operation_type, {
            "start_to_close_timeout": timedelta(minutes=5),
            "heartbeat_timeout": timedelta(seconds=30),
        })

    @staticmethod
    def create_activity_config(operation_type: str, cluster: CrateDBCluster = None, **overrides) -> dict:
        """
        Create a complete activity configuration with overrides.

        Args:
            operation_type: Type of operation
            cluster: Optional cluster object
            **overrides: Configuration overrides

        Returns:
            Dictionary ready for workflow.execute_activity()
        """
        config = StateMachineConfig.get_comprehensive_config(operation_type, cluster)

        # Apply overrides
        if "timeout" in overrides:
            config["timeouts"]["start_to_close_timeout"] = timedelta(seconds=overrides["timeout"])

        if "max_attempts" in overrides:
            config["retry_policy"] = RetryPolicy(
                initial_interval=config["retry_policy"].initial_interval,
                maximum_interval=config["retry_policy"].maximum_interval,
                maximum_attempts=overrides["max_attempts"],
                backoff_coefficient=config["retry_policy"].backoff_coefficient,
                non_retryable_error_types=config["retry_policy"].non_retryable_error_types
            )

        return {
            "start_to_close_timeout": config["timeouts"]["start_to_close_timeout"],
            "heartbeat_timeout": config["timeouts"]["heartbeat_timeout"],
            "retry_policy": config["retry_policy"]
        }

    @staticmethod
    def get_health_check_retry_policy(health_state: str) -> RetryPolicy:
        """Get retry policy based on health state (legacy method - use get_standard_retry_policy instead)."""
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

    @staticmethod
    def should_retry_error(error: Exception) -> bool:
        """
        Determine if an error should be retried based on its type and content.

        Args:
            error: Exception to evaluate

        Returns:
            True if the error should be retried, False otherwise
        """
        error_str = str(error).lower()

        # Non-retryable error types
        if isinstance(error, (ConfigurationError, ValidationError, ResourceNotFoundError, PodNotFoundError)):
            return False

        # Non-retryable error patterns
        non_retryable_patterns = [
            "not found",
            "invalid configuration",
            "permission denied",
            "unauthorized",
            "forbidden",
            "bad request",
            "malformed"
        ]

        for pattern in non_retryable_patterns:
            if pattern in error_str:
                return False

        # Default to retryable for transient errors
        return True
