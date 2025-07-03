"""
Temporal workflows for CrateDB cluster operations.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from .activities import CrateDBActivities
    from .models import (
        ClusterDiscoveryInput,
        ClusterDiscoveryResult,
        CrateDBCluster,
        DecommissionInput,
        DecommissionResult,
        MultiClusterRestartInput,
        MultiClusterRestartResult,
        RestartOptions,
        RestartResult,
    )
    from .state_machines import ClusterRestartStateMachine


@workflow.defn
class ClusterRestartWorkflow:
    """Workflow for restarting a single CrateDB cluster using state machine approach."""

    def __init__(self):
        self.force_restart_signal = False
        self.force_restart_reason = ""

    @workflow.signal
    def force_restart(self, reason: str):
        """Force restart signal to bypass maintenance window restrictions."""
        self.force_restart_signal = True
        self.force_restart_reason = reason

    @workflow.run
    async def run(self, cluster: CrateDBCluster, options: RestartOptions) -> RestartResult:
        """
        Restart a single CrateDB cluster using state machine orchestration.

        Args:
            cluster: The cluster to restart
            options: Restart options

        Returns:
            RestartResult with the outcome
        """
        workflow.logger.info(f"Starting cluster restart workflow for {cluster.name} (using state machine)")

        try:
            # Use cluster restart state machine for orchestrated restart
            result = await workflow.execute_child_workflow(
                ClusterRestartStateMachine.run,
                args=[cluster, options],
                id=f"cluster-restart-{cluster.name}-{workflow.now().timestamp()}",
                task_timeout=timedelta(hours=4),  # Allow for long operations
            )

            # Convert state machine result to RestartResult
            return RestartResult(
                cluster=result["cluster"],
                success=result["success"],
                duration=result["duration"],
                restarted_pods=result["restarted_pods"],
                total_pods=result["total_pods"],
                error=result.get("error"),
                started_at=result["started_at"],
                completed_at=result["completed_at"],
            )

        except Exception as e:
            error_msg = f"Cluster restart workflow failed for {cluster.name}: {e}"
            workflow.logger.error(error_msg)
            
            return RestartResult(
                cluster=cluster,
                success=False,
                duration=0,
                restarted_pods=[],
                total_pods=len(cluster.pods),
                error=error_msg,
                started_at=workflow.now(),
                completed_at=workflow.now(),
            )


@workflow.defn
class MultiClusterRestartWorkflow:
    """Workflow for restarting multiple CrateDB clusters."""

    @workflow.run
    async def run(self, input_data: MultiClusterRestartInput) -> MultiClusterRestartResult:
        """
        Restart multiple CrateDB clusters.

        Args:
            input_data: Multi-cluster restart parameters

        Returns:
            MultiClusterRestartResult with all outcomes
        """
        start_time = workflow.now()
        workflow.logger.info(f"Starting multi-cluster restart workflow for: {input_data.cluster_names}")

        try:
            # Discover clusters
            workflow.logger.info(f"Discovering clusters in restart workflow with names: {input_data.cluster_names}")
            discovery_result = await workflow.execute_activity(
                "discover_clusters",
                ClusterDiscoveryInput(
                    cluster_names=input_data.cluster_names,
                    kubeconfig=input_data.options.kubeconfig,
                    context=input_data.options.context,
                    maintenance_config_path=input_data.options.maintenance_config_path,
                ),
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=3,
                ),
            )

            # Handle case where discovery_result is dict instead of Pydantic model
            workflow.logger.info(f"Discovery result type: {type(discovery_result)}")
            workflow.logger.info(f"Discovery result content: {discovery_result}")

            if isinstance(discovery_result, dict):
                workflow.logger.info("Discovery result is dict, converting to ClusterDiscoveryResult")
                from .models import ClusterDiscoveryResult, CrateDBCluster

                workflow.logger.info(f"Dict keys: {discovery_result.keys()}")
                workflow.logger.info(f"Raw clusters data: {discovery_result.get('clusters', [])}")

                clusters = []
                if 'clusters' in discovery_result and isinstance(discovery_result['clusters'], list):
                    workflow.logger.info(f"Processing {len(discovery_result['clusters'])} cluster entries")
                    for i, cluster_data in enumerate(discovery_result['clusters']):
                        workflow.logger.info(f"Cluster {i}: type={type(cluster_data)}, data={cluster_data}")
                        if isinstance(cluster_data, dict):
                            try:
                                cluster = CrateDBCluster(**cluster_data)
                                clusters.append(cluster)
                                workflow.logger.info(f"Successfully converted cluster {cluster.name}")
                            except Exception as e:
                                workflow.logger.error(f"Failed to convert cluster data {cluster_data}: {e}")
                        elif hasattr(cluster_data, '__dict__'):
                            clusters.append(cluster_data)
                            workflow.logger.info(f"Using existing cluster object: {cluster_data.name if hasattr(cluster_data, 'name') else 'unknown'}")
                else:
                    workflow.logger.error(f"No clusters found in dict or clusters is not a list: {discovery_result.get('clusters', 'MISSING')}")

                workflow.logger.info(f"Converted {len(clusters)} clusters from dict")
                discovery_result = ClusterDiscoveryResult(
                    clusters=clusters,
                    total_found=discovery_result.get('total_found', len(clusters)),
                    errors=discovery_result.get('errors', [])
                )
                workflow.logger.info(f"Created ClusterDiscoveryResult with {discovery_result.total_found} clusters")
            else:
                workflow.logger.info(f"Discovery result is already correct type: {type(discovery_result)}")

            workflow.logger.info(f"Restart workflow discovery result: found {discovery_result.total_found} clusters")
            workflow.logger.info(f"Discovery result type: {type(discovery_result)}")
            if hasattr(discovery_result, 'clusters'):
                workflow.logger.info(f"Clusters: {[c.name if hasattr(c, 'name') else str(c) for c in discovery_result.clusters]}")
            if hasattr(discovery_result, 'errors') and discovery_result.errors:
                workflow.logger.error(f"Discovery errors: {discovery_result.errors}")

            if discovery_result.errors:
                for error in discovery_result.errors:
                    workflow.logger.error(f"Discovery error: {error}")

            if not discovery_result.clusters:
                error_msg = "No clusters found to restart"
                workflow.logger.error(error_msg)
                return MultiClusterRestartResult(
                    results=[],
                    total_clusters=0,
                    successful_clusters=0,
                    failed_clusters=0,
                    total_duration=0,
                    started_at=start_time,
                    completed_at=workflow.now(),
                )

            workflow.logger.info(f"Found {len(discovery_result.clusters)} clusters to restart")

            # Restart clusters sequentially
            results = []
            for cluster in discovery_result.clusters:
                workflow.logger.info(f"Starting restart for cluster {cluster.name}")

                # Start child workflow for cluster restart
                cluster_result = await workflow.execute_child_workflow(
                    ClusterRestartWorkflow.run,
                    args=[cluster, input_data.options],
                    id=f"restart-{cluster.name}-{start_time.isoformat()}",
                    task_queue=workflow.info().task_queue,
                )

                results.append(cluster_result)

                if cluster_result.success:
                    workflow.logger.info(f"Successfully restarted cluster {cluster.name}")
                else:
                    workflow.logger.error(f"Failed to restart cluster {cluster.name}: {cluster_result.error}")

            end_time = workflow.now()
            total_duration = (end_time - start_time).total_seconds()
            successful_clusters = sum(1 for r in results if r.success)
            failed_clusters = len(results) - successful_clusters

            workflow.logger.info(
                f"Multi-cluster restart completed: {successful_clusters} successful, "
                f"{failed_clusters} failed out of {len(results)} total clusters in {total_duration:.2f}s"
            )

            return MultiClusterRestartResult(
                results=results,
                total_clusters=len(results),
                successful_clusters=successful_clusters,
                failed_clusters=failed_clusters,
                total_duration=total_duration,
                started_at=start_time,
                completed_at=end_time,
            )

        except Exception as e:
            end_time = workflow.now()
            total_duration = (end_time - start_time).total_seconds()
            error_msg = f"Unexpected error in multi-cluster restart: {e}"
            workflow.logger.error(error_msg)

            return MultiClusterRestartResult(
                results=[],
                total_clusters=0,
                successful_clusters=0,
                failed_clusters=1,  # Mark as one failure
                total_duration=total_duration,
                started_at=start_time,
                completed_at=end_time,
            )


@workflow.defn
class ClusterDiscoveryWorkflow:
    """Workflow for discovering CrateDB clusters."""

    @workflow.run
    async def run(self, input_data: ClusterDiscoveryInput):
        """
        Discover CrateDB clusters.

        Args:
            input_data: Discovery parameters

        Returns:
            ClusterDiscoveryResult with found clusters
        """
        workflow.logger.info(f"Starting cluster discovery for: {input_data.cluster_names or 'all clusters'}")

        result = await workflow.execute_activity(
            "discover_clusters",
            input_data,
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3,
            ),
        )

        # Handle case where result might be a dict due to serialization issues
        if isinstance(result, dict):
            total_found = result.get('total_found', 0)
            workflow.logger.info(f"Discovery completed: {total_found} clusters found")
            # Convert dict back to ClusterDiscoveryResult
            from .models import ClusterDiscoveryResult
            return ClusterDiscoveryResult(
                clusters=result.get('clusters', []),
                total_found=total_found,
                errors=result.get('errors', [])
            )
        else:
            workflow.logger.info(f"Discovery completed: {result.total_found} clusters found")
            return result


@workflow.defn
class DecommissionWorkflow:
    """Workflow for decommissioning CrateDB pods with intelligent strategy selection."""

    @workflow.run
    async def run(self, decommission_input: DecommissionInput) -> DecommissionResult:
        """
        Decommission a single CrateDB pod using the appropriate strategy.

        This workflow automatically detects whether to use Kubernetes-managed
        decommission (preStop hook) or manual decommission (API calls).
        """
        start_time = workflow.now()
        workflow.logger.info(f"Starting decommission workflow for pod {decommission_input.pod_name}")

        # Calculate timeout based on cluster configuration
        base_timeout = decommission_input.cluster.dc_util_timeout
        activity_timeout = base_timeout + 120  # Add buffer for activity overhead

        workflow.logger.info(f"Using timeout {activity_timeout}s for decommission activity")

        try:
            result = await workflow.execute_activity(
                "decommission_pod",
                decommission_input,
                start_to_close_timeout=timedelta(seconds=activity_timeout),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(seconds=60),
                    maximum_attempts=3 if not decommission_input.cluster.has_dc_util else 2,
                    # Manual decommission might need more retries due to API calls
                    non_retryable_error_types=["ActivityCancellationError", "PodNotFoundError"]
                ),
            )

            workflow.logger.info(f"Decommission workflow completed for pod {decommission_input.pod_name}")
            workflow.logger.info(f"Strategy used: {result.strategy_used}, Duration: {result.duration:.1f}s")

            return result

        except Exception as e:
            error_msg = f"Decommission workflow failed for pod {decommission_input.pod_name}: {str(e)}"
            workflow.logger.error(error_msg)

            # Return failed result
            return DecommissionResult(
                pod_name=decommission_input.pod_name,
                namespace=decommission_input.namespace,
                strategy_used="unknown",
                success=False,
                duration=(workflow.now() - start_time).total_seconds(),
                error=error_msg,
                started_at=start_time,
                completed_at=workflow.now()
            )
