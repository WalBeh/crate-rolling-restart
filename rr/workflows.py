"""
Temporal workflows for CrateDB cluster restart operations.
"""

import asyncio
from datetime import timedelta
from typing import List

from temporalio import workflow
from temporalio.common import RetryPolicy

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from .activities import CrateDBActivities
    from .models import (
        ClusterDiscoveryInput,
        ClusterValidationInput,
        CrateDBCluster,
        HealthCheckInput,
        MaintenanceWindowCheckInput,
        MaintenanceWindowCheckResult,
        MultiClusterRestartInput,
        MultiClusterRestartResult,
        PodRestartInput,
        RestartOptions,
        RestartResult,
    )


@workflow.defn
class ClusterRestartWorkflow:
    """Workflow for restarting a single CrateDB cluster."""

    def __init__(self):
        self.force_restart_signal = False
        self.force_restart_reason = ""

    @workflow.signal
    def force_restart(self, reason: str = "Operator override"):
        """Signal to force restart outside maintenance window."""
        self.force_restart_signal = True
        self.force_restart_reason = reason
        workflow.logger.info(f"Operator override received: {reason}")

    @workflow.run
    async def run(self, cluster: CrateDBCluster, options: RestartOptions) -> RestartResult:
        """
        Restart a single CrateDB cluster.
        
        Args:
            cluster: The cluster to restart
            options: Restart options
            
        Returns:
            RestartResult with the outcome
        """
        start_time = workflow.now()
        restarted_pods = []
        
        workflow.logger.info(f"Starting restart workflow for cluster {cluster.name}")
        
        # Reset override flag for this run
        self.force_restart_signal = False
        self.force_restart_reason = ""
        
        try:
            # Check maintenance window (unless explicitly ignored)
            if not options.ignore_maintenance_windows and options.maintenance_config_path:
                maintenance_result = await workflow.execute_activity(
                    CrateDBActivities.check_maintenance_window,
                    MaintenanceWindowCheckInput(
                        cluster_name=cluster.name,
                        current_time=workflow.now(),
                        config_path=options.maintenance_config_path
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=10),
                        maximum_attempts=3,
                    ),
                )
                
                workflow.logger.info(f"Maintenance window check for {cluster.name}: {maintenance_result.reason}")
                
                if maintenance_result.should_wait:
                    # Wait for maintenance window or operator signal
                    workflow.logger.warning(f"Cluster {cluster.name} is OUTSIDE its maintenance window - restart delayed: {maintenance_result.reason}")
                    
                    # Wait for either maintenance window or operator override
                    while True:
                        # Check for force restart signal first
                        if self.force_restart_signal:
                            workflow.logger.info(f"Proceeding with restart due to operator override: {self.force_restart_reason}")
                            break
                        
                        # Check maintenance window periodically
                        maintenance_check = await workflow.execute_activity(
                            CrateDBActivities.check_maintenance_window,
                            MaintenanceWindowCheckInput(
                                cluster_name=cluster.name,
                                current_time=workflow.now(),
                                config_path=options.maintenance_config_path
                            ),
                            start_to_close_timeout=timedelta(seconds=30),
                            retry_policy=RetryPolicy(
                                initial_interval=timedelta(seconds=1),
                                maximum_interval=timedelta(seconds=10),
                                maximum_attempts=3,
                            ),
                        )
                        
                        if not maintenance_check.should_wait:
                            workflow.logger.info(f"Maintenance window now open for {cluster.name}: {maintenance_check.reason}")
                            break
                        
                        # Log that we're still waiting outside maintenance window
                        workflow.logger.info(f"Cluster {cluster.name} still outside maintenance window, waiting 5 more minutes...")
                        
                        # Wait 5 minutes before checking again
                        await workflow.sleep(300)
            
            # Validate cluster before restart
            validation_result = await workflow.execute_activity(
                CrateDBActivities.validate_cluster,
                ClusterValidationInput(
                    cluster=cluster,
                    skip_hook_warning=options.skip_hook_warning
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=3,
                ),
            )
            
            if not validation_result.is_valid:
                error_msg = f"Cluster validation failed: {', '.join(validation_result.errors)}"
                workflow.logger.error(error_msg)
                return RestartResult(
                    cluster=cluster,
                    success=False,
                    duration=0,
                    restarted_pods=[],
                    total_pods=len(cluster.pods),
                    error=error_msg,
                    started_at=start_time,
                    completed_at=workflow.now(),
                )
            
            # Log validation warnings
            for warning in validation_result.warnings:
                workflow.logger.warning(f"Cluster {cluster.name}: {warning}")
            
            # Check initial cluster health before starting any pod restarts
            workflow.logger.info(f"Checking initial cluster health for {cluster.name}")
            initial_health_result = await workflow.execute_activity(
                CrateDBActivities.check_cluster_health,
                HealthCheckInput(
                    cluster=cluster,
                    dry_run=options.dry_run,
                    timeout=options.health_check_timeout,
                ),
                start_to_close_timeout=timedelta(seconds=options.health_check_timeout + 30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=30,  # Give cluster plenty of time to reach GREEN
                ),
            )
            
            if not initial_health_result.is_healthy:
                error_msg = f"Initial health check failed: {initial_health_result.health_status}. Cluster must be GREEN before starting restart."
                workflow.logger.error(error_msg)
                return RestartResult(
                    cluster=cluster,
                    success=False,
                    duration=(workflow.now() - start_time).total_seconds(),
                    restarted_pods=[],
                    total_pods=len(cluster.pods),
                    error=error_msg,
                    started_at=start_time,
                    completed_at=workflow.now(),
                )
            
            workflow.logger.info(f"Initial cluster health is {initial_health_result.health_status}, proceeding with pod restarts")
            
            # Calculate timeouts based on cluster configuration
            pod_restart_timeout = options.pod_ready_timeout
            if cluster.has_dc_util:
                # Add decommission timeout plus buffer
                pod_restart_timeout = cluster.dc_util_timeout + 120
            
            health_check_timeout = options.health_check_timeout
            
            workflow.logger.info(
                f"Restarting {len(cluster.pods)} pods for cluster {cluster.name} "
                f"(pod timeout: {pod_restart_timeout}s, health check timeout: {health_check_timeout}s)"
            )
            
            # Restart pods sequentially
            for i, pod_name in enumerate(cluster.pods):
                workflow.logger.info(f"Restarting pod {i+1}/{len(cluster.pods)}: {pod_name}")
                
                # Restart the pod
                pod_result = await workflow.execute_activity(
                    CrateDBActivities.restart_pod,
                    PodRestartInput(
                        pod_name=pod_name,
                        namespace=cluster.namespace,
                        cluster=cluster,
                        dry_run=options.dry_run,
                        pod_ready_timeout=pod_restart_timeout,
                    ),
                    start_to_close_timeout=timedelta(seconds=pod_restart_timeout + 60),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=2,  # Limited retries for pod restart
                    ),
                )
                
                if not pod_result.success:
                    error_msg = f"Failed to restart pod {pod_name}: {pod_result.error}"
                    workflow.logger.error(error_msg)
                    return RestartResult(
                        cluster=cluster,
                        success=False,
                        duration=(workflow.now() - start_time).total_seconds(),
                        restarted_pods=restarted_pods,
                        total_pods=len(cluster.pods),
                        error=error_msg,
                        started_at=start_time,
                        completed_at=workflow.now(),
                    )
                
                restarted_pods.append(pod_name)
                workflow.logger.info(f"Successfully restarted pod {pod_name}")
                
                # Check cluster health after each pod restart (except the last one)
                if i < len(cluster.pods) - 1:
                    workflow.logger.info(f"Checking cluster health after restarting pod {pod_name}")
                    
                    # Wait for cluster to stabilize
                    await asyncio.sleep(5)
                    
                    # Check health with retries
                    health_result = await workflow.execute_activity(
                        CrateDBActivities.check_cluster_health,
                        HealthCheckInput(
                            cluster=cluster,
                            dry_run=options.dry_run,
                            timeout=health_check_timeout,
                        ),
                        start_to_close_timeout=timedelta(seconds=health_check_timeout + 30),
                        retry_policy=RetryPolicy(
                            initial_interval=timedelta(seconds=10),
                            maximum_interval=timedelta(seconds=30),
                            maximum_attempts=30,  # Give cluster plenty of time to reach GREEN
                        ),
                    )
                    
                    if not health_result.is_healthy:
                        error_msg = f"Cluster health check failed after restarting pod {pod_name}: {health_result.health_status} (restarted {len(restarted_pods)}/{len(cluster.pods)} pods)"
                        workflow.logger.error(error_msg)
                        return RestartResult(
                            cluster=cluster,
                            success=False,
                            duration=(workflow.now() - start_time).total_seconds(),
                            restarted_pods=restarted_pods,
                            total_pods=len(cluster.pods),
                            error=error_msg,
                            started_at=start_time,
                            completed_at=workflow.now(),
                        )
                    
                    workflow.logger.info(f"Cluster health is {health_result.health_status}, proceeding with next pod")
            
            # Final health check
            workflow.logger.info("Performing final health check")
            final_health_result = await workflow.execute_activity(
                CrateDBActivities.check_cluster_health,
                HealthCheckInput(
                    cluster=cluster,
                    dry_run=options.dry_run,
                    timeout=health_check_timeout,
                ),
                start_to_close_timeout=timedelta(seconds=health_check_timeout + 30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=30,  # Give cluster plenty of time to reach GREEN
                ),
            )
            
            if not final_health_result.is_healthy:
                error_msg = f"Final health check failed: {final_health_result.health_status} (successfully restarted {len(restarted_pods)}/{len(cluster.pods)} pods)"
                workflow.logger.error(error_msg)
                return RestartResult(
                    cluster=cluster,
                    success=False,
                    duration=(workflow.now() - start_time).total_seconds(),
                    restarted_pods=restarted_pods,
                    total_pods=len(cluster.pods),
                    error=error_msg,
                    started_at=start_time,
                    completed_at=workflow.now(),
                )
            
            end_time = workflow.now()
            duration = (end_time - start_time).total_seconds()
            
            workflow.logger.info(f"Successfully restarted cluster {cluster.name} in {duration:.2f}s")
            
            return RestartResult(
                cluster=cluster,
                success=True,
                duration=duration,
                restarted_pods=restarted_pods,
                total_pods=len(cluster.pods),
                started_at=start_time,
                completed_at=end_time,
            )
            
        except Exception as e:
            end_time = workflow.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Unexpected error during cluster restart: {e}"
            workflow.logger.error(error_msg)
            
            return RestartResult(
                cluster=cluster,
                success=False,
                duration=duration,
                restarted_pods=restarted_pods,
                total_pods=len(cluster.pods),
                error=error_msg,
                started_at=start_time,
                completed_at=end_time,
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
                CrateDBActivities.discover_clusters,
                ClusterDiscoveryInput(
                    cluster_names=input_data.cluster_names,
                    kubeconfig=input_data.options.kubeconfig,
                    context=input_data.options.context,
                ),
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
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
            CrateDBActivities.discover_clusters,
            input_data,
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3,
            ),
        )
        
        workflow.logger.info(f"Discovery completed: found {result.total_found} clusters")
        return result