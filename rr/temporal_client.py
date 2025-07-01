"""
Temporal client for executing CrateDB cluster operations.
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional
import uuid

from loguru import logger
from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import pydantic_data_converter

from .models import (
    ClusterDiscoveryInput,
    MultiClusterRestartInput,
    MultiClusterRestartResult,
    RestartOptions,
)

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from .workflows import ClusterDiscoveryWorkflow, MultiClusterRestartWorkflow


class TemporalClient:
    """Client for executing CrateDB operations via Temporal workflows."""

    def __init__(self, temporal_address: str = "localhost:7233", task_queue: str = "cratedb-operations"):
        self.temporal_address = temporal_address
        self.task_queue = task_queue
        self.client: Optional[Client] = None

    async def connect(self) -> None:
        """Connect to Temporal server."""
        try:
            self.client = await Client.connect(
                self.temporal_address,
                data_converter=pydantic_data_converter
            )
            logger.info(f"Connected to Temporal server at {self.temporal_address}")
        except Exception as e:
            logger.error(f"Failed to connect to Temporal server: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Temporal server."""
        if self.client:
            # Temporal client doesn't need explicit close in this version
            self.client = None
            logger.info("Disconnected from Temporal server")

    async def discover_clusters(
        self,
        cluster_names: Optional[List[str]] = None,
        kubeconfig: Optional[str] = None,
        context: Optional[str] = None,
    ):
        """
        Discover CrateDB clusters.

        Args:
            cluster_names: Optional list of cluster names to filter
            kubeconfig: Path to kubeconfig file
            context: Kubernetes context to use

        Returns:
            ClusterDiscoveryResult
        """
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        workflow_id = f"discover-clusters-{uuid.uuid4().hex[:8]}"

        try:
            result = await self.client.execute_workflow(
                ClusterDiscoveryWorkflow.run,
                ClusterDiscoveryInput(
                    cluster_names=cluster_names,
                    kubeconfig=kubeconfig,
                    context=context,
                ),
                id=workflow_id,
                task_queue=self.task_queue,
                execution_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=3,
                ),
            )

            # Handle case where workflow returns dict instead of Pydantic model
            # This can happen with temporal server start-dev or data converter issues
            if isinstance(result, dict):
                logger.debug("Workflow returned dict, converting to ClusterDiscoveryResult")
                from .models import ClusterDiscoveryResult, CrateDBCluster

                clusters = []
                if 'clusters' in result and isinstance(result['clusters'], list):
                    for cluster_data in result['clusters']:
                        if isinstance(cluster_data, dict):
                            try:
                                clusters.append(CrateDBCluster(**cluster_data))
                            except Exception as e:
                                logger.error(f"Failed to convert cluster data: {e}")
                        elif hasattr(cluster_data, '__dict__'):
                            clusters.append(cluster_data)

                result = ClusterDiscoveryResult(
                    clusters=clusters,
                    total_found=result.get('total_found', len(clusters)),
                    errors=result.get('errors', [])
                )

            logger.info(f"Cluster discovery completed: found {result.total_found} clusters")
            return result

        except WorkflowFailureError as e:
            logger.error(f"Cluster discovery workflow failed: {e}")
            if hasattr(e, 'cause') and e.cause:
                logger.error(f"Workflow failure cause: {e.cause}")
            if hasattr(e, 'failure') and e.failure:
                logger.error(f"Workflow failure details: {e.failure}")
            raise
        except Exception as e:
            logger.error(f"Error executing cluster discovery: {e}")
            logger.exception("Full traceback:")
            raise

    async def restart_clusters(
        self,
        cluster_names: List[str],
        options: RestartOptions,
        wait_for_completion: bool = True,
    ) -> MultiClusterRestartResult:
        """
        Restart CrateDB clusters.

        Args:
            cluster_names: List of cluster names to restart
            options: Restart options
            wait_for_completion: Whether to wait for completion

        Returns:
            MultiClusterRestartResult or WorkflowHandle if not waiting
        """
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        workflow_id = f"restart-clusters-{uuid.uuid4().hex[:8]}"

        try:
            if wait_for_completion:
                result = await self.client.execute_workflow(
                    MultiClusterRestartWorkflow.run,
                    MultiClusterRestartInput(
                        cluster_names=cluster_names,
                        options=options,
                    ),
                    id=workflow_id,
                    task_queue=self.task_queue,
                    execution_timeout=timedelta(hours=2),  # Long timeout for cluster restarts
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=1,  # Don't retry the entire workflow
                    ),
                )

                # Handle case where result is dict instead of Pydantic model
                if isinstance(result, dict):
                    logger.debug(f"Restart workflow returned dict: {result}")
                    logger.debug("Converting dict to MultiClusterRestartResult")
                    from .models import MultiClusterRestartResult, RestartResult

                    restart_results = []
                    if 'results' in result and isinstance(result['results'], list):
                        for result_data in result['results']:
                            if isinstance(result_data, dict):
                                try:
                                    restart_results.append(RestartResult(**result_data))
                                except Exception as e:
                                    logger.error(f"Failed to convert restart result: {e}")
                            elif hasattr(result_data, '__dict__'):
                                restart_results.append(result_data)

                    result = MultiClusterRestartResult(
                        results=restart_results,
                        total_clusters=result.get('total_clusters', len(restart_results)),
                        successful_clusters=result.get('successful_clusters', 0),
                        failed_clusters=result.get('failed_clusters', 0),
                        total_duration=result.get('total_duration', 0.0),
                        started_at=result.get('started_at'),
                        completed_at=result.get('completed_at')
                    )
                    logger.debug(f"Converted restart result: {result}")

                logger.info(
                    f"Cluster restart completed: {result.successful_clusters} successful, "
                    f"{result.failed_clusters} failed out of {result.total_clusters} clusters"
                )
                return result
            else:
                # Start workflow without waiting
                handle = await self.client.start_workflow(
                    MultiClusterRestartWorkflow.run,
                    MultiClusterRestartInput(
                        cluster_names=cluster_names,
                        options=options,
                    ),
                    id=workflow_id,
                    task_queue=self.task_queue,
                    execution_timeout=timedelta(hours=2),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=1,
                    ),
                )

                logger.info(f"Started cluster restart workflow: {workflow_id}")
                return handle

        except WorkflowFailureError as e:
            logger.error(f"Cluster restart workflow failed: {e}")
            if hasattr(e, 'cause') and e.cause:
                logger.error(f"Workflow failure cause: {e.cause}")
            if hasattr(e, 'failure') and e.failure:
                logger.error(f"Workflow failure details: {e.failure}")
            raise
        except Exception as e:
            logger.error(f"Error executing cluster restart: {e}")
            logger.exception("Full traceback:")
            raise

    async def get_workflow_status(self, workflow_id: str) -> dict:
        """
        Get the status of a workflow.

        Args:
            workflow_id: Workflow ID

        Returns:
            Dictionary with workflow status information
        """
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            handle = self.client.get_workflow_handle(workflow_id)

            # Get workflow description
            description = await handle.describe()

            status = {
                "workflow_id": workflow_id,
                "status": self._format_workflow_status(description.status),
                "run_id": description.run_id,
                "start_time": description.start_time,
                "execution_time": description.execution_time,
                "close_time": description.close_time,
                "task_queue": description.task_queue,
                "workflow_type": description.workflow_type,
            }

            return status

        except Exception as e:
            logger.error(f"Error getting workflow status: {e}")
            raise

    async def cancel_workflow(self, workflow_id: str) -> None:
        """
        Cancel a running workflow.

        Args:
            workflow_id: Workflow ID to cancel
        """
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            handle = self.client.get_workflow_handle(workflow_id)
            await handle.cancel()
            logger.info(f"Cancelled workflow: {workflow_id}")

        except Exception as e:
            logger.error(f"Error cancelling workflow: {e}")
            raise

    async def signal_workflow(self, workflow_id: str, signal_name: str, *args) -> None:
        """
        Send a signal to a running workflow.

        Args:
            workflow_id: Workflow ID to signal
            signal_name: Name of the signal to send
            *args: Arguments to pass to the signal
        """
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            handle = self.client.get_workflow_handle(workflow_id)
            await handle.signal(signal_name, *args)
            logger.info(f"Sent signal '{signal_name}' to workflow: {workflow_id}")

        except Exception as e:
            logger.error(f"Error sending signal to workflow: {e}")
            raise

    async def force_restart_workflow(self, workflow_id: str, reason: str = "Operator override") -> None:
        """
        Send force_restart signal to override maintenance window restrictions.

        Args:
            workflow_id: Workflow ID to signal
            reason: Reason for the override
        """
        await self.signal_workflow(workflow_id, "force_restart", reason)
        logger.info(f"Sent force restart signal to workflow {workflow_id}: {reason}")

    def _format_workflow_status(self, status) -> str:
        """Format workflow status enum to readable string."""
        status_str = str(status)
        
        # Map common status values to readable names
        status_mapping = {
            "WorkflowExecutionStatus.RUNNING": "Running",
            "WorkflowExecutionStatus.COMPLETED": "Completed", 
            "WorkflowExecutionStatus.FAILED": "Failed",
            "WorkflowExecutionStatus.CANCELED": "Canceled",
            "WorkflowExecutionStatus.TERMINATED": "Terminated",
            "WorkflowExecutionStatus.CONTINUED_AS_NEW": "Continued",
            "WorkflowExecutionStatus.TIMED_OUT": "Timed Out",
        }
        
        # Return mapped name or the numeric part if mapping not found
        if status_str in status_mapping:
            return status_mapping[status_str]
        elif "." in status_str:
            return status_str.split(".")[-1].replace("_", " ").title()
        else:
            # Fallback to simple mapping for numeric values
            numeric_mapping = {
                "1": "Running",
                "2": "Completed", 
                "3": "Failed",
                "4": "Canceled",
                "5": "Terminated",
                "6": "Continued",
                "7": "Timed Out",
            }
            return numeric_mapping.get(status_str, status_str)

    async def list_workflows(self, limit: int = 10) -> List[dict]:
        """
        List recent workflows.

        Args:
            limit: Maximum number of workflows to return

        Returns:
            List of workflow information
        """
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        try:
            # List workflows using the list_workflows API
            workflows = []
            async for workflow in self.client.list_workflows():
                workflows.append({
                    "workflow_id": workflow.id,
                    "run_id": workflow.run_id,
                    "workflow_type": workflow.workflow_type,
                    "status": self._format_workflow_status(workflow.status),
                    "start_time": workflow.start_time,
                    "execution_time": workflow.execution_time,
                    "close_time": workflow.close_time,
                    "task_queue": workflow.task_queue,
                })

                if len(workflows) >= limit:
                    break

            return workflows

        except Exception as e:
            logger.error(f"Error listing workflows: {e}")
            raise

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


async def create_temporal_client(
    temporal_address: str = "localhost:7233",
    task_queue: str = "cratedb-operations"
) -> TemporalClient:
    """
    Create and connect a Temporal client.

    Args:
        temporal_address: Temporal server address
        task_queue: Task queue name

    Returns:
        Connected TemporalClient instance
    """
    client = TemporalClient(temporal_address, task_queue)
    await client.connect()
    return client
