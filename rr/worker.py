"""
Temporal worker for CrateDB cluster operations.
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from datetime import timedelta

from loguru import logger
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.pydantic import pydantic_data_converter

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from .activities import CrateDBActivities
    from .workflows import ClusterDiscoveryWorkflow, ClusterRestartWorkflow, MultiClusterRestartWorkflow, DecommissionWorkflow
    from .state_machines import (
        HealthCheckStateMachine,
        MaintenanceWindowStateMachine,
        PodRestartStateMachine,
        ClusterRestartStateMachine,
    )


class WorkerManager:
    """Manager for the Temporal worker."""

    def __init__(self, temporal_address: str = "localhost:7233", task_queue: str = "cratedb-operations"):
        self.temporal_address = temporal_address
        self.task_queue = task_queue
        self.client = None
        self.worker = None
        self.shutdown_event = asyncio.Event()

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

    async def create_worker(self) -> None:
        """Create and configure the worker."""
        if not self.client:
            raise RuntimeError("Client not connected. Call connect() first.")

        # Create activities instance
        activities = CrateDBActivities()

        # Create worker with development server compatible settings
        self.worker = Worker(
            self.client,
            task_queue=self.task_queue,
            workflows=[
                ClusterRestartWorkflow,
                MultiClusterRestartWorkflow,
                ClusterDiscoveryWorkflow,
                DecommissionWorkflow,
                # State machine workflows
                HealthCheckStateMachine,
                MaintenanceWindowStateMachine,
                PodRestartStateMachine,
                ClusterRestartStateMachine,
            ],
            activities=[
                activities.discover_clusters,
                activities.validate_cluster,
                activities.restart_pod,
                activities.check_cluster_health,
                activities.check_maintenance_window,
                activities.decommission_pod,
                activities.delete_pod,
                activities.wait_for_pod_ready,
                activities.reset_cluster_routing_allocation,
                activities.is_pod_on_suspended_node,
            ],
            # Configure worker options for development
            max_concurrent_activities=5,
            max_concurrent_workflow_tasks=10,
            max_concurrent_local_activities=5,
        )

        logger.info(f"Worker created with task queue: {self.task_queue}")

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            # Create a new event loop task to set the shutdown event
            # This is necessary because signal handlers run in the main thread
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.shutdown_event.set)
            except RuntimeError:
                # If no event loop is running, set it directly
                self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def run(self) -> None:
        """Run the worker until shutdown signal is received."""
        if not self.worker:
            raise RuntimeError("Worker not created. Call create_worker() first.")

        logger.info("Starting Temporal worker...")
        
        worker_task = None
        try:
            # Run worker in background
            worker_task = asyncio.create_task(self.worker.run())
            
            # Create a task to wait for shutdown signal
            shutdown_task = asyncio.create_task(self.shutdown_event.wait())
            
            # Wait for either worker completion or shutdown signal
            done, pending = await asyncio.wait(
                [worker_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel any pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # If shutdown was requested, cancel the worker
            if shutdown_task in done:
                logger.info("Shutdown signal received, stopping worker...")
                if not worker_task.done():
                    worker_task.cancel()
                    try:
                        await worker_task
                    except asyncio.CancelledError:
                        logger.info("Worker stopped successfully")
            else:
                logger.info("Worker completed")
                
        except Exception as e:
            logger.error(f"Error running worker: {e}")
            # Make sure to cancel worker task if it's still running
            if worker_task and not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
            raise

    async def shutdown(self) -> None:
        """Shutdown the worker and client."""
        logger.info("Shutting down worker manager...")
        
        if self.worker:
            logger.info("Stopping worker...")
            # Worker will be stopped by cancelling the run task
            
        if self.client:
            logger.info("Closing client connection...")
            # Temporal client doesn't need explicit close in this version
            self.client = None
            
        logger.info("Worker manager shutdown complete")

    @asynccontextmanager
    async def managed_worker(self):
        """Context manager for worker lifecycle."""
        try:
            await self.connect()
            await self.create_worker()
            yield self
        finally:
            await self.shutdown()


async def run_worker(
    temporal_address: str = "localhost:7233",
    task_queue: str = "cratedb-operations",
    log_level: str = "INFO"
) -> None:
    """
    Run the Temporal worker compatible with temporal server start-dev.
    
    Args:
        temporal_address: Temporal server address
        task_queue: Task queue name
        log_level: Logging level
    """
    # Setup logging
    logger.remove()
    logger.add(sys.stderr, level=log_level, format="{time} | {level} | {message}")
    
    logger.info(f"Starting CrateDB Temporal worker for development server...")
    logger.info(f"Connecting to: {temporal_address}")
    logger.info(f"Task queue: {task_queue}")
    
    worker_manager = WorkerManager(temporal_address, task_queue)
    worker_manager.setup_signal_handlers()
    
    try:
        async with worker_manager.managed_worker() as manager:
            await manager.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Worker error: {e}")
        logger.debug("Full error details:", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="CrateDB Temporal Worker")
    parser.add_argument(
        "--temporal-address",
        default="localhost:7233",
        help="Temporal server address (default: localhost:7233)"
    )
    parser.add_argument(
        "--task-queue",
        default="cratedb-operations",
        help="Task queue name (default: cratedb-operations)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Log level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    asyncio.run(run_worker(
        temporal_address=args.temporal_address,
        task_queue=args.task_queue,
        log_level=args.log_level
    ))