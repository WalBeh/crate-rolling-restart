"""
Test worker for Temporal development server compatibility.
"""

import asyncio
import sys
from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.common import RetryPolicy
from datetime import timedelta


@activity.defn
async def hello_activity(name: str) -> str:
    """Simple test activity."""
    activity.logger.info(f"Running hello activity with name: {name}")
    return f"Hello, {name}!"


@activity.defn  
async def echo_activity(message: str) -> str:
    """Echo activity for testing."""
    activity.logger.info(f"Echo: {message}")
    return f"Echo: {message}"


@workflow.defn
class HelloWorkflow:
    """Simple test workflow compatible with temporal server start-dev."""

    @workflow.run
    async def run(self, name: str) -> str:
        """Run a simple deterministic workflow."""
        workflow.logger.info(f"Starting hello workflow for: {name}")
        
        # Use workflow.now() for deterministic time
        start_time = workflow.now()
        
        # Execute activity with proper retry policy
        result = await workflow.execute_activity(
            hello_activity,
            name,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=3,
            ),
        )
        
        # Execute another activity
        echo_result = await workflow.execute_activity(
            echo_activity,
            f"Workflow completed for {name}",
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        end_time = workflow.now()
        duration = (end_time - start_time).total_seconds()
        
        workflow.logger.info(f"Workflow completed in {duration:.2f}s")
        return f"{result} | {echo_result}"


async def main():
    """Run the test worker."""
    print("Starting test Temporal worker for development server...")
    
    try:
        # Connect to Temporal development server
        client = await Client.connect("localhost:7233")
        print("Connected to Temporal development server")
        
        # Create worker with minimal configuration
        worker = Worker(
            client,
            task_queue="test-dev-queue",
            workflows=[HelloWorkflow],
            activities=[hello_activity, echo_activity],
            max_concurrent_activities=5,
            max_concurrent_workflow_tasks=10,
        )
        
        print("Worker created successfully")
        print("Starting worker (press Ctrl+C to stop)...")
        print("You can test it with: temporal workflow start --type HelloWorkflow --task-queue test-dev-queue --input '\"World\"'")
        
        # Run worker
        await worker.run()
        
    except KeyboardInterrupt:
        print("Worker stopped by user")
    except Exception as e:
        print(f"Worker error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())