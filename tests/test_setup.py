#!/usr/bin/env python3
"""
Test script to verify Temporal setup works with temporal server start-dev.
"""

import asyncio
import sys
from datetime import timedelta

from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker


@activity.defn
async def simple_test_activity(message: str) -> str:
    """Simple test activity."""
    activity.logger.info(f"Test activity called with: {message}")
    return f"Activity processed: {message}"


@workflow.defn
class TestWorkflow:
    """Simple test workflow."""

    @workflow.run
    async def run(self, input_message: str) -> str:
        """Run test workflow."""
        workflow.logger.info(f"Test workflow started with: {input_message}")
        
        # Use workflow.now() for deterministic time
        start_time = workflow.now()
        
        # Execute activity
        result = await workflow.execute_activity(
            simple_test_activity,
            input_message,
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        end_time = workflow.now()
        duration = (end_time - start_time).total_seconds()
        
        workflow.logger.info(f"Test workflow completed in {duration:.2f}s")
        return f"Workflow result: {result}"


async def check_connection():
    """Test connection to Temporal server."""
    print("Testing connection to Temporal development server...")
    
    try:
        client = await Client.connect("localhost:7233")
        print("✓ Successfully connected to Temporal at localhost:7233")
        return client
    except Exception as e:
        print(f"✗ Failed to connect to Temporal: {e}")
        print("Make sure 'temporal server start-dev' is running")
        return None


async def check_workflow_execution(client):
    """Test workflow execution."""
    print("Testing workflow execution...")
    
    try:
        # Start workflow
        handle = await client.start_workflow(
            TestWorkflow.run,
            "Hello from test setup!",
            id="test-setup-workflow",
            task_queue="test-setup-queue",
        )
        
        print(f"✓ Started workflow with ID: {handle.id}")
        
        # Wait for result (with timeout)
        try:
            result = await asyncio.wait_for(handle.result(), timeout=10.0)
            print(f"✓ Workflow completed successfully: {result}")
            return True
        except asyncio.TimeoutError:
            print("✗ Workflow execution timed out (worker might not be running)")
            return False
            
    except Exception as e:
        print(f"✗ Failed to execute workflow: {e}")
        return False


async def run_worker(client, duration=5):
    """Run test worker for a short duration."""
    print(f"Running test worker for {duration} seconds...")
    
    try:
        worker = Worker(
            client,
            task_queue="test-setup-queue",
            workflows=[TestWorkflow],
            activities=[simple_test_activity],
        )
        
        print("✓ Worker created successfully")
        
        # Run worker for limited time
        worker_task = asyncio.create_task(worker.run())
        
        try:
            await asyncio.wait_for(worker_task, timeout=duration)
        except asyncio.TimeoutError:
            print("✓ Worker ran successfully for the specified duration")
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        
        return True
        
    except Exception as e:
        print(f"✗ Worker failed: {e}")
        return False


async def check_imports():
    """Test importing the main modules."""
    print("Testing module imports...")
    
    try:
        from rr.workflows import ClusterRestartWorkflow
        print("✓ Successfully imported ClusterRestartWorkflow")
    except Exception as e:
        print(f"✗ Failed to import ClusterRestartWorkflow: {e}")
        return False
    
    try:
        from rr.activities import CrateDBActivities
        print("✓ Successfully imported CrateDBActivities")
    except Exception as e:
        print(f"✗ Failed to import CrateDBActivities: {e}")
        return False
    
    try:
        from rr.worker import WorkerManager
        print("✓ Successfully imported WorkerManager")
    except Exception as e:
        print(f"✗ Failed to import WorkerManager: {e}")
        return False
    
    return True


async def main():
    """Main test function."""
    print("=== CrateDB Temporal Setup Test ===\n")
    
    # Test 1: Module imports
    print("1. Testing module imports...")
    if not await check_imports():
        print("✗ Import test failed")
        sys.exit(1)
    print("✓ All imports successful\n")
    
    # Test 2: Temporal connection
    print("2. Testing Temporal connection...")
    client = await check_connection()
    if not client:
        sys.exit(1)
    print("✓ Connection test successful\n")
    
    # Test 3: Worker functionality
    print("3. Testing worker functionality...")
    worker_success = await run_worker(client, duration=3)
    if not worker_success:
        print("✗ Worker test failed")
        sys.exit(1)
    print("✓ Worker test successful\n")
    
    # Test 4: Workflow execution
    print("4. Testing workflow execution...")
    
    # Start worker in background
    worker = Worker(
        client,
        task_queue="test-setup-queue",
        workflows=[TestWorkflow],
        activities=[simple_test_activity],
    )
    
    worker_task = asyncio.create_task(worker.run())
    
    # Give worker time to start
    await asyncio.sleep(1)
    
    # Execute workflow
    workflow_success = await check_workflow_execution(client)
    
    # Stop worker
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    
    if not workflow_success:
        print("✗ Workflow execution test failed")
        sys.exit(1)
    
    print("✓ Workflow execution test successful\n")
    
    print("=== All tests passed! ===")
    print("Your Temporal setup is working correctly with temporal server start-dev")
    print("\nNext steps:")
    print("1. Start the main worker: uv run python -m rr.worker")
    print("2. Test the CLI: uv run rr --help")
    print("3. View Temporal UI at: http://localhost:8233")


if __name__ == "__main__":
    asyncio.run(main())