#!/usr/bin/env python3
"""
Test script to debug the cluster discovery workflow with detailed logging.
"""

import asyncio
import sys
from datetime import timedelta

from loguru import logger
from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import pydantic_data_converter

# Configure detailed logging
logger.remove()
logger.add(sys.stderr, level="DEBUG", format="{time} | {level} | {name} | {message}")

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from rr.activities import CrateDBActivities
    from rr.models import ClusterDiscoveryInput, ClusterDiscoveryResult
    from rr.workflows import ClusterDiscoveryWorkflow


async def test_discovery_with_detailed_logging():
    """Test cluster discovery with detailed error logging."""
    logger.info("Starting detailed cluster discovery test...")
    
    try:
        # Connect to Temporal with Pydantic data converter
        logger.info("Connecting to Temporal server...")
        client = await Client.connect(
            "localhost:7233",
            data_converter=pydantic_data_converter
        )
        logger.success("✓ Connected to Temporal server")
        
        # Create input
        input_data = ClusterDiscoveryInput(
            cluster_names=["aqua-darth-vader"],
            kubeconfig=None,
            context="eks1-us-east-1-dev"
        )
        logger.info(f"Input data: {input_data}")
        
        # Execute workflow with detailed timeout and retry settings
        workflow_id = "test-discovery-detailed"
        logger.info(f"Starting workflow: {workflow_id}")
        
        try:
            result = await client.execute_workflow(
                ClusterDiscoveryWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="cratedb-operations",
                execution_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=1,  # No retries for debugging
                ),
            )
            
            logger.info(f"Workflow completed successfully!")
            logger.info(f"Result type: {type(result)}")
            logger.info(f"Result: {result}")
            
            if isinstance(result, ClusterDiscoveryResult):
                logger.success("✓ Workflow returned correct type")
                logger.info(f"Total found: {result.total_found}")
                logger.info(f"Clusters: {len(result.clusters)}")
                logger.info(f"Errors: {result.errors}")
            elif isinstance(result, dict):
                logger.error("✗ Workflow returned dict instead of ClusterDiscoveryResult")
                logger.info(f"Dict keys: {result.keys()}")
                logger.info(f"Dict content: {result}")
            else:
                logger.error(f"✗ Workflow returned unexpected type: {type(result)}")
                
        except WorkflowFailureError as e:
            logger.error(f"Workflow failed: {e}")
            
            # Get detailed failure information
            if hasattr(e, 'cause') and e.cause:
                logger.error(f"Failure cause: {e.cause}")
                
            if hasattr(e, 'failure') and e.failure:
                logger.error(f"Failure details: {e.failure}")
                
                # Try to get more details from the failure
                failure = e.failure
                if hasattr(failure, 'message'):
                    logger.error(f"Failure message: {failure.message}")
                if hasattr(failure, 'stack_trace'):
                    logger.error(f"Stack trace: {failure.stack_trace}")
                if hasattr(failure, 'application_failure_info'):
                    logger.error(f"Application failure: {failure.application_failure_info}")
                if hasattr(failure, 'activity_failure_info'):
                    logger.error(f"Activity failure: {failure.activity_failure_info}")
                if hasattr(failure, 'timeout_failure_info'):
                    logger.error(f"Timeout failure: {failure.timeout_failure_info}")
                if hasattr(failure, 'canceled_failure_info'):
                    logger.error(f"Canceled failure: {failure.canceled_failure_info}")
                if hasattr(failure, 'terminated_failure_info'):
                    logger.error(f"Terminated failure: {failure.terminated_failure_info}")
                if hasattr(failure, 'server_failure_info'):
                    logger.error(f"Server failure: {failure.server_failure_info}")
                if hasattr(failure, 'reset_workflow_failure_info'):
                    logger.error(f"Reset workflow failure: {failure.reset_workflow_failure_info}")
                if hasattr(failure, 'child_workflow_execution_failure_info'):
                    logger.error(f"Child workflow failure: {failure.child_workflow_execution_failure_info}")
                    
            # Try to get workflow execution history
            try:
                handle = client.get_workflow_handle(workflow_id)
                description = await handle.describe()
                logger.error(f"Workflow status: {description.status}")
                logger.error(f"Workflow type: {description.workflow_type}")
                if hasattr(description, 'failure') and description.failure:
                    logger.error(f"Description failure: {description.failure}")
            except Exception as desc_e:
                logger.error(f"Could not get workflow description: {desc_e}")
                
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.exception("Full traceback:")
            raise
            
    except Exception as e:
        logger.error(f"Connection or setup error: {e}")
        logger.exception("Full traceback:")
        raise


async def test_activity_directly():
    """Test the activity directly without workflow."""
    logger.info("Testing activity directly...")
    
    try:
        activities = CrateDBActivities()
        
        input_data = ClusterDiscoveryInput(
            cluster_names=["aqua-darth-vader"],
            kubeconfig=None,
            context="eks1-us-east-1-dev"
        )
        
        logger.info(f"Calling activity with: {input_data}")
        result = await activities.discover_clusters(input_data)
        
        logger.info(f"Activity result type: {type(result)}")
        logger.info(f"Activity result: {result}")
        
        if isinstance(result, ClusterDiscoveryResult):
            logger.success("✓ Activity returned correct type")
            logger.info(f"Total found: {result.total_found}")
            logger.info(f"Errors: {result.errors}")
        else:
            logger.error(f"✗ Activity returned wrong type: {type(result)}")
            
    except Exception as e:
        logger.error(f"Activity test failed: {e}")
        logger.exception("Full traceback:")


async def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("CLUSTER DISCOVERY DEBUG TEST")
    logger.info("=" * 60)
    
    # Test 1: Direct activity call
    logger.info("\n" + "=" * 40)
    logger.info("TEST 1: Direct Activity Call")
    logger.info("=" * 40)
    await test_activity_directly()
    
    # Test 2: Full workflow execution
    logger.info("\n" + "=" * 40)
    logger.info("TEST 2: Full Workflow Execution")
    logger.info("=" * 40)
    await test_discovery_with_detailed_logging()
    
    logger.info("\n" + "=" * 60)
    logger.info("ALL TESTS COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())