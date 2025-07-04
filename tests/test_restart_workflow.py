#!/usr/bin/env python3
"""
Test script to debug restart workflow issue by testing each component separately.
"""

import asyncio
import sys
from datetime import timedelta

from loguru import logger
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import pydantic_data_converter

# Configure logging
logger.remove()
logger.add(sys.stderr, level="DEBUG", format="{time} | {level} | {name} | {message}")

# Use unsafe imports for temporal server start-dev compatibility
with workflow.unsafe.imports_passed_through():
    from rr.activities import CrateDBActivities
    from rr.models import (
        ClusterDiscoveryInput,
        ClusterDiscoveryResult,
        MultiClusterRestartInput,
        RestartOptions,
        CrateDBCluster,
    )
    from rr.workflows import MultiClusterRestartWorkflow


async def test_activity_discovery():
    """Test cluster discovery activity directly."""
    logger.info("=" * 60)
    logger.info("TEST 1: Direct Activity Discovery")
    logger.info("=" * 60)
    
    activities = CrateDBActivities()
    
    input_data = ClusterDiscoveryInput(
        cluster_names=["aqua-darth-vader"],
        kubeconfig=None,
        context="eks1-us-east-1-dev"
    )
    
    try:
        result = await activities.discover_clusters(input_data)
        logger.info(f"✓ Direct activity result type: {type(result)}")
        logger.info(f"✓ Found {result.total_found} clusters")
        logger.info(f"✓ Cluster names: {[c.name for c in result.clusters]}")
        
        if result.clusters:
            cluster = result.clusters[0]
            logger.info(f"✓ First cluster details:")
            logger.info(f"  - Name: {cluster.name}")
            logger.info(f"  - Namespace: {cluster.namespace}")
            logger.info(f"  - Type: {type(cluster)}")
            logger.info(f"  - Has prestop hook: {cluster.has_prestop_hook}")
            logger.info(f"  - Has dc util: {cluster.has_dc_util}")
            return cluster  # Return for use in next test
            
    except Exception as e:
        logger.error(f"✗ Direct activity test failed: {e}")
        logger.exception("Full traceback:")
    
    return None


async def test_workflow_discovery():
    """Test cluster discovery via workflow."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Workflow Discovery")
    logger.info("=" * 60)
    
    try:
        client = await Client.connect(
            "localhost:7233",
            data_converter=pydantic_data_converter
        )
        logger.info("✓ Connected to Temporal server")
        
        input_data = ClusterDiscoveryInput(
            cluster_names=["aqua-darth-vader"],
            kubeconfig=None,
            context="eks1-us-east-1-dev"
        )
        
        from rr.workflows import ClusterDiscoveryWorkflow
        
        result = await client.execute_workflow(
            ClusterDiscoveryWorkflow.run,
            input_data,
            id="test-discovery-workflow",
            task_queue="cratedb-operations",
            execution_timeout=timedelta(minutes=1),
        )
        
        logger.info(f"✓ Workflow result type: {type(result)}")
        
        if isinstance(result, dict):
            logger.warning("⚠ Workflow returned dict, converting...")
            from rr.models import ClusterDiscoveryResult, CrateDBCluster
            
            clusters = []
            if 'clusters' in result and isinstance(result['clusters'], list):
                for cluster_data in result['clusters']:
                    if isinstance(cluster_data, dict):
                        clusters.append(CrateDBCluster(**cluster_data))
                    else:
                        clusters.append(cluster_data)
            
            result = ClusterDiscoveryResult(
                clusters=clusters,
                total_found=result.get('total_found', len(clusters)),
                errors=result.get('errors', [])
            )
            logger.info("✓ Converted to ClusterDiscoveryResult")
        
        logger.info(f"✓ Found {result.total_found} clusters via workflow")
        logger.info(f"✓ Cluster names: {[c.name for c in result.clusters]}")
        
        if result.clusters:
            return result.clusters[0]  # Return for use in next test
            
    except Exception as e:
        logger.error(f"✗ Workflow discovery test failed: {e}")
        logger.exception("Full traceback:")
    
    return None


async def test_restart_workflow():
    """Test the full restart workflow."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Full Restart Workflow")
    logger.info("=" * 60)
    
    try:
        client = await Client.connect(
            "localhost:7233",
            data_converter=pydantic_data_converter
        )
        logger.info("✓ Connected to Temporal server")
        
        # Test with dry run first
        options = RestartOptions(
            kubeconfig=None,
            context="eks1-us-east-1-dev",
            dry_run=True,
            skip_hook_warning=True,
            output_format="text",
            log_level="DEBUG"
        )
        
        input_data = MultiClusterRestartInput(
            cluster_names=["aqua-darth-vader"],
            options=options
        )
        
        logger.info(f"Input data: {input_data}")
        logger.info(f"Cluster names: {input_data.cluster_names}")
        logger.info(f"Options: {input_data.options}")
        
        result = await client.execute_workflow(
            MultiClusterRestartWorkflow.run,
            input_data,
            id="test-restart-workflow",
            task_queue="cratedb-operations",
            execution_timeout=timedelta(minutes=2),
        )
        
        logger.info(f"✓ Restart workflow result type: {type(result)}")
        
        if isinstance(result, dict):
            logger.info(f"Raw dict result: {result}")
            logger.warning("⚠ Restart workflow returned dict, converting...")
            from rr.models import MultiClusterRestartResult, RestartResult
            
            restart_results = []
            if 'results' in result and isinstance(result['results'], list):
                for result_data in result['results']:
                    if isinstance(result_data, dict):
                        restart_results.append(RestartResult(**result_data))
                    else:
                        restart_results.append(result_data)
            
            from rr.models import MultiClusterRestartResult
            result = MultiClusterRestartResult(
                results=restart_results,
                total_clusters=result.get('total_clusters', len(restart_results)),
                successful_clusters=result.get('successful_clusters', 0),
                failed_clusters=result.get('failed_clusters', 0),
                total_duration=result.get('total_duration', 0.0),
                started_at=result.get('started_at'),
                completed_at=result.get('completed_at')
            )
            logger.info("✓ Converted to MultiClusterRestartResult")
        
        logger.info(f"✓ Restart workflow completed:")
        logger.info(f"  - Total clusters: {result.total_clusters}")
        logger.info(f"  - Successful: {result.successful_clusters}")
        logger.info(f"  - Failed: {result.failed_clusters}")
        logger.info(f"  - Duration: {result.total_duration}s")
        logger.info(f"  - Results count: {len(result.results)}")
        
        if result.results:
            for i, res in enumerate(result.results):
                logger.info(f"  - Result {i+1}: cluster={res.cluster.name if hasattr(res.cluster, 'name') else 'unknown'}, success={res.success}, error={res.error}")
        
        if result.total_clusters == 0:
            logger.error("✗ ISSUE FOUND: total_clusters is 0, but we passed 1 cluster!")
            logger.error("This suggests the cluster discovery within the restart workflow is failing")
        else:
            logger.success("✓ Restart workflow found the correct number of clusters")
            
    except Exception as e:
        logger.error(f"✗ Restart workflow test failed: {e}")
        logger.exception("Full traceback:")


async def test_restart_workflow_with_cluster_object():
    """Test restart workflow by passing cluster object directly."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: Restart Workflow with Cluster Object")
    logger.info("=" * 60)
    
    # First get the cluster object
    cluster = await test_activity_discovery()
    if not cluster:
        logger.error("✗ Cannot test without cluster object")
        return
    
    try:
        client = await Client.connect(
            "localhost:7233",
            data_converter=pydantic_data_converter
        )
        
        options = RestartOptions(
            kubeconfig=None,
            context="eks1-us-east-1-dev",
            dry_run=True,
            skip_hook_warning=True,
            output_format="text",
            log_level="DEBUG"
        )
        
        from rr.workflows import ClusterRestartWorkflow
        
        logger.info(f"Testing single cluster restart with: {cluster.name}")
        logger.info(f"Cluster type: {type(cluster)}")
        
        result = await client.execute_workflow(
            ClusterRestartWorkflow.run,
            args=[cluster, options],
            id="test-single-cluster-restart",
            task_queue="cratedb-operations",
            execution_timeout=timedelta(minutes=2),
        )
        
        logger.info(f"✓ Single cluster restart result type: {type(result)}")
        
        if isinstance(result, dict):
            logger.info(f"Raw dict result: {result}")
            logger.warning("⚠ Single restart workflow returned dict, converting...")
            from rr.models import RestartResult
            result = RestartResult(**result)
            logger.info("✓ Converted to RestartResult")
        
        logger.info(f"✓ Single cluster restart completed:")
        logger.info(f"  - Cluster: {result.cluster.name if hasattr(result.cluster, 'name') else 'unknown'}")
        logger.info(f"  - Success: {result.success}")
        logger.info(f"  - Duration: {result.duration}s")
        logger.info(f"  - Pods restarted: {len(result.restarted_pods)}/{result.total_pods}")
        logger.info(f"  - Error: {result.error}")
        
    except Exception as e:
        logger.error(f"✗ Single cluster restart test failed: {e}")
        logger.exception("Full traceback:")


async def main():
    """Run all tests."""
    logger.info("Starting comprehensive restart workflow debugging...")
    
    # Test 1: Direct activity
    await test_activity_discovery()
    
    # Test 2: Workflow discovery
    await test_workflow_discovery()
    
    # Test 3: Full restart workflow
    await test_restart_workflow()
    
    # Test 4: Single cluster restart
    await test_restart_workflow_with_cluster_object()
    
    logger.info("\n" + "=" * 60)
    logger.info("ALL TESTS COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())