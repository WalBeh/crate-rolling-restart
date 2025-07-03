"""
Example usage of the CrateDB decommission activity with smart strategy selection.

This example demonstrates how to use the new decommission activity that automatically
detects whether a cluster has Kubernetes-managed decommission (preStop hook with dc_util)
or requires manual decommission.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from temporalio.client import Client
from temporalio.worker import Worker

from rr.activities import CrateDBActivities
from rr.models import (
    CrateDBCluster,
    DecommissionInput,
    RestartOptions,
    PodRestartInput
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def example_decommission_only():
    """
    Example: Decommission a pod without restart.
    
    This is useful when you want to gracefully remove a pod from the cluster
    without restarting it (e.g., for cluster downsizing).
    """
    
    # Example cluster with Kubernetes-managed decommission (has dc_util in preStop hook)
    cluster_with_dc_util = CrateDBCluster(
        name="cluster1",
        namespace="crate-system",
        statefulset_name="crate-data-hot-cluster1",
        health="GREEN",
        replicas=3,
        pods=["crate-data-hot-cluster1-0", "crate-data-hot-cluster1-1", "crate-data-hot-cluster1-2"],
        has_prestop_hook=True,
        has_dc_util=True,
        dc_util_timeout=720,
        crd_name="cluster1"
    )
    
    # Connect to Temporal
    client = await Client.connect("localhost:7233")
    
    # Create decommission input
    decommission_input = DecommissionInput(
        pod_name="crate-data-hot-cluster1-2",  # Decommission the last pod
        namespace="crate-system",
        cluster=cluster_with_dc_util,
        dry_run=False
    )
    
    # Execute decommission activity
    activities = CrateDBActivities()
    result = await activities.decommission_pod(decommission_input)
    
    if result.success:
        logger.info(f"✅ Decommission successful!")
        logger.info(f"   Strategy used: {result.strategy_used}")
        logger.info(f"   Duration: {result.duration:.1f}s")
        logger.info(f"   Timeout configured: {result.decommission_timeout}s")
    else:
        logger.error(f"❌ Decommission failed: {result.error}")


async def example_decommission_with_restart():
    """
    Example: Decommission and restart a pod.
    
    This shows how decommission is integrated into the normal pod restart flow.
    """
    
    # Example cluster requiring manual decommission (no dc_util in preStop hook)
    cluster_manual = CrateDBCluster(
        name="cluster2",
        namespace="default",
        statefulset_name="crate-data-hot-cluster2", 
        health="GREEN",
        replicas=3,
        pods=["crate-data-hot-cluster2-0", "crate-data-hot-cluster2-1", "crate-data-hot-cluster2-2"],
        has_prestop_hook=False,
        has_dc_util=False,
        dc_util_timeout=720,
        crd_name="cluster2"
    )
    
    # Create restart input (includes decommission)
    restart_input = PodRestartInput(
        pod_name="crate-data-hot-cluster2-0",
        namespace="default",
        cluster=cluster_manual,
        dry_run=False,
        pod_ready_timeout=600
    )
    
    # Execute restart (which includes smart decommission)
    activities = CrateDBActivities()
    result = await activities.restart_pod(restart_input)
    
    if result.success:
        logger.info(f"✅ Pod restart successful!")
        logger.info(f"   Duration: {result.duration:.1f}s")
        logger.info(f"   Pod was decommissioned and restarted")
    else:
        logger.error(f"❌ Pod restart failed: {result.error}")


async def example_compare_strategies():
    """
    Example: Compare both decommission strategies side by side.
    """
    
    clusters = [
        # Kubernetes-managed decommission
        CrateDBCluster(
            name="k8s-managed",
            namespace="crate-system", 
            statefulset_name="crate-data-hot-k8s-managed",
            health="GREEN",
            replicas=3,
            pods=["crate-data-hot-k8s-managed-0"],
            has_prestop_hook=True,
            has_dc_util=True,
            dc_util_timeout=600,
            crd_name="k8s-managed"
        ),
        # Manual decommission
        CrateDBCluster(
            name="manual",
            namespace="default",
            statefulset_name="crate-data-hot-manual",
            health="GREEN", 
            replicas=3,
            pods=["crate-data-hot-manual-0"],
            has_prestop_hook=False,
            has_dc_util=False,
            dc_util_timeout=720,
            crd_name="manual"
        )
    ]
    
    activities = CrateDBActivities()
    
    for cluster in clusters:
        logger.info(f"\n=== Testing {cluster.name} cluster ===")
        logger.info(f"Has preStop hook: {cluster.has_prestop_hook}")
        logger.info(f"Has dc_util: {cluster.has_dc_util}")
        logger.info(f"Timeout: {cluster.dc_util_timeout}s")
        
        decommission_input = DecommissionInput(
            pod_name=cluster.pods[0],
            namespace=cluster.namespace,
            cluster=cluster,
            dry_run=True  # Dry run for demonstration
        )
        
        result = await activities.decommission_pod(decommission_input)
        
        logger.info(f"Strategy selected: {result.strategy_used}")
        logger.info(f"Would succeed: {result.success}")


async def example_error_handling():
    """
    Example: Proper error handling for decommission operations.
    """
    
    # Cluster with invalid configuration (for demonstration)
    invalid_cluster = CrateDBCluster(
        name="invalid",
        namespace="nonexistent",
        statefulset_name="does-not-exist",
        health="RED",
        replicas=0,
        pods=["nonexistent-pod-0"],
        has_prestop_hook=False,
        has_dc_util=False,
        dc_util_timeout=720,
        crd_name="invalid"
    )
    
    decommission_input = DecommissionInput(
        pod_name="nonexistent-pod-0",
        namespace="nonexistent", 
        cluster=invalid_cluster,
        dry_run=False
    )
    
    activities = CrateDBActivities()
    
    try:
        result = await activities.decommission_pod(decommission_input)
        
        if not result.success:
            logger.error(f"Decommission failed as expected: {result.error}")
            logger.info(f"Duration before failure: {result.duration:.1f}s")
            logger.info(f"Strategy attempted: {result.strategy_used}")
            
            # Handle the error appropriately
            if "not found" in result.error.lower():
                logger.info("Pod or namespace does not exist")
            elif "timeout" in result.error.lower():
                logger.info("Operation timed out - cluster may be unhealthy")
            else:
                logger.info("Unknown error - requires investigation")
                
    except Exception as e:
        logger.error(f"Unexpected exception during decommission: {e}")


async def example_batch_decommission():
    """
    Example: Decommission multiple pods in sequence.
    
    This shows how to gracefully decommission multiple pods while maintaining
    cluster health and data availability.
    """
    
    cluster = CrateDBCluster(
        name="batch-test",
        namespace="default",
        statefulset_name="crate-data-hot-batch-test",
        health="GREEN",
        replicas=5,
        pods=[
            "crate-data-hot-batch-test-0",
            "crate-data-hot-batch-test-1", 
            "crate-data-hot-batch-test-2",
            "crate-data-hot-batch-test-3",
            "crate-data-hot-batch-test-4"
        ],
        has_prestop_hook=True,
        has_dc_util=True,
        dc_util_timeout=900,  # Longer timeout for larger cluster
        crd_name="batch-test"
    )
    
    activities = CrateDBActivities()
    
    # Decommission pods one by one (last pod first to maintain availability)
    pods_to_decommission = cluster.pods[-2:]  # Decommission last 2 pods
    
    logger.info(f"Starting batch decommission of {len(pods_to_decommission)} pods")
    
    successful_decommissions = []
    failed_decommissions = []
    
    for pod_name in pods_to_decommission:
        logger.info(f"\n--- Decommissioning {pod_name} ---")
        
        decommission_input = DecommissionInput(
            pod_name=pod_name,
            namespace=cluster.namespace,
            cluster=cluster,
            dry_run=False
        )
        
        try:
            result = await activities.decommission_pod(decommission_input)
            
            if result.success:
                successful_decommissions.append(pod_name)
                logger.info(f"✅ {pod_name} decommissioned successfully in {result.duration:.1f}s")
                
                # Wait a bit before decommissioning next pod to allow cluster to stabilize
                logger.info("Waiting 30s for cluster to stabilize...")
                await asyncio.sleep(30)
                
            else:
                failed_decommissions.append(pod_name)
                logger.error(f"❌ {pod_name} decommission failed: {result.error}")
                break  # Stop on first failure to avoid cascading issues
                
        except Exception as e:
            failed_decommissions.append(pod_name)
            logger.error(f"❌ {pod_name} decommission exception: {e}")
            break
    
    # Summary
    logger.info(f"\n=== Batch Decommission Summary ===")
    logger.info(f"Successful: {len(successful_decommissions)} pods")
    logger.info(f"Failed: {len(failed_decommissions)} pods")
    
    if successful_decommissions:
        logger.info(f"Successfully decommissioned: {', '.join(successful_decommissions)}")
    
    if failed_decommissions:
        logger.error(f"Failed to decommission: {', '.join(failed_decommissions)}")


async def main():
    """Run all examples."""
    
    logger.info("🚀 CrateDB Decommission Examples")
    logger.info("=" * 50)
    
    try:
        logger.info("\n1. Testing decommission strategy comparison...")
        await example_compare_strategies()
        
        logger.info("\n2. Testing error handling...")
        await example_error_handling()
        
        # Uncomment these for actual cluster operations (requires running clusters)
        # logger.info("\n3. Testing decommission only...")
        # await example_decommission_only()
        # 
        # logger.info("\n4. Testing decommission with restart...")
        # await example_decommission_with_restart()
        #
        # logger.info("\n5. Testing batch decommission...")  
        # await example_batch_decommission()
        
        logger.info("\n✅ All examples completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())