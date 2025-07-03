#!/usr/bin/env python3
"""
Test script to verify the health validation fix for pod restart regression.

This test ensures that pods are not deleted when the cluster is not GREEN.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from rr.activities import CrateDBActivities
from rr.models import (
    CrateDBCluster,
    HealthCheckInput,
    HealthCheckResult,
    PodRestartInput,
    PodRestartResult,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_pod_restart_blocks_on_non_green_cluster():
    """Test that pod restart fails when cluster health is not GREEN."""
    
    # Create test cluster
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="YELLOW",  # Not GREEN
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    # Create pod restart input
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    # Mock the activities
    activities = CrateDBActivities()
    
    # Mock health check to return YELLOW (not GREEN)
    yellow_health_result = HealthCheckResult(
        cluster_name="test-cluster",
        health_status="YELLOW",
        is_healthy=False,
        checked_at=None,
    )
    
    with patch.object(activities, 'check_cluster_health', return_value=yellow_health_result) as mock_health_check:
        with patch.object(activities, '_ensure_kube_client'):
            # Test should fail because cluster is not GREEN
            result = await activities.restart_pod(restart_input)
            
            # Verify health check was called
            mock_health_check.assert_called_once()
            
            # Verify restart failed due to health check
            assert not result.success
            assert "cluster health is YELLOW, must be GREEN" in result.error
            
            logger.info("✅ Test passed: Pod restart correctly blocked on YELLOW cluster")


async def test_pod_restart_proceeds_on_green_cluster():
    """Test that pod restart proceeds when cluster health is GREEN."""
    
    # Create test cluster
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="GREEN",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    # Create pod restart input
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    # Mock the activities
    activities = CrateDBActivities()
    
    # Mock health check to return GREEN
    green_health_result = HealthCheckResult(
        cluster_name="test-cluster",
        health_status="GREEN",
        is_healthy=True,
        checked_at=None,
    )
    
    with patch.object(activities, 'check_cluster_health', return_value=green_health_result) as mock_health_check:
        with patch.object(activities, '_ensure_kube_client'):
            with patch.object(activities, '_execute_decommission_strategy', return_value=None) as mock_decommission:
                with patch('asyncio.to_thread') as mock_to_thread:
                    with patch.object(activities, '_wait_for_pod_ready', return_value=None) as mock_wait:
                        
                        # Test should succeed because cluster is GREEN
                        result = await activities.restart_pod(restart_input)
                        
                        # Verify health check was called
                        mock_health_check.assert_called_once()
                        
                        # Verify decommission strategy was called (after health check passed)
                        mock_decommission.assert_called_once()
                        
                        # Verify pod deletion was attempted
                        mock_to_thread.assert_called_once()
                        
                        # Verify restart succeeded
                        assert result.success
                        
                        logger.info("✅ Test passed: Pod restart correctly proceeded on GREEN cluster")


async def test_pod_restart_blocks_on_red_cluster():
    """Test that pod restart fails when cluster health is RED."""
    
    # Create test cluster
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="RED",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    # Create pod restart input
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    # Mock the activities
    activities = CrateDBActivities()
    
    # Mock health check to return RED (unhealthy)
    red_health_result = HealthCheckResult(
        cluster_name="test-cluster",
        health_status="RED",
        is_healthy=False,
        checked_at=None,
    )
    
    with patch.object(activities, 'check_cluster_health', return_value=red_health_result) as mock_health_check:
        with patch.object(activities, '_ensure_kube_client'):
            # Test should fail because cluster is RED
            result = await activities.restart_pod(restart_input)
            
            # Verify health check was called
            mock_health_check.assert_called_once()
            
            # Verify restart failed due to health check
            assert not result.success
            assert "cluster health is RED, must be GREEN" in result.error
            
            logger.info("✅ Test passed: Pod restart correctly blocked on RED cluster")


async def test_pod_restart_blocks_on_health_check_exception():
    """Test that pod restart fails when health check throws exception."""
    
    # Create test cluster
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="UNKNOWN",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    # Create pod restart input
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    # Mock the activities
    activities = CrateDBActivities()
    
    # Mock health check to throw exception
    with patch.object(activities, 'check_cluster_health', side_effect=Exception("CrateDB API unreachable")) as mock_health_check:
        with patch.object(activities, '_ensure_kube_client'):
            # Test should fail because health check failed
            result = await activities.restart_pod(restart_input)
            
            # Verify health check was called
            mock_health_check.assert_called_once()
            
            # Verify restart failed due to health check exception
            assert not result.success
            assert "Health check failed before restarting pod" in result.error
            assert "CrateDB API unreachable" in result.error
            
            logger.info("✅ Test passed: Pod restart correctly blocked on health check exception")


async def test_pod_restart_works_for_manual_decommission_clusters():
    """Test that pod restart with health validation works for non-dc_util clusters."""
    
    # Create test cluster without dc_util
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="GREEN",
        has_dc_util=False,  # No dc_util - manual decommission
        dc_util_timeout=0,
        has_prestop_hook=False,
    )
    
    # Create pod restart input
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    # Mock the activities
    activities = CrateDBActivities()
    
    # Mock health check to return GREEN
    green_health_result = HealthCheckResult(
        cluster_name="test-cluster",
        health_status="GREEN",
        is_healthy=True,
        checked_at=None,
    )
    
    with patch.object(activities, 'check_cluster_health', return_value=green_health_result) as mock_health_check:
        with patch.object(activities, '_ensure_kube_client'):
            with patch.object(activities, '_execute_manual_decommission', return_value=None) as mock_manual_decommission:
                with patch('asyncio.to_thread') as mock_to_thread:
                    with patch.object(activities, '_wait_for_pod_ready', return_value=None) as mock_wait:
                        
                        # Test should succeed for manual decommission clusters too
                        result = await activities.restart_pod(restart_input)
                        
                        # Verify health check was called first
                        mock_health_check.assert_called_once()
                        
                        # Verify manual decommission was called (after health check passed)
                        mock_manual_decommission.assert_called_once()
                        
                        # Verify pod deletion was attempted
                        mock_to_thread.assert_called_once()
                        
                        # Verify restart succeeded
                        assert result.success
                        
                        logger.info("✅ Test passed: Pod restart with health validation works for manual decommission clusters")


async def main():
    """Run all tests."""
    logger.info("🧪 Running health validation fix tests...")
    
    # Test cases
    test_cases = [
        test_pod_restart_blocks_on_non_green_cluster,
        test_pod_restart_proceeds_on_green_cluster,
        test_pod_restart_blocks_on_red_cluster,
        test_pod_restart_blocks_on_health_check_exception,
        test_pod_restart_works_for_manual_decommission_clusters,
    ]
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        try:
            logger.info(f"Running {test_case.__name__}...")
            await test_case()
            passed += 1
        except Exception as e:
            logger.error(f"❌ Test failed: {test_case.__name__}: {e}")
            failed += 1
    
    logger.info(f"\n📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("🎉 All tests passed! Health validation fix is working correctly.")
    else:
        logger.error("💥 Some tests failed! Please review the implementation.")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())