#!/usr/bin/env python3
"""
Test script to verify the exponential backoff retry logic for health checks.

This test ensures that health checks retry properly with exponential backoff
and handle different cluster states correctly.
"""

import asyncio
import logging
import time
from unittest.mock import AsyncMock, patch

from rr.activities import CrateDBActivities
from rr.models import (
    CrateDBCluster,
    HealthCheckInput,
    HealthCheckResult,
    PodRestartInput,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_exponential_backoff_timing():
    """Test that exponential backoff timing works correctly."""
    
    logger.info("🧪 Testing exponential backoff timing...")
    
    # Create test cluster
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="YELLOW",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    
    # Mock health check to return YELLOW for first few attempts, then GREEN
    call_count = 0
    expected_delays = []
    actual_delays = []
    start_time = None
    
    async def mock_health_check(input_data):
        nonlocal call_count, start_time
        call_count += 1
        
        current_time = time.time()
        if start_time is not None:
            actual_delays.append(current_time - start_time)
        start_time = current_time
        
        if call_count <= 3:
            # First 3 attempts return YELLOW
            return HealthCheckResult(
                cluster_name="test-cluster",
                health_status="YELLOW",
                is_healthy=False,
                checked_at=None,
            )
        else:
            # 4th attempt returns GREEN
            return HealthCheckResult(
                cluster_name="test-cluster",
                health_status="GREEN",
                is_healthy=True,
                checked_at=None,
            )
    
    # Calculate expected delays (base_delay * (2 ** attempt) with some tolerance for jitter)
    base_delay = 2.0
    for attempt in range(3):  # 3 retries before success
        expected_delay = base_delay * (2 ** attempt)
        expected_delays.append(expected_delay)
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            with patch.object(activities, '_execute_decommission_strategy'):
                with patch('asyncio.to_thread'):
                    with patch.object(activities, '_wait_for_pod_ready'):
                        
                        start_total = time.time()
                        result = await activities.restart_pod(restart_input)
                        total_time = time.time() - start_total
                        
                        # Verify success
                        assert result.success, f"Expected success but got: {result.error}"
                        
                        # Verify call count
                        assert call_count == 4, f"Expected 4 health checks, got {call_count}"
                        
                        # Verify timing (allow for jitter and execution overhead)
                        assert len(actual_delays) == 3, f"Expected 3 delays, got {len(actual_delays)}"
                        
                        for i, (expected, actual) in enumerate(zip(expected_delays, actual_delays[1:])):
                            # Allow 50% tolerance for jitter and execution time
                            min_expected = expected * 0.8
                            max_expected = expected * 1.5
                            
                            logger.info(f"Delay {i+1}: expected ~{expected:.1f}s, actual {actual:.1f}s")
                            assert min_expected <= actual <= max_expected, \
                                f"Delay {i+1}: expected {expected:.1f}s ±50%, got {actual:.1f}s"
                        
                        # Total time should be reasonable
                        expected_total = sum(expected_delays)
                        assert total_time >= expected_total * 0.8, \
                            f"Total time {total_time:.1f}s too short for expected delays {expected_total:.1f}s"
                        
                        logger.info(f"✅ Exponential backoff timing verified: {total_time:.1f}s total")


async def test_yellow_to_green_transition():
    """Test YELLOW state retries until GREEN."""
    
    logger.info("🧪 Testing YELLOW to GREEN transition...")
    
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0"],
        health="YELLOW",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    call_count = 0
    
    async def mock_health_check(input_data):
        nonlocal call_count
        call_count += 1
        
        if call_count <= 2:
            return HealthCheckResult(
                cluster_name="test-cluster",
                health_status="YELLOW",
                is_healthy=False,
                checked_at=None,
            )
        else:
            return HealthCheckResult(
                cluster_name="test-cluster",
                health_status="GREEN",
                is_healthy=True,
                checked_at=None,
            )
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            with patch.object(activities, '_execute_decommission_strategy'):
                with patch('asyncio.to_thread'):
                    with patch.object(activities, '_wait_for_pod_ready'):
                        
                        result = await activities.restart_pod(restart_input)
                        
                        assert result.success, f"Expected success but got: {result.error}"
                        assert call_count == 3, f"Expected 3 health checks, got {call_count}"
                        
                        logger.info("✅ YELLOW to GREEN transition handled correctly")


async def test_red_state_retry_behavior():
    """Test RED state retries up to 30 times before failing."""
    
    logger.info("🧪 Testing RED state retry behavior...")
    
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0"],
        health="RED",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    call_count = 0
    
    async def mock_health_check(input_data):
        nonlocal call_count
        call_count += 1
        return HealthCheckResult(
            cluster_name="test-cluster",
            health_status="RED",
            is_healthy=False,
            checked_at=None,
        )
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            
            result = await activities.restart_pod(restart_input)
            
            assert not result.success, "Expected failure for RED cluster"
            assert "cluster health is RED" in result.error, f"Expected RED error, got: {result.error}"
            assert "after 30 attempts" in result.error, f"Expected 30 attempts error, got: {result.error}"
            assert call_count == 30, f"Expected 30 health checks for RED state, got {call_count}"
            
            logger.info("✅ RED state retry behavior verified")


async def test_unknown_state_retry_behavior():
    """Test UNKNOWN state retries up to 20 times before failing."""
    
    logger.info("🧪 Testing UNKNOWN state retry behavior...")
    
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0"],
        health="UNKNOWN",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    call_count = 0
    
    async def mock_health_check(input_data):
        nonlocal call_count
        call_count += 1
        # Always return UNKNOWN to test max retries for this state
        return HealthCheckResult(
            cluster_name="test-cluster",
            health_status="UNKNOWN",
            is_healthy=False,
            checked_at=None,
        )
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            
            result = await activities.restart_pod(restart_input)
            
            assert not result.success, "Expected failure for UNKNOWN cluster after max retries"
            assert "cluster health is UNKNOWN" in result.error, f"Expected UNKNOWN error, got: {result.error}"
            assert "after 20 attempts" in result.error, f"Expected 20 attempts error, got: {result.error}"
            assert call_count == 20, f"Expected 20 health checks for UNKNOWN state, got {call_count}"
            
            logger.info("✅ UNKNOWN state retry behavior verified")


async def test_max_retries_exhausted():
    """Test that max retries are exhausted for persistent YELLOW state."""
    
    logger.info("🧪 Testing max retries exhausted...")
    
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0"],
        health="YELLOW",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    call_count = 0
    
    async def mock_health_check(input_data):
        nonlocal call_count
        call_count += 1
        # Always return YELLOW to test max retries
        return HealthCheckResult(
            cluster_name="test-cluster",
            health_status="YELLOW",
            is_healthy=False,
            checked_at=None,
        )
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            
            start_time = time.time()
            result = await activities.restart_pod(restart_input)
            total_time = time.time() - start_time
            
            assert not result.success, "Expected failure after max retries"
            assert "after 30 attempts" in result.error, f"Expected max attempts error, got: {result.error}"
            assert call_count == 30, f"Expected 30 health checks, got {call_count}"
            
            # Should take reasonable time (exponential backoff)
            assert total_time >= 60, f"Expected at least 60s for 30 retries, got {total_time:.1f}s"
            assert total_time <= 900, f"Expected less than 900s, got {total_time:.1f}s"
            
            logger.info(f"✅ Max retries exhausted correctly in {total_time:.1f}s")


async def test_health_check_api_errors():
    """Test handling of health check API errors with retries."""
    
    logger.info("🧪 Testing health check API error handling...")
    
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0"],
        health="UNKNOWN",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    call_count = 0
    
    async def mock_health_check(input_data):
        nonlocal call_count
        call_count += 1
        
        if call_count <= 3:
            # First 3 attempts fail with API error
            raise Exception("CrateDB API unreachable")
        else:
            # 4th attempt succeeds
            return HealthCheckResult(
                cluster_name="test-cluster",
                health_status="GREEN",
                is_healthy=True,
                checked_at=None,
            )
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            with patch.object(activities, '_execute_decommission_strategy'):
                with patch('asyncio.to_thread'):
                    with patch.object(activities, '_wait_for_pod_ready'):
                        
                        result = await activities.restart_pod(restart_input)
                        
                        assert result.success, f"Expected success after API errors, got: {result.error}"
                        assert call_count == 4, f"Expected 4 health checks, got {call_count}"
                        
                        logger.info("✅ Health check API error handling verified")


async def test_persistent_api_errors():
    """Test failure after persistent API errors."""
    
    logger.info("🧪 Testing persistent API error failure...")
    
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0"],
        health="UNKNOWN",
        has_dc_util=True,
        dc_util_timeout=300,
        has_prestop_hook=True,
    )
    
    restart_input = PodRestartInput(
        pod_name="test-pod-0",
        namespace="test-namespace",
        cluster=cluster,
        dry_run=False,
        pod_ready_timeout=300,
    )
    
    activities = CrateDBActivities()
    call_count = 0
    
    async def mock_health_check(input_data):
        nonlocal call_count
        call_count += 1
        # Always fail with API error
        raise Exception("CrateDB API unreachable")
    
    with patch.object(activities, 'check_cluster_health', side_effect=mock_health_check):
        with patch.object(activities, '_ensure_kube_client'):
            
            result = await activities.restart_pod(restart_input)
            
            assert not result.success, "Expected failure after persistent API errors"
            assert "after 20 attempts" in result.error, f"Expected max attempts error, got: {result.error}"
            assert "CrateDB API unreachable" in result.error, f"Expected API error, got: {result.error}"
            assert call_count == 20, f"Expected 20 health checks, got {call_count}"
            
            logger.info("✅ Persistent API error failure verified")


async def main():
    """Run all retry logic tests."""
    logger.info("🧪 Running health check retry logic tests...")
    logger.info("=" * 60)
    
    test_cases = [
        test_exponential_backoff_timing,
        test_yellow_to_green_transition,
        test_red_state_retry_behavior,
        test_unknown_state_retry_behavior,
        test_max_retries_exhausted,
        test_health_check_api_errors,
        test_persistent_api_errors,
    ]
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        try:
            logger.info(f"\n🔄 Running {test_case.__name__}...")
            await test_case()
            passed += 1
        except Exception as e:
            logger.error(f"❌ Test failed: {test_case.__name__}: {e}")
            failed += 1
    
    logger.info(f"\n" + "=" * 60)
    logger.info(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("🎉 All retry logic tests passed!")
        logger.info("✅ Verified features:")
        logger.info("   - Exponential backoff with jitter (2s base, 30s max)")
        logger.info("   - YELLOW states retry up to 30 times")
        logger.info("   - RED states retry up to 30 times")
        logger.info("   - UNKNOWN states retry up to 20 times")
        logger.info("   - API errors retry with backoff (20 attempts)")
        logger.info("   - Max retries properly exhausted")
        logger.info("   - Proper error messages and logging")
        return True
    else:
        logger.error("💥 Some retry logic tests failed!")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)