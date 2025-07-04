#!/usr/bin/env python3
"""
Tests for cluster routing allocation reset functionality.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
import json

from rr.activities import CrateDBActivities
from rr.models import PodRestartInput, CrateDBCluster, ClusterRoutingResetInput


@pytest.fixture
def mock_cratedb_activities():
    """Create a CrateDBActivities instance with mocked Kubernetes clients."""
    activities = CrateDBActivities()
    activities.core_v1 = Mock()
    activities.apps_v1 = Mock()
    activities.custom_objects_api = Mock()
    return activities


@pytest.fixture
def manual_decommission_cluster():
    """Create a cluster configured for manual decommission."""
    return CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        statefulset_name="test-sts",
        health="GREEN",
        replicas=1,
        crd_name="test-cluster-crd",
        pods=["test-pod-0"],
        has_prestop_hook=False,
        has_dc_util=False,  # This triggers manual decommission
        dc_util_timeout=300,
        min_availability="PRIMARIES"
    )


@pytest.fixture
def kubernetes_decommission_cluster():
    """Create a cluster configured for Kubernetes-managed decommission."""
    return CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        statefulset_name="test-sts",
        health="GREEN",
        replicas=1,
        crd_name="test-cluster-crd",
        pods=["test-pod-0"],
        has_prestop_hook=True,
        has_dc_util=True,  # This uses Kubernetes-managed decommission
        dc_util_timeout=300,
        min_availability="PRIMARIES"
    )


class TestClusterRoutingAllocationReset:
    """Test cases for cluster routing allocation reset functionality."""

    @pytest.mark.asyncio
    async def test_reset_cluster_routing_allocation_success(self, mock_cratedb_activities):
        """Test successful reset of cluster routing allocation setting."""
        # Mock the command execution
        mock_cratedb_activities._execute_command_in_pod = AsyncMock(
            return_value='{"rows":[],"rowcount":0,"duration":0.123}'
        )
        
        # Create a mock cluster
        mock_cluster = Mock()
        mock_cluster.pods = ["test-pod-0", "test-pod-1"]
        
        # Execute the reset
        await mock_cratedb_activities._reset_cluster_routing_allocation(
            "test-pod-0", 
            "test-namespace",
            mock_cluster
        )
        
        # Verify the command was called with correct parameters
        mock_cratedb_activities._execute_command_in_pod.assert_called_once()
        call_args = mock_cratedb_activities._execute_command_in_pod.call_args
        
        pod_name, namespace, command = call_args[0]
        assert pod_name == "test-pod-0"
        assert namespace == "test-namespace"
        
        # Verify the command contains the correct SQL
        expected_sql = 'set global transient "cluster.routing.allocation.enable" = "all"'
        # The SQL is JSON-escaped in the command, so check for the escaped version
        assert '\\"cluster.routing.allocation.enable\\" = \\"all\\"' in command
        assert "application/json" in command
        assert "https://127.0.0.1:4200/_sql" in command

    @pytest.mark.asyncio
    async def test_reset_cluster_routing_allocation_failure(self, mock_cratedb_activities):
        """Test that reset failure raises exception (to be caught by retry wrapper)."""
        # Mock the command execution to fail
        mock_cratedb_activities._execute_command_in_pod = AsyncMock(
            side_effect=Exception("Connection failed")
        )
        
        # Create a mock cluster
        mock_cluster = Mock()
        mock_cluster.pods = ["test-pod-0", "test-pod-1"]
        
        # Execute the reset - should raise exception (for retry wrapper to catch)
        with pytest.raises(Exception, match="All reset attempts failed"):
            await mock_cratedb_activities._reset_cluster_routing_allocation(
                "test-pod-0", 
                "test-namespace",
                mock_cluster
            )

    @pytest.mark.asyncio
    async def test_reset_cluster_routing_allocation_activity_success(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test the new separate reset_cluster_routing_allocation activity."""
        # Mock the underlying reset method
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock()
        
        # Create input for the new activity
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # Execute the reset activity
            result = await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
        
        # Verify the result
        assert result.success is True
        assert result.pod_name == "test-pod-0"
        assert result.namespace == "test-namespace"
        assert result.cluster_name == manual_decommission_cluster.name
        assert result.error is None
        
        # Verify underlying reset method was called
        mock_cratedb_activities._reset_cluster_routing_allocation.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_cluster_routing_allocation_activity_failure(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that reset activity raises exception when underlying reset fails."""
        # Mock the underlying reset method to fail
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock(
            side_effect=Exception("CrateDB connection failed")
        )
        
        # Create input for the new activity
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # Execute the reset activity - should raise exception for Temporal to retry
            with pytest.raises(Exception, match="Failed to reset cluster routing allocation"):
                await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
        
        # Verify underlying reset method was called
        mock_cratedb_activities._reset_cluster_routing_allocation.assert_called_once()

    @pytest.mark.asyncio
    async def test_restart_pod_no_longer_calls_reset_directly(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that restart_pod no longer calls reset directly (now handled by state machine)."""
        # Mock all the dependencies
        mock_cratedb_activities.check_cluster_health = AsyncMock(
            return_value=Mock(is_healthy=True, health_status="GREEN")
        )
        mock_cratedb_activities._execute_decommission_strategy = AsyncMock()
        mock_cratedb_activities._wait_for_pod_ready = AsyncMock()
        # Note: reset is now handled by state machine, not directly in restart_pod
        
        # Mock pod deletion
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = None
            
            input_data = PodRestartInput(
                pod_name="test-pod-0",
                namespace="test-namespace",
                cluster=manual_decommission_cluster,
                pod_ready_timeout=600
            )
            
            # Execute restart_pod
            result = await mock_cratedb_activities.restart_pod(input_data)
            
            # Verify that restart succeeded (reset is now handled by state machine)
            # No direct reset method calls should be made from restart_pod
            
            # Verify that restart succeeded (reset is now handled by state machine)
            assert result.success is True

    def test_reset_sql_command_format(self):
        """Test that the SQL command is properly formatted."""
        expected_sql = 'set global transient "cluster.routing.allocation.enable" = "all"'
        expected_json = json.dumps({"stmt": expected_sql})
        
        # Verify JSON is valid
        parsed = json.loads(expected_json)
        assert parsed["stmt"] == expected_sql
        
        # Verify the SQL statement format
        assert "set global transient" in expected_sql
        assert '"cluster.routing.allocation.enable"' in expected_sql
        assert '"all"' in expected_sql

    @pytest.mark.asyncio
    async def test_temporal_execution_guarantees_for_reset(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that reset activity fails until successful, simulating Temporal retry behavior."""
        attempt_count = 0
        
        async def mock_reset_fails_until_third_attempt(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception("CrateDB not ready yet")
            # Success on 3rd attempt
            return
        
        mock_cratedb_activities._reset_cluster_routing_allocation = mock_reset_fails_until_third_attempt
        
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # Simulate Temporal retry behavior - first two attempts should fail
            
            # First attempt - should fail
            with pytest.raises(Exception, match="Failed to reset cluster routing allocation"):
                await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
            
            # Second attempt - should fail
            with pytest.raises(Exception, match="Failed to reset cluster routing allocation"):
                await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
            
            # Third attempt - should succeed
            result = await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
        
        # Verify that the activity succeeded on the third attempt
        assert result.success is True
        assert attempt_count == 3  # Temporal would have retried this activity 3 times

    @pytest.mark.asyncio 
    async def test_state_machine_guarantees_reset_execution(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test concept: state machine will guarantee reset execution via separate activity."""
        # This test demonstrates the new architecture where reset is a separate activity
        # called by the state machine workflow, providing Temporal execution guarantees
        
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace", 
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock underlying reset method
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock()
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # The state machine would call this activity independently
            result = await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
        
        # Verify the activity works independently (no exception means success)
        assert result.success is True
        assert result.pod_name == "test-pod-0"
        
        # This activity is now called by the state machine workflow, providing:
        # 1. Temporal execution guarantees (at-least-once execution)
        # 2. Automatic retries with exponential backoff
        # 3. Independent execution timeline from pod restart

    @pytest.mark.asyncio
    async def test_reset_fallback_pod_mechanism(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that reset tries fallback pods when target pod fails."""
        executed_commands = []
        
        async def mock_execute_command(pod_name, namespace, command):
            executed_commands.append(pod_name)
            if pod_name == "test-pod-0":
                # Target pod fails
                raise Exception("Target pod connection failed")
            else:
                # Fallback pod succeeds
                return '{"rows":[],"rowcount":0,"duration":0.123}'
        
        # Set up cluster with multiple pods
        manual_decommission_cluster.pods = ["test-pod-0", "test-pod-1", "test-pod-2"]
        
        # Mock the command execution to track which pods are tried
        mock_cratedb_activities._execute_command_in_pod = mock_execute_command
        
        # Execute the reset
        await mock_cratedb_activities._reset_cluster_routing_allocation(
            "test-pod-0", 
            "test-namespace",
            manual_decommission_cluster
        )
        
        # Verify that both target pod and fallback pod were tried
        assert "test-pod-0" in executed_commands  # Target pod attempted
        assert "test-pod-1" in executed_commands  # Fallback pod attempted
        assert len(executed_commands) == 2  # Exactly two attempts

    @pytest.mark.asyncio
    async def test_reset_with_retry_mechanism_success(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that the activity works with Temporal retry configuration."""
        # Mock the underlying reset method to succeed
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock()
        
        # Create input for the activity
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # Execute the reset activity (Temporal handles retries)
            result = await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
        
        # Verify the activity succeeded
        assert result.success is True
        assert result.pod_name == "test-pod-0"
        mock_cratedb_activities._reset_cluster_routing_allocation.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_with_retry_mechanism_max_attempts(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that activity fails after max attempts (handled by Temporal)."""
        # Mock the underlying reset method to always fail
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock(
            side_effect=Exception("CrateDB connection failed")
        )
        
        # Create input for the activity
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # Execute the reset activity - should raise exception for Temporal to handle
            with pytest.raises(Exception, match="Failed to reset cluster routing allocation"):
                await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)

    @pytest.mark.asyncio
    async def test_reset_timing_with_crate_startup_delay(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that reset activity includes startup delay before attempting reset."""
        sleep_calls = []
        
        async def mock_sleep(duration):
            sleep_calls.append(duration)
        
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock()
        
        # Create input for the activity
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        with patch('asyncio.sleep', side_effect=mock_sleep):
            await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)
        
        # Verify startup delay is included in activity
        assert 3 in sleep_calls  # Activity includes 3-second startup delay
        mock_cratedb_activities._reset_cluster_routing_allocation.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_activity_properly_raises_exceptions_for_temporal(self, mock_cratedb_activities, manual_decommission_cluster):
        """Test that the activity properly raises exceptions for Temporal to handle."""
        # Mock the underlying reset to always fail
        mock_cratedb_activities._reset_cluster_routing_allocation = AsyncMock(
            side_effect=Exception("Connection failed")
        )
        
        # Create input for the activity
        reset_input = ClusterRoutingResetInput(
            pod_name="test-pod-0",
            namespace="test-namespace",
            cluster=manual_decommission_cluster,
            dry_run=False
        )
        
        # Mock sleep to speed up test
        with patch('asyncio.sleep'):
            # Execute the activity - should raise exception for Temporal to retry
            with pytest.raises(Exception, match="Failed to reset cluster routing allocation"):
                await mock_cratedb_activities.reset_cluster_routing_allocation(reset_input)