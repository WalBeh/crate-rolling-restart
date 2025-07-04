#!/usr/bin/env python3
"""
Test suite for Temporal.io feature improvements.

This test suite validates the enhanced Temporal features including:
- Standardized retry policies and timeout configurations
- Heartbeat functionality in long-running activities
- Enhanced signal handling and query support
- Comprehensive configuration utilities
- Better error handling and classification
"""

import asyncio
import pytest
from datetime import timedelta
from unittest.mock import Mock, patch, AsyncMock



from rr.state_machines import (
    StateMachineConfig,
    HealthCheckStateMachine,
    PodRestartStateMachine,
    ClusterRestartStateMachine,
    ConfigurationError,
    ValidationError,
    ResourceNotFoundError,
    PodNotFoundError,
    TransientError,
    NetworkError,
    APIError,
)
from rr.models import (
    CrateDBCluster,
    HealthCheckInput,
    PodRestartInput,
    RestartOptions,
)
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment


class TestStateMachineConfig:
    """Test StateMachineConfig utility methods."""

    def test_standard_retry_policy_health_check(self):
        """Test retry policy for health check operations."""
        policy = StateMachineConfig.get_standard_retry_policy("health_check")
        
        assert isinstance(policy, RetryPolicy)
        assert policy.initial_interval == timedelta(seconds=5)
        assert policy.maximum_interval == timedelta(seconds=30)
        assert policy.maximum_attempts == 30
        assert policy.backoff_coefficient == 2.0
        assert policy.non_retryable_error_types is not None
        assert "ConfigurationError" in policy.non_retryable_error_types
        assert "ValidationError" in policy.non_retryable_error_types

    def test_standard_retry_policy_decommission(self):
        """Test retry policy for decommission operations."""
        policy = StateMachineConfig.get_standard_retry_policy("decommission")
        
        assert isinstance(policy, RetryPolicy)
        assert policy.initial_interval == timedelta(seconds=10)
        assert policy.maximum_interval == timedelta(seconds=60)
        assert policy.maximum_attempts == 3
        assert policy.backoff_coefficient == 2.0
        assert policy.non_retryable_error_types is not None
        assert "PodNotFoundError" in policy.non_retryable_error_types

    def test_standard_retry_policy_unknown_operation(self):
        """Test retry policy for unknown operations returns default."""
        policy = StateMachineConfig.get_standard_retry_policy("unknown_operation")
        
        assert isinstance(policy, RetryPolicy)
        assert policy.maximum_attempts == 3

    def test_standard_timeouts_health_check(self):
        """Test timeout configuration for health check operations."""
        timeouts = StateMachineConfig.get_standard_timeouts("health_check")
        
        assert "start_to_close_timeout" in timeouts
        assert "heartbeat_timeout" in timeouts
        assert timeouts["start_to_close_timeout"] == timedelta(minutes=10)
        assert timeouts["heartbeat_timeout"] == timedelta(seconds=30)

    def test_standard_timeouts_decommission(self):
        """Test timeout configuration for decommission operations."""
        timeouts = StateMachineConfig.get_standard_timeouts("decommission")
        
        assert timeouts["start_to_close_timeout"] == timedelta(minutes=15)
        assert timeouts["heartbeat_timeout"] == timedelta(seconds=30)

    def test_comprehensive_config_without_cluster(self):
        """Test comprehensive configuration without cluster object."""
        config = StateMachineConfig.get_comprehensive_config("health_check")
        
        assert "retry_policy" in config
        assert "timeouts" in config
        assert isinstance(config["retry_policy"], RetryPolicy)
        assert isinstance(config["timeouts"], dict)

    def test_comprehensive_config_with_cluster_decommission(self):
        """Test comprehensive configuration with cluster object for decommission."""
        cluster = CrateDBCluster(
            name="test-cluster",
            namespace="default",
            crd_name="test-crd",
            statefulset_name="test-sts",
            health="GREEN",
            replicas=1,
            pods=["test-pod-0"],
            dc_util_timeout=300,
            has_dc_util=True,
            has_prestop_hook=True,
            min_availability="primaries"
        )
        
        config = StateMachineConfig.get_comprehensive_config("decommission", cluster)
        
        assert config["timeouts"]["start_to_close_timeout"] == timedelta(seconds=420)  # 300 + 120
        assert config["retry_policy"].maximum_attempts == 2  # Fewer retries for K8s-managed

    def test_create_activity_config_basic(self):
        """Test creating basic activity configuration."""
        config = StateMachineConfig.create_activity_config("health_check")
        
        assert "start_to_close_timeout" in config
        assert "heartbeat_timeout" in config
        assert "retry_policy" in config
        assert isinstance(config["retry_policy"], RetryPolicy)

    def test_create_activity_config_with_overrides(self):
        """Test creating activity configuration with overrides."""
        config = StateMachineConfig.create_activity_config(
            "health_check",
            timeout=120,
            max_attempts=5
        )
        
        assert config["start_to_close_timeout"] == timedelta(seconds=120)
        assert config["retry_policy"].maximum_attempts == 5

    def test_should_retry_error_non_retryable_types(self):
        """Test error retry logic for non-retryable error types."""
        assert not StateMachineConfig.should_retry_error(ConfigurationError("test"))
        assert not StateMachineConfig.should_retry_error(ValidationError("test"))
        assert not StateMachineConfig.should_retry_error(ResourceNotFoundError("test"))
        assert not StateMachineConfig.should_retry_error(PodNotFoundError("test"))

    def test_should_retry_error_retryable_types(self):
        """Test error retry logic for retryable error types."""
        assert StateMachineConfig.should_retry_error(TransientError("test"))
        assert StateMachineConfig.should_retry_error(NetworkError("test"))
        assert StateMachineConfig.should_retry_error(APIError("test"))
        assert StateMachineConfig.should_retry_error(Exception("temporary failure"))

    def test_should_retry_error_patterns(self):
        """Test error retry logic for specific error patterns."""
        assert not StateMachineConfig.should_retry_error(Exception("Resource not found"))
        assert not StateMachineConfig.should_retry_error(Exception("Invalid configuration"))
        assert not StateMachineConfig.should_retry_error(Exception("Permission denied"))
        assert not StateMachineConfig.should_retry_error(Exception("Unauthorized access"))
        assert not StateMachineConfig.should_retry_error(Exception("Forbidden operation"))
        assert not StateMachineConfig.should_retry_error(Exception("Bad request format"))
        assert not StateMachineConfig.should_retry_error(Exception("Malformed input"))

    def test_should_retry_error_retryable_patterns(self):
        """Test error retry logic for retryable error patterns."""
        assert StateMachineConfig.should_retry_error(Exception("Connection timeout"))
        assert StateMachineConfig.should_retry_error(Exception("Service unavailable"))
        assert StateMachineConfig.should_retry_error(Exception("Internal server error"))


class TestExceptionHierarchy:
    """Test the new exception hierarchy."""

    def test_non_retryable_error_inheritance(self):
        """Test that non-retryable errors inherit correctly."""
        assert issubclass(ConfigurationError, Exception)
        assert issubclass(ValidationError, Exception)
        assert issubclass(ResourceNotFoundError, Exception)
        assert issubclass(PodNotFoundError, Exception)

    def test_transient_error_inheritance(self):
        """Test that transient errors inherit correctly."""
        assert issubclass(TransientError, Exception)
        assert issubclass(NetworkError, TransientError)
        assert issubclass(APIError, TransientError)

    def test_exception_creation(self):
        """Test exception creation with messages."""
        config_error = ConfigurationError("Invalid config")
        assert str(config_error) == "Invalid config"

        validation_error = ValidationError("Validation failed")
        assert str(validation_error) == "Validation failed"

        network_error = NetworkError("Network timeout")
        assert str(network_error) == "Network timeout"


class TestHealthCheckStateMachine:
    """Test the enhanced HealthCheckStateMachine."""

    def test_health_check_uses_standard_config(self):
        """Test that HealthCheckStateMachine uses standardized configuration."""
        # Create test cluster
        cluster = CrateDBCluster(
            name="test-cluster",
            namespace="default",
            crd_name="test-crd",
            statefulset_name="test-sts",
            health="GREEN",
            replicas=1,
            pods=["test-pod-0"],
            dc_util_timeout=300,
            has_dc_util=True,
            has_prestop_hook=True,
            min_availability="primaries"
        )

        health_input = HealthCheckInput(
            cluster=cluster,
            dry_run=False,
            timeout=30
        )

        # Test would require full Temporal environment setup
        # This validates the structure and imports work correctly
        state_machine = HealthCheckStateMachine()
        assert hasattr(state_machine, 'run')


class TestClusterRestartStateMachine:
    """Test the enhanced ClusterRestartStateMachine."""

    def test_signal_handling(self):
        """Test signal handling in ClusterRestartStateMachine."""
        state_machine = ClusterRestartStateMachine()
        
        # Test initial state
        assert not state_machine.force_restart_signal
        assert not state_machine.pause_signal
        assert not state_machine.cancel_signal
        assert state_machine.current_pod == ""
        assert state_machine.pods_completed == []
        assert state_machine.skipped_pods == []

        # Test force restart signal - patch the workflow logger to avoid issues
        with patch('rr.state_machines.workflow.logger') as mock_logger:
            state_machine.force_restart("Emergency restart needed")
            assert state_machine.force_restart_signal
            assert state_machine.force_restart_reason == "Emergency restart needed"

        # Test pause signal
        with patch('rr.state_machines.workflow.logger') as mock_logger:
            state_machine.pause_restart("Maintenance window")
            assert state_machine.pause_signal

        # Test resume signal
        with patch('rr.state_machines.workflow.logger') as mock_logger:
            state_machine.resume_restart()
            assert not state_machine.pause_signal

        # Test cancel signal
        with patch('rr.state_machines.workflow.logger') as mock_logger:
            state_machine.cancel_restart("User cancelled")
            assert state_machine.cancel_signal

    def test_query_status(self):
        """Test query functionality in ClusterRestartStateMachine."""
        state_machine = ClusterRestartStateMachine()
        
        # Update some state
        state_machine.current_pod = "test-pod-1"
        state_machine.pods_completed = ["test-pod-0"]
        state_machine.skipped_pods = ["test-pod-2"]
        state_machine.pause_signal = True
        state_machine.force_restart_signal = True

        status = state_machine.get_status()
        
        assert status["current_pod"] == "test-pod-1"
        assert status["pods_completed"] == ["test-pod-0"]
        assert status["skipped_pods"] == ["test-pod-2"]
        assert status["paused"] == True
        assert status["cancelled"] == False
        assert status["force_restart_active"] == True


class TestConfigurationIntegration:
    """Integration tests for configuration utilities."""

    def test_end_to_end_configuration_flow(self):
        """Test complete configuration flow for different operation types."""
        # Test health check configuration
        health_config = StateMachineConfig.create_activity_config("health_check")
        assert health_config["start_to_close_timeout"] == timedelta(minutes=10)
        assert health_config["heartbeat_timeout"] == timedelta(seconds=30)
        assert health_config["retry_policy"].maximum_attempts == 30

        # Test decommission configuration with cluster
        cluster = CrateDBCluster(
            name="test-cluster",
            namespace="default",
            crd_name="test-crd",
            statefulset_name="test-sts",
            health="GREEN",
            replicas=1,
            pods=["test-pod-0"],
            dc_util_timeout=600,
            has_dc_util=False,  # Manual decommission
            has_prestop_hook=False,
            min_availability="primaries"
        )
        
        decommission_config = StateMachineConfig.create_activity_config("decommission", cluster)
        assert decommission_config["start_to_close_timeout"] == timedelta(seconds=720)  # 600 + 120

        # Test with overrides
        custom_config = StateMachineConfig.create_activity_config(
            "pod_operations",
            timeout=180,
            max_attempts=8
        )
        assert custom_config["start_to_close_timeout"] == timedelta(seconds=180)
        assert custom_config["retry_policy"].maximum_attempts == 8

    def test_configuration_consistency(self):
        """Test that configurations are consistent across different methods."""
        # Test that get_standard_* methods match create_activity_config
        retry_policy = StateMachineConfig.get_standard_retry_policy("health_check")
        timeouts = StateMachineConfig.get_standard_timeouts("health_check")
        
        activity_config = StateMachineConfig.create_activity_config("health_check")
        
        assert activity_config["retry_policy"].maximum_attempts == retry_policy.maximum_attempts
        assert activity_config["start_to_close_timeout"] == timeouts["start_to_close_timeout"]
        assert activity_config["heartbeat_timeout"] == timeouts["heartbeat_timeout"]

    def test_comprehensive_config_structure(self):
        """Test the structure of comprehensive configuration."""
        config = StateMachineConfig.get_comprehensive_config("api_calls")
        
        # Verify required keys
        assert "retry_policy" in config
        assert "timeouts" in config
        
        # Verify structure
        assert isinstance(config["retry_policy"], RetryPolicy)
        assert isinstance(config["timeouts"], dict)
        assert "start_to_close_timeout" in config["timeouts"]
        assert "heartbeat_timeout" in config["timeouts"]

    def test_legacy_method_compatibility(self):
        """Test that legacy methods still work for backward compatibility."""
        # Test legacy health check retry policy method
        legacy_policy = StateMachineConfig.get_health_check_retry_policy("YELLOW")
        assert isinstance(legacy_policy, RetryPolicy)
        assert legacy_policy.maximum_attempts == 30

        # Test legacy timeout calculation methods
        cluster = CrateDBCluster(
            name="test-cluster",
            namespace="default",
            crd_name="test-crd",
            statefulset_name="test-sts",
            health="GREEN",
            replicas=1,
            pods=["test-pod-0"],
            dc_util_timeout=300,
            has_dc_util=True,
            has_prestop_hook=True,
            min_availability="primaries"
        )
        
        decommission_timeout = StateMachineConfig.get_decommission_timeout(cluster)
        assert decommission_timeout == 420  # 300 + 120

        pod_timeout = StateMachineConfig.get_pod_restart_timeout(cluster, 600)
        assert pod_timeout == 1020  # 300 + 600 + 120


if __name__ == "__main__":
    pytest.main([__file__, "-v"])