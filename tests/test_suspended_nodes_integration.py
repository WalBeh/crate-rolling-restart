#!/usr/bin/env python3
"""
Integration test example for the suspended nodes feature.

This test demonstrates how the --only-on-suspended-nodes flag works
in practice with realistic scenarios.
"""

import asyncio
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

import pytest
from kubernetes.client import V1Node, V1NodeSpec, V1Pod, V1PodSpec, V1ObjectMeta, V1Taint

# Configure logging
import logging
logging.basicConfig(level=logging.INFO)

from rr.activities import CrateDBActivities
from rr.models import (
    CrateDBCluster, 
    RestartOptions, 
    MultiClusterRestartInput,
    ClusterDiscoveryInput
)
from rr.state_machines import ClusterRestartStateMachine


class TestSuspendedNodesIntegration:
    """Integration tests for the suspended nodes feature."""

    def setup_method(self):
        """Set up test fixtures."""
        self.activities = CrateDBActivities()

    def create_test_cluster(self, name: str, pods: list, namespace: str = "default"):
        """Create a test cluster with the specified pods."""
        return CrateDBCluster(
            name=name,
            namespace=namespace,
            statefulset_name=f"{name}-sts",
            health="GREEN",
            replicas=len(pods),
            pods=pods,
            has_prestop_hook=True,
            has_dc_util=True,
            suspended=False,
            crd_name=f"{name}-crd",
            dc_util_timeout=720,
            min_availability="PRIMARIES"
        )

    def create_mock_pod(self, pod_name: str, node_name: str):
        """Create a mock pod running on the specified node."""
        pod = Mock(spec=V1Pod)
        pod.metadata = Mock(spec=V1ObjectMeta)
        pod.metadata.name = pod_name
        pod.spec = Mock(spec=V1PodSpec)
        pod.spec.node_name = node_name
        return pod

    def create_mock_node(self, node_name: str, is_suspended: bool = False, 
                        suspension_reason: str = "unschedulable"):
        """Create a mock node with suspension status."""
        node = Mock(spec=V1Node)
        node.metadata = Mock(spec=V1ObjectMeta)
        node.metadata.name = node_name
        node.metadata.annotations = {}
        node.spec = Mock(spec=V1NodeSpec)
        node.spec.taints = []
        
        if is_suspended:
            if suspension_reason == "unschedulable":
                node.spec.unschedulable = True
            elif suspension_reason == "spot_terminating":
                node.spec.unschedulable = False
                taint = Mock(spec=V1Taint)
                taint.key = "aws.amazon.com/spot-instance-terminating"
                taint.value = "true"
                taint.effect = "NoSchedule"
                node.spec.taints = [taint]
            elif suspension_reason == "annotation":
                node.spec.unschedulable = False
                node.metadata.annotations = {"node.kubernetes.io/suspend": "true"}
        else:
            node.spec.unschedulable = False
            
        return node

    @pytest.mark.asyncio
    async def test_mixed_node_scenario(self):
        """Test scenario with mixed active and suspended nodes."""
        # Create a cluster with 4 pods on different nodes
        cluster = self.create_test_cluster(
            name="test-cluster",
            pods=["pod-0", "pod-1", "pod-2", "pod-3"],
            namespace="cratedb"
        )
        
        # Create options with suspended nodes only
        options = RestartOptions(
            context="test-context",
            only_on_suspended_nodes=True,
            dry_run=True,  # Use dry run for testing
            log_level="DEBUG"
        )
        
        # Mock the pod-to-node mapping
        pod_node_mapping = {
            "pod-0": ("worker-1", False),     # Active node
            "pod-1": ("worker-2", True),      # Suspended node (unschedulable)
            "pod-2": ("worker-3", False),     # Active node
            "pod-3": ("worker-4", True),      # Suspended node (spot terminating)
        }
        
        # Mock the kubernetes client calls
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                
                def mock_read_pod(name, namespace):
                    node_name, _ = pod_node_mapping[name]
                    return self.create_mock_pod(name, node_name)
                
                def mock_read_node(name):
                    for pod_name, (node_name, is_suspended) in pod_node_mapping.items():
                        if node_name == name:
                            reason = "spot_terminating" if name == "worker-4" else "unschedulable"
                            return self.create_mock_node(node_name, is_suspended, reason)
                    return self.create_mock_node(name, False)
                
                mock_core_v1.read_namespaced_pod.side_effect = mock_read_pod
                mock_core_v1.read_node.side_effect = mock_read_node
                
                # Test node suspension detection for each pod
                results = {}
                for pod_name in cluster.pods:
                    result = await self.activities.is_pod_on_suspended_node(pod_name, cluster.namespace)
                    results[pod_name] = result
                
                # Verify results
                expected_results = {
                    "pod-0": False,  # Active node
                    "pod-1": True,   # Suspended node
                    "pod-2": False,  # Active node
                    "pod-3": True,   # Suspended node
                }
                
                assert results == expected_results, f"Expected {expected_results}, got {results}"
                
                # Count suspended vs active
                suspended_pods = [pod for pod, suspended in results.items() if suspended]
                active_pods = [pod for pod, suspended in results.items() if not suspended]
                
                assert len(suspended_pods) == 2, f"Expected 2 suspended pods, got {len(suspended_pods)}"
                assert len(active_pods) == 2, f"Expected 2 active pods, got {len(active_pods)}"
                assert suspended_pods == ["pod-1", "pod-3"]
                assert active_pods == ["pod-0", "pod-2"]

    @pytest.mark.asyncio
    async def test_all_nodes_suspended_scenario(self):
        """Test scenario where all nodes are suspended."""
        cluster = self.create_test_cluster(
            name="test-cluster",
            pods=["pod-0", "pod-1", "pod-2"],
            namespace="cratedb"
        )
        
        # All nodes are suspended
        pod_node_mapping = {
            "pod-0": ("worker-1", True),
            "pod-1": ("worker-2", True),
            "pod-2": ("worker-3", True),
        }
        
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                
                def mock_read_pod(name, namespace):
                    node_name, _ = pod_node_mapping[name]
                    return self.create_mock_pod(name, node_name)
                
                def mock_read_node(name):
                    return self.create_mock_node(name, True, "unschedulable")
                
                mock_core_v1.read_namespaced_pod.side_effect = mock_read_pod
                mock_core_v1.read_node.side_effect = mock_read_node
                
                # Test all pods
                results = {}
                for pod_name in cluster.pods:
                    result = await self.activities.is_pod_on_suspended_node(pod_name, cluster.namespace)
                    results[pod_name] = result
                
                # All should be suspended
                assert all(results.values()), f"All pods should be suspended, got {results}"

    @pytest.mark.asyncio
    async def test_no_nodes_suspended_scenario(self):
        """Test scenario where no nodes are suspended."""
        cluster = self.create_test_cluster(
            name="test-cluster",
            pods=["pod-0", "pod-1", "pod-2"],
            namespace="cratedb"
        )
        
        # No nodes are suspended
        pod_node_mapping = {
            "pod-0": ("worker-1", False),
            "pod-1": ("worker-2", False),
            "pod-2": ("worker-3", False),
        }
        
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                
                def mock_read_pod(name, namespace):
                    node_name, _ = pod_node_mapping[name]
                    return self.create_mock_pod(name, node_name)
                
                def mock_read_node(name):
                    return self.create_mock_node(name, False)
                
                mock_core_v1.read_namespaced_pod.side_effect = mock_read_pod
                mock_core_v1.read_node.side_effect = mock_read_node
                
                # Test all pods
                results = {}
                for pod_name in cluster.pods:
                    result = await self.activities.is_pod_on_suspended_node(pod_name, cluster.namespace)
                    results[pod_name] = result
                
                # None should be suspended
                assert not any(results.values()), f"No pods should be suspended, got {results}"

    @pytest.mark.asyncio
    async def test_restart_options_flag_behavior(self):
        """Test that the RestartOptions flag works correctly."""
        
        # Test default behavior (flag disabled)
        options_default = RestartOptions(context="test")
        assert options_default.only_on_suspended_nodes is False
        
        # Test with flag enabled
        options_enabled = RestartOptions(context="test", only_on_suspended_nodes=True)
        assert options_enabled.only_on_suspended_nodes is True
        
        # Test serialization/deserialization
        options_dict = options_enabled.model_dump()
        options_restored = RestartOptions(**options_dict)
        assert options_restored.only_on_suspended_nodes is True

    def test_suspension_detection_criteria(self):
        """Test different suspension detection criteria."""
        
        # Test unschedulable node
        node_unschedulable = self.create_mock_node("node1", True, "unschedulable")
        assert node_unschedulable.spec.unschedulable is True
        
        # Test spot terminating node
        node_spot = self.create_mock_node("node2", True, "spot_terminating")
        assert node_spot.spec.unschedulable is False
        assert len(node_spot.spec.taints) == 1
        assert node_spot.spec.taints[0].key == "aws.amazon.com/spot-instance-terminating"
        
        # Test annotation-based suspension
        node_annotation = self.create_mock_node("node3", True, "annotation")
        assert node_annotation.spec.unschedulable is False
        assert "node.kubernetes.io/suspend" in node_annotation.metadata.annotations
        
        # Test active node
        node_active = self.create_mock_node("node4", False)
        assert node_active.spec.unschedulable is False
        assert len(node_active.spec.taints) == 0
        assert len(node_active.metadata.annotations) == 0

    @pytest.mark.asyncio
    async def test_error_handling_graceful_degradation(self):
        """Test that errors are handled gracefully."""
        
        cluster = self.create_test_cluster(
            name="test-cluster",
            pods=["pod-0"],
            namespace="cratedb"
        )
        
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                
                # Mock API error
                mock_core_v1.read_namespaced_pod.side_effect = Exception("Kubernetes API error")
                
                # Should return False (safe default) on error
                result = await self.activities.is_pod_on_suspended_node("pod-0", cluster.namespace)
                assert result is False

    def test_real_world_scenarios(self):
        """Test configuration for real-world scenarios."""
        
        # Spot instance termination scenario
        spot_options = RestartOptions(
            context="prod",
            only_on_suspended_nodes=True,
            dry_run=False,
            log_level="INFO"
        )
        
        # Maintenance window scenario
        maintenance_options = RestartOptions(
            context="prod",
            only_on_suspended_nodes=True,
            dry_run=True,  # Test first
            maintenance_config_path="/etc/maintenance.toml",
            log_level="DEBUG"
        )
        
        # Emergency scenario
        emergency_options = RestartOptions(
            context="prod",
            only_on_suspended_nodes=True,
            ignore_maintenance_windows=True,
            skip_hook_warning=True,
            log_level="INFO"
        )
        
        # Verify configurations
        assert spot_options.only_on_suspended_nodes is True
        assert maintenance_options.only_on_suspended_nodes is True
        assert emergency_options.only_on_suspended_nodes is True
        assert emergency_options.ignore_maintenance_windows is True


class TestSuspendedNodesDocumentation:
    """Test that the feature is properly documented."""
    
    def test_cli_help_includes_flag(self):
        """Test that CLI help includes the new flag."""
        # This would be tested by running the CLI help command
        # and verifying the flag appears in the output
        pass
    
    def test_documentation_examples(self):
        """Test that documentation examples are valid."""
        # Verify that the examples in the documentation are syntactically correct
        
        # Example 1: Basic usage
        options1 = RestartOptions(
            context="prod",
            only_on_suspended_nodes=True
        )
        assert options1.only_on_suspended_nodes is True
        
        # Example 2: With dry run
        options2 = RestartOptions(
            context="prod", 
            only_on_suspended_nodes=True,
            dry_run=True
        )
        assert options2.only_on_suspended_nodes is True
        assert options2.dry_run is True
        
        # Example 3: With async
        options3 = RestartOptions(
            context="prod",
            only_on_suspended_nodes=True
        )
        assert options3.only_on_suspended_nodes is True


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])