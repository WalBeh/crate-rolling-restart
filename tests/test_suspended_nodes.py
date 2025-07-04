#!/usr/bin/env python3
"""
Test the suspended nodes functionality.
"""

import asyncio
import sys
from unittest.mock import Mock, patch

import pytest
from kubernetes.client import V1Node, V1NodeSpec, V1NodeStatus, V1Pod, V1PodSpec, V1ObjectMeta, V1Taint

# Configure logging
import logging
logging.basicConfig(level=logging.DEBUG)

from rr.activities import CrateDBActivities
from rr.models import RestartOptions


class TestSuspendedNodesActivity:
    """Test the is_pod_on_suspended_node activity."""

    def setup_method(self):
        """Set up test fixtures."""
        self.activities = CrateDBActivities()

    def create_mock_pod(self, pod_name: str, node_name: str):
        """Create a mock pod object."""
        pod = Mock(spec=V1Pod)
        pod.metadata = Mock(spec=V1ObjectMeta)
        pod.metadata.name = pod_name
        pod.spec = Mock(spec=V1PodSpec)
        pod.spec.node_name = node_name
        return pod

    def create_mock_node(self, node_name: str, unschedulable: bool = False, taints: list = None):
        """Create a mock node object."""
        node = Mock(spec=V1Node)
        node.metadata = Mock(spec=V1ObjectMeta)
        node.metadata.name = node_name
        node.metadata.annotations = {}
        node.spec = Mock(spec=V1NodeSpec)
        node.spec.unschedulable = unschedulable
        node.spec.taints = taints or []
        return node

    def create_mock_taint(self, key: str, value: str = "true", effect: str = "NoSchedule"):
        """Create a mock taint object."""
        taint = Mock(spec=V1Taint)
        taint.key = key
        taint.value = value
        taint.effect = effect
        return taint

    @pytest.mark.asyncio
    async def test_pod_on_unschedulable_node(self):
        """Test that a pod on an unschedulable node is detected as suspended."""
        pod_name = "test-pod"
        node_name = "test-node"
        namespace = "test-namespace"

        # Create mocks
        mock_pod = self.create_mock_pod(pod_name, node_name)
        mock_node = self.create_mock_node(node_name, unschedulable=True)

        # Mock the kubernetes client
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(return_value=mock_pod)
                mock_core_v1.read_node = Mock(return_value=mock_node)

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result
                assert result is True

    @pytest.mark.asyncio
    async def test_pod_on_active_node(self):
        """Test that a pod on an active node is not detected as suspended."""
        pod_name = "test-pod"
        node_name = "test-node"
        namespace = "test-namespace"

        # Create mocks
        mock_pod = self.create_mock_pod(pod_name, node_name)
        mock_node = self.create_mock_node(node_name, unschedulable=False)

        # Mock the kubernetes client
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(return_value=mock_pod)
                mock_core_v1.read_node = Mock(return_value=mock_node)

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result
                assert result is False

    @pytest.mark.asyncio
    async def test_pod_on_node_with_suspension_taint(self):
        """Test that a pod on a node with suspension taint is detected as suspended."""
        pod_name = "test-pod"
        node_name = "test-node"
        namespace = "test-namespace"

        # Create mocks with suspension taint
        mock_pod = self.create_mock_pod(pod_name, node_name)
        suspension_taint = self.create_mock_taint("node.kubernetes.io/unschedulable")
        mock_node = self.create_mock_node(node_name, unschedulable=False, taints=[suspension_taint])

        # Mock the kubernetes client
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(return_value=mock_pod)
                mock_core_v1.read_node = Mock(return_value=mock_node)

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result
                assert result is True

    @pytest.mark.asyncio
    async def test_pod_on_node_with_spot_termination_taint(self):
        """Test that a pod on a node with spot termination taint is detected as suspended."""
        pod_name = "test-pod"
        node_name = "test-node"
        namespace = "test-namespace"

        # Create mocks with spot termination taint
        mock_pod = self.create_mock_pod(pod_name, node_name)
        spot_taint = self.create_mock_taint("aws.amazon.com/spot-instance-terminating")
        mock_node = self.create_mock_node(node_name, unschedulable=False, taints=[spot_taint])

        # Mock the kubernetes client
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(return_value=mock_pod)
                mock_core_v1.read_node = Mock(return_value=mock_node)

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result
                assert result is True

    @pytest.mark.asyncio
    async def test_pod_on_node_with_suspension_annotation(self):
        """Test that a pod on a node with suspension annotation is detected as suspended."""
        pod_name = "test-pod"
        node_name = "test-node"
        namespace = "test-namespace"

        # Create mocks with suspension annotation
        mock_pod = self.create_mock_pod(pod_name, node_name)
        mock_node = self.create_mock_node(node_name, unschedulable=False)
        mock_node.metadata.annotations = {"node.kubernetes.io/suspend": "true"}

        # Mock the kubernetes client
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(return_value=mock_pod)
                mock_core_v1.read_node = Mock(return_value=mock_node)

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result
                assert result is True

    @pytest.mark.asyncio
    async def test_pod_with_no_node_assignment(self):
        """Test that a pod with no node assignment is not detected as suspended."""
        pod_name = "test-pod"
        namespace = "test-namespace"

        # Create mock pod with no node assignment
        mock_pod = self.create_mock_pod(pod_name, None)
        mock_pod.spec.node_name = None

        # Mock the kubernetes client
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(return_value=mock_pod)

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result
                assert result is False

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test that errors are handled gracefully."""
        pod_name = "test-pod"
        namespace = "test-namespace"

        # Mock the kubernetes client to raise an exception
        with patch.object(self.activities, '_ensure_kube_client'):
            with patch.object(self.activities, 'core_v1') as mock_core_v1:
                mock_core_v1.read_namespaced_pod = Mock(side_effect=Exception("API Error"))

                # Test the activity
                result = await self.activities.is_pod_on_suspended_node(pod_name, namespace)

                # Verify the result defaults to False on error
                assert result is False


class TestRestartOptionsModel:
    """Test the RestartOptions model with the new field."""

    def test_restart_options_default_values(self):
        """Test that RestartOptions has correct default values."""
        options = RestartOptions()
        
        assert options.only_on_suspended_nodes is False
        assert options.dry_run is False
        assert options.skip_hook_warning is False

    def test_restart_options_with_suspended_nodes_flag(self):
        """Test RestartOptions with suspended nodes flag enabled."""
        options = RestartOptions(
            only_on_suspended_nodes=True,
            context="test-context"
        )
        
        assert options.only_on_suspended_nodes is True
        assert options.context == "test-context"

    def test_restart_options_serialization(self):
        """Test that RestartOptions can be serialized and deserialized."""
        original_options = RestartOptions(
            only_on_suspended_nodes=True,
            context="test-context",
            dry_run=True
        )
        
        # Test conversion to dict
        options_dict = original_options.model_dump()
        assert options_dict["only_on_suspended_nodes"] is True
        assert options_dict["context"] == "test-context"
        assert options_dict["dry_run"] is True
        
        # Test reconstruction from dict
        reconstructed_options = RestartOptions(**options_dict)
        assert reconstructed_options.only_on_suspended_nodes is True
        assert reconstructed_options.context == "test-context"
        assert reconstructed_options.dry_run is True


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])