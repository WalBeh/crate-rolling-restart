#!/usr/bin/env python3
"""
Test script to verify maintenance configuration integration works correctly.

This test ensures that dc_util_timeout and min_availability values from 
maintenance-windows.toml are properly applied to cluster configurations.
"""

import asyncio
import logging
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from rr.activities import CrateDBActivities
from rr.models import ClusterDiscoveryInput, CrateDBCluster
from rr.maintenance_windows import MaintenanceWindowChecker

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_test_maintenance_config():
    """Create a temporary maintenance configuration file for testing."""
    config_content = """
# Test Maintenance Configuration
[test-cluster-1]
timezone = "UTC"
min_window_duration = 30
dc_util_timeout = 900
min_availability = "FULL"

[[test-cluster-1.windows]]
time = "02:00-04:00"
weekdays = ["sat", "sun"]
description = "Weekend maintenance"

[test-cluster-2]
timezone = "UTC"
min_window_duration = 60
dc_util_timeout = 1200
min_availability = "NONE"

[[test-cluster-2.windows]]
time = "18:00-20:00"
weekdays = ["mon", "tue", "wed"]
description = "Evening maintenance"

[test-cluster-default]
timezone = "UTC"
min_window_duration = 45
# No dc_util_timeout or min_availability - should use defaults

[[test-cluster-default.windows]]
time = "12:00-13:00"
weekdays = ["fri"]
description = "Friday lunch maintenance"
"""
    
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False)
    temp_file.write(config_content)
    temp_file.close()
    return temp_file.name


async def test_maintenance_config_loading():
    """Test that maintenance configuration is loaded correctly."""
    
    logger.info("🧪 Testing maintenance configuration loading...")
    
    # Create test config file
    config_path = create_test_maintenance_config()
    
    try:
        # Load maintenance configuration
        checker = MaintenanceWindowChecker(config_path)
        
        # Test cluster with custom values
        config1 = checker.get_cluster_config("test-cluster-1")
        assert config1 is not None, "test-cluster-1 config should exist"
        assert config1.dc_util_timeout == 900, f"Expected dc_util_timeout=900, got {config1.dc_util_timeout}"
        assert config1.min_availability == "FULL", f"Expected min_availability=FULL, got {config1.min_availability}"
        
        # Test cluster with different custom values
        config2 = checker.get_cluster_config("test-cluster-2")
        assert config2 is not None, "test-cluster-2 config should exist"
        assert config2.dc_util_timeout == 1200, f"Expected dc_util_timeout=1200, got {config2.dc_util_timeout}"
        assert config2.min_availability == "NONE", f"Expected min_availability=NONE, got {config2.min_availability}"
        
        # Test cluster with defaults
        config3 = checker.get_cluster_config("test-cluster-default")
        assert config3 is not None, "test-cluster-default config should exist"
        assert config3.dc_util_timeout == 720, f"Expected default dc_util_timeout=720, got {config3.dc_util_timeout}"
        assert config3.min_availability == "PRIMARIES", f"Expected default min_availability=PRIMARIES, got {config3.min_availability}"
        
        # Test non-existent cluster
        config4 = checker.get_cluster_config("non-existent-cluster")
        assert config4 is None, "Non-existent cluster should return None"
        
        logger.info("✅ Maintenance configuration loading test passed")
        
    finally:
        # Clean up
        Path(config_path).unlink()


async def test_cluster_discovery_with_maintenance_config():
    """Test that cluster discovery applies maintenance configuration overrides."""
    
    logger.info("🧪 Testing cluster discovery with maintenance config...")
    
    # Create test config file
    config_path = create_test_maintenance_config()
    
    try:
        # Create mock CRD item
        mock_crd_item = {
            "metadata": {
                "name": "test-cluster-1",
                "namespace": "crate-system"
            },
            "spec": {
                "cluster": {
                    "name": "test-cluster-1"
                }
            },
            "status": {
                "health": "GREEN"
            }
        }
        
        # Create mock StatefulSet
        mock_sts = MagicMock()
        mock_sts.spec.replicas = 3
        mock_sts.spec.template.spec.containers = []
        
        # Create activities instance
        activities = CrateDBActivities()
        
        # Mock dependencies
        with patch.object(activities, '_find_statefulset', return_value=("test-sts", mock_sts)):
            with patch.object(activities, '_analyze_prestop_hook', return_value=(True, True, 720)):
                with patch.object(activities, '_find_pods', return_value=["test-pod-0", "test-pod-1", "test-pod-2"]):
                    with patch.object(activities, '_extract_health_status', return_value="GREEN"):
                        
                        # Test cluster discovery with maintenance config
                        cluster = await activities._process_crd_item(
                            mock_crd_item, 
                            ["test-cluster-1"], 
                            config_path
                        )
                        
                        assert cluster is not None, "Cluster should be discovered"
                        assert cluster.name == "test-cluster-1", f"Expected name=test-cluster-1, got {cluster.name}"
                        assert cluster.dc_util_timeout == 900, f"Expected dc_util_timeout=900 (from config), got {cluster.dc_util_timeout}"
                        assert cluster.min_availability == "FULL", f"Expected min_availability=FULL (from config), got {cluster.min_availability}"
                        
                        logger.info("✅ Cluster discovery with maintenance config test passed")
        
    finally:
        # Clean up
        Path(config_path).unlink()


async def test_cluster_discovery_without_maintenance_config():
    """Test that cluster discovery uses defaults when no maintenance config is provided."""
    
    logger.info("🧪 Testing cluster discovery without maintenance config...")
    
    # Create mock CRD item
    mock_crd_item = {
        "metadata": {
            "name": "test-cluster-no-config",
            "namespace": "default"
        },
        "spec": {
            "cluster": {
                "name": "test-cluster-no-config"
            }
        },
        "status": {
            "health": "YELLOW"
        }
    }
    
    # Create mock StatefulSet
    mock_sts = MagicMock()
    mock_sts.spec.replicas = 2
    mock_sts.spec.template.spec.containers = []
    
    # Create activities instance
    activities = CrateDBActivities()
    
    # Mock dependencies
    with patch.object(activities, '_find_statefulset', return_value=("test-sts", mock_sts)):
        with patch.object(activities, '_analyze_prestop_hook', return_value=(False, False, 720)):
            with patch.object(activities, '_find_pods', return_value=["test-pod-0", "test-pod-1"]):
                with patch.object(activities, '_extract_health_status', return_value="YELLOW"):
                    
                    # Test cluster discovery without maintenance config
                    cluster = await activities._process_crd_item(
                        mock_crd_item, 
                        ["test-cluster-no-config"], 
                        None  # No maintenance config
                    )
                    
                    assert cluster is not None, "Cluster should be discovered"
                    assert cluster.name == "test-cluster-no-config", f"Expected name=test-cluster-no-config, got {cluster.name}"
                    assert cluster.dc_util_timeout == 720, f"Expected default dc_util_timeout=720, got {cluster.dc_util_timeout}"
                    assert cluster.min_availability == "PRIMARIES", f"Expected default min_availability=PRIMARIES, got {cluster.min_availability}"
                    
                    logger.info("✅ Cluster discovery without maintenance config test passed")


async def test_manual_decommission_uses_config_values():
    """Test that manual decommission uses the configuration values from the cluster."""
    
    logger.info("🧪 Testing manual decommission uses config values...")
    
    # Create test cluster with custom values
    cluster = CrateDBCluster(
        name="test-cluster",
        namespace="test-namespace",
        crd_name="test-crd",
        statefulset_name="test-sts",
        replicas=2,
        pods=["test-pod-0", "test-pod-1"],
        health="GREEN",
        has_dc_util=False,  # Manual decommission
        dc_util_timeout=1500,  # Custom timeout
        has_prestop_hook=False,
        min_availability="NONE"  # Custom min_availability
    )
    
    # Create activities instance
    activities = CrateDBActivities()
    
    # Mock the command execution to capture what SQL is generated
    executed_commands = []
    
    async def mock_execute_command(pod_name, namespace, command):
        executed_commands.append(command)
        return "Mock response"
    
    with patch.object(activities, '_ensure_kube_client'):
        with patch.object(activities, '_execute_command_in_pod', side_effect=mock_execute_command):
            
            # Execute manual decommission
            await activities._execute_manual_decommission("test-pod-0", "test-namespace", cluster)
            
            # Verify the SQL commands contain our custom values
            assert len(executed_commands) == 5, f"Expected 5 commands, got {len(executed_commands)}"
            
            # Check timeout value is used
            timeout_command = None
            min_availability_command = None
            
            for cmd in executed_commands:
                if "cluster.graceful_stop.timeout" in cmd:
                    timeout_command = cmd
                if "cluster.graceful_stop.min_availability" in cmd:
                    min_availability_command = cmd
            
            assert timeout_command is not None, "Timeout command should be present"
            assert "1500s" in timeout_command, f"Expected 1500s in timeout command, got: {timeout_command}"
            
            assert min_availability_command is not None, "Min availability command should be present"
            assert "NONE" in min_availability_command, f"Expected NONE in min_availability command, got: {min_availability_command}"
            
            logger.info("✅ Manual decommission uses config values test passed")


async def test_cluster_discovery_input_integration():
    """Test the full integration from ClusterDiscoveryInput to cluster configuration."""
    
    logger.info("🧪 Testing full ClusterDiscoveryInput integration...")
    
    # Create test config file
    config_path = create_test_maintenance_config()
    
    try:
        # Create discovery input with maintenance config
        discovery_input = ClusterDiscoveryInput(
            cluster_names=["test-cluster-1"],
            kubeconfig=None,
            context=None,
            maintenance_config_path=config_path
        )
        
        # Create mock data
        mock_crds = {
            "items": [{
                "metadata": {
                    "name": "test-cluster-1",
                    "namespace": "crate-system"
                },
                "spec": {
                    "cluster": {
                        "name": "test-cluster-1"
                    }
                },
                "status": {
                    "health": "GREEN"
                }
            }]
        }
        
        mock_sts = MagicMock()
        mock_sts.spec.replicas = 3
        mock_sts.spec.template.spec.containers = []
        
        # Create activities instance
        activities = CrateDBActivities()
        
        # Mock all dependencies
        with patch.object(activities, '_ensure_kube_client'):
            with patch.object(activities.core_v1, 'list_namespace') as mock_list_ns:
                with patch.object(activities.custom_api, 'list_namespaced_custom_object', return_value=mock_crds):
                    with patch.object(activities, '_find_statefulset', return_value=("test-sts", mock_sts)):
                        with patch.object(activities, '_analyze_prestop_hook', return_value=(True, True, 720)):
                            with patch.object(activities, '_find_pods', return_value=["test-pod-0", "test-pod-1", "test-pod-2"]):
                                with patch.object(activities, '_extract_health_status', return_value="GREEN"):
                                    
                                    # Mock namespace list
                                    mock_namespace = MagicMock()
                                    mock_namespace.metadata.name = "crate-system"
                                    mock_list_ns.return_value.items = [mock_namespace]
                                    
                                    # Execute discovery
                                    result = await activities.discover_clusters(discovery_input)
                                    
                                    assert result.total_found == 1, f"Expected 1 cluster found, got {result.total_found}"
                                    assert len(result.clusters) == 1, f"Expected 1 cluster in result, got {len(result.clusters)}"
                                    
                                    cluster = result.clusters[0]
                                    assert cluster.name == "test-cluster-1", f"Expected name=test-cluster-1, got {cluster.name}"
                                    assert cluster.dc_util_timeout == 900, f"Expected dc_util_timeout=900 (from config), got {cluster.dc_util_timeout}"
                                    assert cluster.min_availability == "FULL", f"Expected min_availability=FULL (from config), got {cluster.min_availability}"
                                    
                                    logger.info("✅ Full ClusterDiscoveryInput integration test passed")
        
    finally:
        # Clean up
        Path(config_path).unlink()


async def test_invalid_maintenance_config_handling():
    """Test that invalid maintenance configuration is handled gracefully."""
    
    logger.info("🧪 Testing invalid maintenance config handling...")
    
    # Create mock CRD item
    mock_crd_item = {
        "metadata": {
            "name": "test-cluster",
            "namespace": "default"
        },
        "spec": {
            "cluster": {
                "name": "test-cluster"
            }
        },
        "status": {
            "health": "GREEN"
        }
    }
    
    # Create mock StatefulSet
    mock_sts = MagicMock()
    mock_sts.spec.replicas = 1
    mock_sts.spec.template.spec.containers = []
    
    # Create activities instance
    activities = CrateDBActivities()
    
    # Mock dependencies
    with patch.object(activities, '_find_statefulset', return_value=("test-sts", mock_sts)):
        with patch.object(activities, '_analyze_prestop_hook', return_value=(False, False, 720)):
            with patch.object(activities, '_find_pods', return_value=["test-pod-0"]):
                with patch.object(activities, '_extract_health_status', return_value="GREEN"):
                    
                    # Test with non-existent config file
                    cluster = await activities._process_crd_item(
                        mock_crd_item, 
                        ["test-cluster"], 
                        "/non/existent/config.toml"  # Invalid path
                    )
                    
                    # Should still work with defaults
                    assert cluster is not None, "Cluster should be discovered despite invalid config"
                    assert cluster.dc_util_timeout == 720, f"Expected fallback to default dc_util_timeout=720, got {cluster.dc_util_timeout}"
                    assert cluster.min_availability == "PRIMARIES", f"Expected fallback to default min_availability=PRIMARIES, got {cluster.min_availability}"
                    
                    logger.info("✅ Invalid maintenance config handling test passed")


async def main():
    """Run all integration tests."""
    logger.info("🧪 Running maintenance configuration integration tests...")
    logger.info("=" * 60)
    
    test_cases = [
        test_maintenance_config_loading,
        test_cluster_discovery_with_maintenance_config,
        test_cluster_discovery_without_maintenance_config,
        test_manual_decommission_uses_config_values,
        test_cluster_discovery_input_integration,
        test_invalid_maintenance_config_handling,
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
        logger.info("🎉 All maintenance configuration integration tests passed!")
        logger.info("\n✅ Verified features:")
        logger.info("   - Maintenance config loading from TOML files")
        logger.info("   - dc_util_timeout override from maintenance config")
        logger.info("   - min_availability override from maintenance config")
        logger.info("   - Default value fallback when config not available")
        logger.info("   - Manual decommission uses cluster config values")
        logger.info("   - Graceful handling of invalid config files")
        logger.info("   - Full integration through ClusterDiscoveryInput")
        return True
    else:
        logger.error("💥 Some maintenance configuration integration tests failed!")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)