#!/usr/bin/env python3
"""
Demo script showing the suspended nodes feature in action.

This script demonstrates how to use the --only-on-suspended-nodes flag
and shows the different behaviors with various node states.
"""

import asyncio
import sys
from unittest.mock import Mock, patch
from datetime import datetime

# Add the parent directory to the path so we can import rr modules
sys.path.insert(0, '.')

from rr.activities import CrateDBActivities
from rr.models import CrateDBCluster, RestartOptions
from kubernetes.client import V1Node, V1NodeSpec, V1Pod, V1PodSpec, V1ObjectMeta, V1Taint


def create_demo_cluster():
    """Create a demo cluster with multiple pods."""
    return CrateDBCluster(
        name="demo-cluster",
        namespace="cratedb-demo",
        statefulset_name="demo-cluster-sts",
        health="GREEN",
        replicas=4,
        pods=["demo-cluster-0", "demo-cluster-1", "demo-cluster-2", "demo-cluster-3"],
        has_prestop_hook=True,
        has_dc_util=True,
        suspended=False,
        crd_name="demo-cluster-crd",
        dc_util_timeout=720,
        min_availability="PRIMARIES"
    )


def create_mock_pod(pod_name: str, node_name: str):
    """Create a mock pod object."""
    pod = Mock(spec=V1Pod)
    pod.metadata = Mock(spec=V1ObjectMeta)
    pod.metadata.name = pod_name
    pod.spec = Mock(spec=V1PodSpec)
    pod.spec.node_name = node_name
    return pod


def create_mock_node(node_name: str, suspended: bool = False, suspension_type: str = "unschedulable"):
    """Create a mock node object."""
    node = Mock(spec=V1Node)
    node.metadata = Mock(spec=V1ObjectMeta)
    node.metadata.name = node_name
    node.metadata.annotations = {}
    node.spec = Mock(spec=V1NodeSpec)
    node.spec.taints = []
    node.spec.unschedulable = False
    
    if suspended:
        if suspension_type == "unschedulable":
            node.spec.unschedulable = True
        elif suspension_type == "spot_terminating":
            taint = Mock(spec=V1Taint)
            taint.key = "aws.amazon.com/spot-instance-terminating"
            taint.value = "true"
            taint.effect = "NoSchedule"
            node.spec.taints = [taint]
        elif suspension_type == "annotation":
            node.metadata.annotations = {"node.kubernetes.io/suspend": "true"}
        elif suspension_type == "autoscaler":
            node.metadata.annotations = {"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true"}
    
    return node


async def demo_node_detection():
    """Demonstrate node suspension detection."""
    print("=" * 80)
    print("🔍 DEMO: Node Suspension Detection")
    print("=" * 80)
    
    activities = CrateDBActivities()
    
    # Create demo scenarios
    scenarios = [
        ("demo-cluster-0", "worker-1", False, "active"),
        ("demo-cluster-1", "worker-2", True, "unschedulable"),
        ("demo-cluster-2", "worker-3", True, "spot_terminating"),
        ("demo-cluster-3", "worker-4", True, "annotation"),
    ]
    
    with patch.object(activities, '_ensure_kube_client'):
        with patch.object(activities, 'core_v1') as mock_core_v1:
            
            def mock_read_pod(name, namespace):
                for pod_name, node_name, _, _ in scenarios:
                    if pod_name == name:
                        return create_mock_pod(pod_name, node_name)
                return create_mock_pod(name, "unknown-node")
            
            def mock_read_node(name):
                for pod_name, node_name, suspended, suspension_type in scenarios:
                    if node_name == name:
                        return create_mock_node(node_name, suspended, suspension_type)
                return create_mock_node(name, False)
            
            mock_core_v1.read_namespaced_pod.side_effect = mock_read_pod
            mock_core_v1.read_node.side_effect = mock_read_node
            
            print("Checking pod-to-node mappings and suspension status:\n")
            
            for pod_name, node_name, expected_suspended, suspension_type in scenarios:
                print(f"Pod: {pod_name}")
                print(f"  └── Node: {node_name}")
                print(f"  └── Expected: {'SUSPENDED' if expected_suspended else 'ACTIVE'}")
                
                result = await activities.is_pod_on_suspended_node(pod_name, "cratedb-demo")
                
                status = "✅ SUSPENDED" if result else "❌ ACTIVE"
                print(f"  └── Detected: {status}")
                
                if expected_suspended and suspension_type != "active":
                    print(f"  └── Reason: {suspension_type}")
                
                print()


async def demo_restart_behavior():
    """Demonstrate restart behavior with suspended nodes flag."""
    print("=" * 80)
    print("🔄 DEMO: Restart Behavior Comparison")
    print("=" * 80)
    
    cluster = create_demo_cluster()
    
    print("Cluster Configuration:")
    print(f"  Name: {cluster.name}")
    print(f"  Namespace: {cluster.namespace}")
    print(f"  Pods: {', '.join(cluster.pods)}")
    print()
    
    # Scenario 1: Normal restart (all pods)
    print("📋 Scenario 1: Normal Restart (--only-on-suspended-nodes=False)")
    print("  Would restart ALL pods:")
    for pod in cluster.pods:
        print(f"    ✓ {pod}")
    print()
    
    # Scenario 2: Suspended nodes only
    print("📋 Scenario 2: Suspended Nodes Only (--only-on-suspended-nodes=True)")
    print("  Would restart ONLY pods on suspended nodes:")
    
    suspended_pods = ["demo-cluster-1", "demo-cluster-2", "demo-cluster-3"]
    active_pods = ["demo-cluster-0"]
    
    for pod in suspended_pods:
        print(f"    ✓ {pod} (on suspended node)")
    
    print("  Would SKIP pods on active nodes:")
    for pod in active_pods:
        print(f"    ⏭️  {pod} (on active node)")
    print()


def demo_cli_usage():
    """Demonstrate CLI usage examples."""
    print("=" * 80)
    print("💻 DEMO: CLI Usage Examples")
    print("=" * 80)
    
    examples = [
        {
            "title": "Basic Usage",
            "command": "rr restart --context prod --only-on-suspended-nodes cluster1",
            "description": "Restart pods on suspended nodes only"
        },
        {
            "title": "Dry Run",
            "command": "rr restart --context prod --only-on-suspended-nodes --dry-run cluster1",
            "description": "Preview which pods would be restarted"
        },
        {
            "title": "All Clusters",
            "command": "rr restart --context prod --only-on-suspended-nodes all",
            "description": "Apply to all clusters (with confirmation)"
        },
        {
            "title": "Asynchronous",
            "command": "rr restart --context prod --only-on-suspended-nodes --async cluster1",
            "description": "Start workflow asynchronously"
        },
        {
            "title": "With Maintenance Windows",
            "command": "rr restart --context prod --only-on-suspended-nodes --maintenance-config windows.toml cluster1",
            "description": "Respect maintenance windows"
        },
        {
            "title": "Debug Mode",
            "command": "rr restart --context prod --only-on-suspended-nodes --log-level DEBUG cluster1",
            "description": "Enable detailed logging"
        }
    ]
    
    for example in examples:
        print(f"📝 {example['title']}:")
        print(f"   Command: {example['command']}")
        print(f"   Purpose: {example['description']}")
        print()


def demo_options_model():
    """Demonstrate the RestartOptions model."""
    print("=" * 80)
    print("⚙️  DEMO: Configuration Options")
    print("=" * 80)
    
    # Default options
    default_options = RestartOptions(context="prod")
    print("Default Options:")
    print(f"  only_on_suspended_nodes: {default_options.only_on_suspended_nodes}")
    print(f"  dry_run: {default_options.dry_run}")
    print(f"  context: {default_options.context}")
    print()
    
    # Suspended nodes enabled
    suspended_options = RestartOptions(
        context="prod",
        only_on_suspended_nodes=True,
        dry_run=True,
        log_level="DEBUG"
    )
    print("Suspended Nodes Options:")
    print(f"  only_on_suspended_nodes: {suspended_options.only_on_suspended_nodes}")
    print(f"  dry_run: {suspended_options.dry_run}")
    print(f"  log_level: {suspended_options.log_level}")
    print()
    
    # Serialization demo
    options_dict = suspended_options.model_dump()
    print("Serialized Options:")
    for key, value in options_dict.items():
        if key in ['only_on_suspended_nodes', 'dry_run', 'context', 'log_level']:
            print(f"  {key}: {value}")
    print()


def demo_real_world_scenarios():
    """Demonstrate real-world scenarios."""
    print("=" * 80)
    print("🌍 DEMO: Real-World Scenarios")
    print("=" * 80)
    
    scenarios = [
        {
            "name": "AWS Spot Instance Termination",
            "description": "Handle spot instance termination gracefully",
            "detection": "aws.amazon.com/spot-instance-terminating taint",
            "command": "rr restart --context prod --only-on-suspended-nodes --async all",
            "benefit": "Proactively restart pods before instance termination"
        },
        {
            "name": "Planned Node Maintenance",
            "description": "Coordinate with cluster maintenance",
            "detection": "node.kubernetes.io/unschedulable flag",
            "command": "rr restart --context prod --only-on-suspended-nodes --maintenance-config maintenance.toml cluster1",
            "benefit": "Respect maintenance windows while handling node drainage"
        },
        {
            "name": "Cluster Autoscaler Scale-Down",
            "description": "Work with cluster autoscaler",
            "detection": "cluster-autoscaler.kubernetes.io/scale-down-disabled annotation",
            "command": "rr restart --context prod --only-on-suspended-nodes --dry-run all",
            "benefit": "Preview impact before actual scale-down"
        },
        {
            "name": "Emergency Node Evacuation",
            "description": "Quickly evacuate failing nodes",
            "detection": "node.kubernetes.io/suspend annotation",
            "command": "rr restart --context prod --only-on-suspended-nodes --ignore-maintenance-windows cluster1",
            "benefit": "Bypass maintenance windows for emergency situations"
        }
    ]
    
    for i, scenario in enumerate(scenarios, 1):
        print(f"📊 Scenario {i}: {scenario['name']}")
        print(f"   Description: {scenario['description']}")
        print(f"   Detection: {scenario['detection']}")
        print(f"   Command: {scenario['command']}")
        print(f"   Benefit: {scenario['benefit']}")
        print()


async def main():
    """Run all demonstrations."""
    print("🚀 CrateDB Suspended Nodes Feature Demo")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        await demo_node_detection()
        await demo_restart_behavior()
        demo_cli_usage()
        demo_options_model()
        demo_real_world_scenarios()
        
        print("=" * 80)
        print("✅ Demo completed successfully!")
        print("=" * 80)
        print()
        print("Next Steps:")
        print("1. ⚠️  RESTART THE TEMPORAL WORKER to register the new activity")
        print("2. Try the CLI commands with your own clusters")
        print("3. Test with --dry-run first to see the behavior")
        print("4. Monitor logs to understand pod selection")
        print("5. Set up appropriate RBAC permissions for node access")
        print()
        print("⚠️  Important: The worker must be restarted after upgrading to use this feature.")
        print("    Otherwise you'll get: 'Activity function is_pod_on_suspended_node is not registered'")
        print()
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))