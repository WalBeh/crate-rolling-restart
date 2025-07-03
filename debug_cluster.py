#!/usr/bin/env python3
"""
Debug script to inspect CrateDB cluster configuration and understand
why prestop hook detection or health checks are failing.

Usage:
    python debug_cluster.py --cluster-name aqua-darth-vader --namespace default
"""

import argparse
import json
import sys
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


def load_kube_config():
    """Load Kubernetes configuration."""
    try:
        config.load_incluster_config()
        print("✅ Loaded in-cluster config")
    except Exception:
        try:
            config.load_kube_config()
            print("✅ Loaded kubeconfig")
        except Exception as e:
            print(f"❌ Failed to load kubeconfig: {e}")
            sys.exit(1)


def find_statefulset(apps_v1, cluster_name, namespace):
    """Find the StatefulSet for a CrateDB cluster."""
    possible_patterns = [
        f"crate-data-hot-{cluster_name}",
        f"crate-{cluster_name}",
        f"{cluster_name}",
    ]

    print(f"\n🔍 Searching for StatefulSet with patterns: {possible_patterns}")
    
    for pattern in possible_patterns:
        try:
            sts = apps_v1.read_namespaced_stateful_set(name=pattern, namespace=namespace)
            print(f"✅ Found StatefulSet: {pattern}")
            return pattern, sts
        except ApiException as e:
            if e.status == 404:
                print(f"   - {pattern}: Not found")
            else:
                print(f"   - {pattern}: Error - {e}")
    
    # List all StatefulSets to help debug
    try:
        sts_list = apps_v1.list_namespaced_stateful_set(namespace=namespace)
        print(f"\n📋 Available StatefulSets in namespace {namespace}:")
        for sts in sts_list.items:
            print(f"   - {sts.metadata.name}")
    except Exception as e:
        print(f"❌ Failed to list StatefulSets: {e}")
    
    return None, None


def analyze_prestop_hook(sts, cluster_name):
    """Analyze prestop hook configuration."""
    print(f"\n🔍 Analyzing preStop hook for {cluster_name}")
    
    if not sts.spec.template.spec.containers:
        print("❌ No containers found in StatefulSet")
        return False, False, 720
    
    print(f"📦 Found {len(sts.spec.template.spec.containers)} containers:")
    
    has_prestop_hook = False
    has_dc_util = False
    dc_util_timeout = 720
    
    for container in sts.spec.template.spec.containers:
        print(f"   - Container: {container.name}")
        
        if container.name == "crate":
            print(f"     ✅ Found crate container")
            
            if container.lifecycle:
                print(f"     ✅ Has lifecycle configuration")
                
                if container.lifecycle.pre_stop:
                    print(f"     ✅ Has preStop hook")
                    has_prestop_hook = True
                    
                    # Check exec attribute
                    exec_attr = getattr(container.lifecycle.pre_stop, "exec", None)
                    print(f"     📝 preStop exec: {exec_attr}")
                    
                    if exec_attr and exec_attr.command:
                        print(f"     📝 preStop command: {exec_attr.command}")
                        
                        # Extract shell command
                        cmd = exec_attr.command
                        if len(cmd) >= 3 and cmd[0] in ["/bin/sh", "/bin/bash"] and cmd[1] == "-c":
                            shell_command = str(cmd[2])
                        else:
                            shell_command = " ".join(str(c) for c in cmd if c is not None)
                        
                        print(f"     📝 Shell command: {shell_command}")
                        
                        # Check for dc_util patterns
                        decomm_patterns = ["dc_util", "dc-util", "dcutil", "decommission", "decomm", "/dc_util-", "/dc-util-"]
                        found_patterns = [p for p in decomm_patterns if p in shell_command]
                        
                        if found_patterns:
                            has_dc_util = True
                            print(f"     ✅ Found decommission patterns: {found_patterns}")
                            
                            # Extract timeout
                            import re
                            timeout_patterns = [
                                r"(?:--|-)(?:timeout|t)\s*(?:=|\s+)(\d+)([smh]?)",
                                r"timeout\s+(\d+)([smh]?)",
                                r"-min-availability\s+\w+\s+-timeout\s+(\d+)([smh]?)",
                            ]
                            
                            for pattern in timeout_patterns:
                                match = re.search(pattern, shell_command)
                                if match:
                                    value = int(match.group(1))
                                    unit = match.group(2) if len(match.groups()) > 1 else ""
                                    
                                    if unit == "m":
                                        timeout = value * 60
                                    elif unit == "h":
                                        timeout = value * 3600
                                    else:
                                        timeout = value
                                    
                                    dc_util_timeout = timeout
                                    print(f"     ⏱️  Extracted timeout: {timeout}s")
                                    break
                        else:
                            print(f"     ❌ No decommission patterns found")
                    else:
                        print(f"     ❌ No command in preStop exec")
                else:
                    print(f"     ❌ No preStop hook")
            else:
                print(f"     ❌ No lifecycle configuration")
    
    print(f"\n📊 PreStop Analysis Results:")
    print(f"   - has_prestop_hook: {has_prestop_hook}")
    print(f"   - has_dc_util: {has_dc_util}")
    print(f"   - dc_util_timeout: {dc_util_timeout}")
    
    return has_prestop_hook, has_dc_util, dc_util_timeout


def find_cratedb_crd(custom_api, cluster_name, namespace):
    """Find the CrateDB CRD."""
    print(f"\n🔍 Searching for CrateDB CRD")
    
    possible_patterns = [
        cluster_name,
        f"crate-{cluster_name}",
        f"{cluster_name}-crate",
    ]
    
    for pattern in possible_patterns:
        try:
            crd = custom_api.get_namespaced_custom_object(
                group="cloud.crate.io",
                version="v1",
                namespace=namespace,
                plural="cratedbs",
                name=pattern,
            )
            print(f"✅ Found CrateDB CRD: {pattern}")
            return pattern, crd
        except ApiException as e:
            if e.status == 404:
                print(f"   - {pattern}: Not found")
            else:
                print(f"   - {pattern}: Error - {e}")
    
    # List all CrateDB CRDs
    try:
        crd_list = custom_api.list_namespaced_custom_object(
            group="cloud.crate.io",
            version="v1",
            namespace=namespace,
            plural="cratedbs"
        )
        print(f"\n📋 Available CrateDB CRDs in namespace {namespace}:")
        for item in crd_list.get("items", []):
            name = item.get("metadata", {}).get("name", "unknown")
            print(f"   - {name}")
    except Exception as e:
        print(f"❌ Failed to list CrateDB CRDs: {e}")
    
    return None, None


def analyze_cluster_health(crd):
    """Analyze cluster health from CRD."""
    print(f"\n🏥 Analyzing cluster health")
    
    if not crd:
        print("❌ No CRD available")
        return "UNKNOWN"
    
    status = crd.get("status", {})
    print(f"📝 CRD status: {json.dumps(status, indent=2)}")
    
    # Try different health extraction methods
    health_sources = [
        ("status.crateDBStatus.health", status.get("crateDBStatus", {}).get("health")),
        ("status.health", status.get("health")),
        ("spec.cluster.health", crd.get("spec", {}).get("cluster", {}).get("health")),
    ]
    
    print(f"\n🔍 Health sources:")
    for source, value in health_sources:
        print(f"   - {source}: {value}")
    
    # Use the first non-None value
    for source, health in health_sources:
        if health:
            print(f"✅ Using health from {source}: {health}")
            return health
    
    print(f"❌ No health information found")
    return "UNKNOWN"


def get_pods(core_v1, sts_name, namespace):
    """Get pods for the StatefulSet."""
    print(f"\n🔍 Finding pods for StatefulSet {sts_name}")
    
    try:
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"app=crate,statefulset={sts_name}"
        )
        
        if not pods.items:
            # Try alternative selectors
            selectors = [
                "app=crate",
                f"app=crate,crate-cluster={sts_name}",
            ]
            
            for selector in selectors:
                pods = core_v1.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=selector
                )
                if pods.items:
                    print(f"✅ Found pods with selector: {selector}")
                    break
        
        if pods.items:
            print(f"📦 Found {len(pods.items)} pods:")
            for pod in pods.items:
                phase = pod.status.phase
                ready = "Unknown"
                if pod.status.container_statuses:
                    ready_count = sum(1 for c in pod.status.container_statuses if c.ready)
                    total_count = len(pod.status.container_statuses)
                    ready = f"{ready_count}/{total_count}"
                
                print(f"   - {pod.metadata.name}: {phase} (Ready: {ready})")
                
                # Check for recent restarts
                if pod.status.container_statuses:
                    for container in pod.status.container_statuses:
                        if container.restart_count > 0:
                            print(f"     ⚠️  Container {container.name}: {container.restart_count} restarts")
            
            return [pod.metadata.name for pod in pods.items]
        else:
            print(f"❌ No pods found")
            return []
            
    except Exception as e:
        print(f"❌ Failed to get pods: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Debug CrateDB cluster configuration")
    parser.add_argument("--cluster-name", required=True, help="Cluster name")
    parser.add_argument("--namespace", default="default", help="Namespace (default: default)")
    
    args = parser.parse_args()
    
    print(f"🔍 Debugging cluster: {args.cluster_name} in namespace: {args.namespace}")
    print("=" * 80)
    
    # Load Kubernetes config
    load_kube_config()
    
    # Initialize clients
    apps_v1 = client.AppsV1Api()
    custom_api = client.CustomObjectsApi()
    core_v1 = client.CoreV1Api()
    
    # Find StatefulSet
    sts_name, sts = find_statefulset(apps_v1, args.cluster_name, args.namespace)
    if not sts:
        print(f"❌ Could not find StatefulSet for cluster {args.cluster_name}")
        sys.exit(1)
    
    # Analyze preStop hook
    has_prestop_hook, has_dc_util, dc_util_timeout = analyze_prestop_hook(sts, args.cluster_name)
    
    # Find CRD
    crd_name, crd = find_cratedb_crd(custom_api, args.cluster_name, args.namespace)
    
    # Analyze health
    health = analyze_cluster_health(crd)
    
    # Get pods
    pods = get_pods(core_v1, sts_name, args.namespace)
    
    # Summary
    print(f"\n" + "=" * 80)
    print(f"📊 SUMMARY")
    print(f"=" * 80)
    print(f"Cluster Name: {args.cluster_name}")
    print(f"Namespace: {args.namespace}")
    print(f"StatefulSet: {sts_name}")
    print(f"CRD Name: {crd_name}")
    print(f"Health: {health}")
    print(f"Pods: {len(pods)} found")
    print(f"Has PreStop Hook: {has_prestop_hook}")
    print(f"Has DC Util: {has_dc_util}")
    print(f"DC Util Timeout: {dc_util_timeout}s")
    
    if has_dc_util:
        print(f"\n✅ This cluster SHOULD use Kubernetes-managed decommission")
        print(f"   - Pod deletion will trigger preStop hook")
        print(f"   - dc_util will handle decommission automatically")
        print(f"   - Use grace period: {dc_util_timeout + 60}s")
    else:
        print(f"\n⚠️  This cluster SHOULD use manual decommission")
        print(f"   - Execute decommission commands via API")
        print(f"   - Wait for process exit")
        print(f"   - Then delete pod")
    
    if health not in ["GREEN"]:
        print(f"\n⚠️  HEALTH ISSUE: Cluster health is {health}")
        print(f"   - Check pod status and logs")
        print(f"   - Verify cluster configuration")
        print(f"   - Health checks will fail until this is resolved")


if __name__ == "__main__":
    main()