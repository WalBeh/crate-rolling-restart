"""
Temporal activities for CrateDB Kubernetes operations.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import List, Optional

from kubernetes import client
from kubernetes.client.exceptions import ApiException
from temporalio import activity

from .kubeconfig import KubeConfigHandler
from .maintenance_windows import MaintenanceWindowChecker
from .models import (
    ClusterDiscoveryInput,
    ClusterDiscoveryResult,
    ClusterValidationInput,
    ClusterValidationResult,
    CrateDBCluster,
    HealthCheckInput,
    HealthCheckResult,
    MaintenanceWindowCheckInput,
    MaintenanceWindowCheckResult,
    PodRestartInput,
    PodRestartResult,
)


class CrateDBActivities:
    """Activities for CrateDB cluster operations."""

    def __init__(self):
        self.kube_client: Optional[client.ApiClient] = None
        self.apps_v1: Optional[client.AppsV1Api] = None
        self.core_v1: Optional[client.CoreV1Api] = None
        self.custom_api: Optional[client.CustomObjectsApi] = None

    def _ensure_kube_client(self, kubeconfig: Optional[str] = None, context: Optional[str] = None) -> None:
        """Ensure Kubernetes client is initialized."""
        if self.kube_client is None:
            kube_handler = KubeConfigHandler(kubeconfig)
            kube_handler.load_context(context)

            self.kube_client = client.ApiClient()
            self.apps_v1 = client.AppsV1Api(self.kube_client)
            self.core_v1 = client.CoreV1Api(self.kube_client)
            self.custom_api = client.CustomObjectsApi(self.kube_client)

    @activity.defn
    async def discover_clusters(self, input_data: ClusterDiscoveryInput) -> ClusterDiscoveryResult:
        """
        Discover CrateDB clusters in the Kubernetes cluster.

        Args:
            input_data: Discovery parameters

        Returns:
            ClusterDiscoveryResult with found clusters
        """
        activity.logger.info(f"Discovering CrateDB clusters: {input_data.cluster_names or 'all'}")

        try:
            self._ensure_kube_client(input_data.kubeconfig, input_data.context)

            # Get all CrateDB resources from all namespaces
            # CrateDB resources are namespaced, so we need to query all namespaces
            try:
                # Get all namespaces first
                namespaces = self.core_v1.list_namespace()
                all_crds = {"items": []}
                
                for namespace in namespaces.items:
                    try:
                        crds = self.custom_api.list_namespaced_custom_object(
                            group="cloud.crate.io",
                            version="v1",
                            namespace=namespace.metadata.name,
                            plural="cratedbs",
                        )
                        all_crds["items"].extend(crds.get("items", []))
                    except ApiException as ns_e:
                        if ns_e.status != 404:  # Ignore 404s for individual namespaces
                            activity.logger.warning(f"Error querying namespace {namespace.metadata.name}: {ns_e}")
                
                crds = all_crds
                
            except ApiException as e:
                if e.status == 404:
                    error_msg = "CrateDB CRD not found. Is the CrateDB operator installed?"
                    activity.logger.error(error_msg)
                    return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])
                raise

            clusters = []
            errors = []

            for item in crds.get("items", []):
                try:
                    cluster = await self._process_crd_item(item, input_data.cluster_names)
                    if cluster:
                        clusters.append(cluster)
                except Exception as e:
                    error_msg = f"Error processing CRD {item.get('metadata', {}).get('name', 'unknown')}: {e}"
                    activity.logger.error(error_msg)
                    errors.append(error_msg)

            activity.logger.info(f"Found {len(clusters)} CrateDB clusters")
            return ClusterDiscoveryResult(clusters=clusters, total_found=len(clusters), errors=errors)

        except Exception as e:
            error_msg = f"Error discovering clusters: {e}"
            activity.logger.error(error_msg)
            return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])

    async def _process_crd_item(self, item: dict, filter_names: Optional[List[str]]) -> Optional[CrateDBCluster]:
        """Process a single CRD item and return CrateDBCluster if it matches criteria."""
        crd_name = item["metadata"]["name"]
        namespace = item["metadata"]["namespace"]

        # Get cluster name from spec or use CRD name
        if "spec" in item and "cluster" in item["spec"] and "name" in item["spec"]["cluster"]:
            cluster_name = item["spec"]["cluster"]["name"]
        else:
            cluster_name = crd_name

        # Skip if not in specified cluster names
        if filter_names and cluster_name not in filter_names:
            return None

        # Find corresponding StatefulSet
        sts_name, sts = await self._find_statefulset(crd_name, cluster_name, namespace)
        if not sts:
            raise ValueError(f"Could not find StatefulSet for cluster {cluster_name}")

        # Get cluster health
        health = self._extract_health_status(item)

        # Check for prestop hook and decommissioning utility
        has_prestop_hook, has_dc_util, dc_util_timeout = self._analyze_prestop_hook(sts, cluster_name)

        # Find pods
        pods = await self._find_pods(namespace, sts_name, crd_name, cluster_name)

        return CrateDBCluster(
            name=cluster_name,
            namespace=namespace,
            statefulset_name=sts_name,
            health=health,
            replicas=sts.spec.replicas,
            pods=pods,
            has_prestop_hook=has_prestop_hook,
            has_dc_util=has_dc_util,
            suspended=sts.spec.replicas == 0,
            crd_name=crd_name,
            dc_util_timeout=dc_util_timeout,
        )

    async def _find_statefulset(self, crd_name: str, cluster_name: str, namespace: str):
        """Find the StatefulSet for a CrateDB cluster."""
        possible_patterns = [
            f"crate-data-hot-{crd_name}",
            f"crate-{crd_name}",
            f"{crd_name}",
            f"crate-{cluster_name}",
            f"crate-data-hot-{cluster_name}",
        ]

        for pattern in possible_patterns:
            try:
                sts = self.apps_v1.read_namespaced_stateful_set(name=pattern, namespace=namespace)
                return pattern, sts
            except ApiException as e:
                if e.status != 404:
                    raise

        return None, None

    def _extract_health_status(self, item: dict) -> str:
        """Extract health status from CRD item."""
        try:
            if "status" in item:
                if "crateDBStatus" in item["status"]:
                    return item["status"]["crateDBStatus"].get("health", "UNKNOWN")
                else:
                    return item["status"].get("health", "UNKNOWN")
        except Exception:
            pass
        return "UNKNOWN"

    def _analyze_prestop_hook(self, sts, cluster_name: str) -> tuple[bool, bool, int]:
        """Analyze prestop hook configuration."""
        has_prestop_hook = False
        has_dc_util = False
        dc_util_timeout = 720

        if not sts.spec.template.spec.containers:
            return has_prestop_hook, has_dc_util, dc_util_timeout

        for container in sts.spec.template.spec.containers:
            if container.name == "crate" and container.lifecycle and container.lifecycle.pre_stop:
                has_prestop_hook = True

                try:
                    # Check for decommissioning utility
                    exec_attr = getattr(container.lifecycle.pre_stop, "exec", None) or \
                               getattr(container.lifecycle.pre_stop, "_exec", None)

                    if exec_attr:
                        cmd = getattr(exec_attr, "command", None) or \
                              getattr(exec_attr, "_command", None)

                        if cmd:
                            shell_command = self._extract_shell_command(cmd)
                            has_dc_util, dc_util_timeout = self._check_decommission_utility(shell_command, cluster_name)

                except Exception as e:
                    activity.logger.warning(f"Error analyzing prestop hook for {cluster_name}: {e}")

        return has_prestop_hook, has_dc_util, dc_util_timeout

    def _extract_shell_command(self, cmd: List[str]) -> str:
        """Extract shell command from command array."""
        if len(cmd) >= 3 and cmd[0] in ["/bin/sh", "/bin/bash"] and cmd[1] == "-c":
            return str(cmd[2]) if cmd[2] is not None else ""
        return " ".join(str(c) for c in cmd if c is not None)

    def _check_decommission_utility(self, shell_command: str, cluster_name: str) -> tuple[bool, int]:
        """Check for decommissioning utility and extract timeout."""
        decomm_patterns = ["dc_util", "dc-util", "dcutil", "decommission", "decomm", "/dc_util-", "/dc-util-"]

        if not any(pattern in shell_command for pattern in decomm_patterns):
            return False, 720

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
                unit = match.group(2)

                if unit == "m":
                    timeout = value * 60
                elif unit == "h":
                    timeout = value * 3600
                else:
                    timeout = value

                activity.logger.debug(f"Extracted dc_util timeout for {cluster_name}: {timeout}s")
                return True, timeout

        return True, 720

    async def _find_pods(self, namespace: str, sts_name: str, crd_name: str, cluster_name: str) -> List[str]:
        """Find pods for a CrateDB cluster."""
        possible_selectors = [
            f"app=crate,crate-cluster={crd_name}",
            f"app=crate,crate-cluster={cluster_name}",
            f"app=crate,statefulset={sts_name}",
            "app=crate",
        ]

        for selector in possible_selectors:
            try:
                pod_list = self.core_v1.list_namespaced_pod(namespace=namespace, label_selector=selector)
                if pod_list.items:
                    return [pod.metadata.name for pod in pod_list.items]
            except ApiException:
                continue

        # Try owner reference lookup
        try:
            all_pods = self.core_v1.list_namespaced_pod(namespace=namespace)
            pods = []
            for pod in all_pods.items:
                for owner_ref in pod.metadata.owner_references or []:
                    if owner_ref.kind == "StatefulSet" and owner_ref.name == sts_name:
                        pods.append(pod.metadata.name)
            return pods
        except ApiException:
            return []

    @activity.defn
    async def validate_cluster(self, input_data: ClusterValidationInput) -> ClusterValidationResult:
        """
        Validate if a cluster is ready for restart.

        Args:
            input_data: Validation parameters

        Returns:
            ClusterValidationResult with validation status
        """
        cluster = input_data.cluster
        warnings = []
        errors = []
        is_valid = True

        if cluster.suspended:
            errors.append("Cluster is SUSPENDED")
            is_valid = False

        if cluster.health != "GREEN":
            warnings.append(f"Cluster health is {cluster.health}, not GREEN")

        if not input_data.skip_hook_warning:
            if not cluster.has_prestop_hook:
                warnings.append("No prestop hook detected")
            elif not cluster.has_dc_util:
                warnings.append("Prestop hook detected but no decommissioning utility found")

        return ClusterValidationResult(
            cluster_name=cluster.name,
            is_valid=is_valid,
            warnings=warnings,
            errors=errors,
        )

    @activity.defn
    async def restart_pod(self, input_data: PodRestartInput) -> PodRestartResult:
        """
        Restart a single pod.

        Args:
            input_data: Pod restart parameters

        Returns:
            PodRestartResult with restart status
        """
        start_time = time.time()
        # Use time.time() for timestamps to avoid datetime.now() issues
        started_at = None

        try:
            self._ensure_kube_client()

            if input_data.dry_run:
                activity.logger.info(f"[DRY RUN] Would restart pod {input_data.pod_name}")
                await asyncio.sleep(5)  # Simulate restart time
                return PodRestartResult(
                    pod_name=input_data.pod_name,
                    namespace=input_data.namespace,
                    success=True,
                    duration=5.0,
                    started_at=started_at,
                    completed_at=None,
                )

            activity.logger.info(f"Restarting pod {input_data.pod_name}")

            # Delete the pod
            self.core_v1.delete_namespaced_pod(
                name=input_data.pod_name,
                namespace=input_data.namespace
            )

            # Wait for pod to be ready
            await self._wait_for_pod_ready(
                input_data.pod_name,
                input_data.namespace,
                input_data.pod_ready_timeout
            )

            duration = time.time() - start_time
            activity.logger.info(f"Successfully restarted pod {input_data.pod_name} in {duration:.2f}s")

            return PodRestartResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                success=True,
                duration=duration,
                started_at=started_at,
                completed_at=None,
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed to restart pod {input_data.pod_name}: {e}"
            activity.logger.error(error_msg)

            return PodRestartResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                success=False,
                duration=duration,
                error=error_msg,
                started_at=started_at,
                completed_at=None,
            )

    async def _wait_for_pod_ready(self, pod_name: str, namespace: str, timeout: int) -> None:
        """Wait for a pod to be ready."""
        start_time = time.time()
        pod_ready_time = None
        min_ready_duration = 20  # Minimum time pod should be ready

        while time.time() - start_time < timeout:
            try:
                pod = self.core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)

                if pod.status.phase == "Running":
                    ready = True
                    for condition in pod.status.conditions or []:
                        if condition.type == "Ready" and condition.status != "True":
                            ready = False
                            break

                    if ready:
                        current_time = time.time()
                        if pod_ready_time is None:
                            pod_ready_time = current_time
                            activity.logger.info(f"Pod {pod_name} is ready, waiting for stability...")
                        elif current_time - pod_ready_time >= min_ready_duration:
                            activity.logger.info(f"Pod {pod_name} has been stable for {current_time - pod_ready_time:.1f}s")
                            return
                    else:
                        pod_ready_time = None

            except ApiException as e:
                activity.logger.warning(f"Error checking pod status: {e}")
                pod_ready_time = None

            await asyncio.sleep(10)

        raise TimeoutError(f"Pod {pod_name} did not become ready within {timeout} seconds")

    @activity.defn
    async def check_cluster_health(self, input_data: HealthCheckInput) -> HealthCheckResult:
        """
        Check the health of a CrateDB cluster.

        Args:
            input_data: Health check parameters

        Returns:
            HealthCheckResult with health status
        """
        cluster = input_data.cluster
        # Use None for timestamps to avoid datetime.now() issues
        checked_at = None

        try:
            self._ensure_kube_client()

            if input_data.dry_run:
                activity.logger.info(f"[DRY RUN] Would check health of cluster {cluster.name}")
                return HealthCheckResult(
                    cluster_name=cluster.name,
                    health_status="GREEN",
                    is_healthy=True,
                    checked_at=checked_at,
                )

            # Get fresh cluster CRD information
            crd = self.custom_api.get_namespaced_custom_object(
                group="cloud.crate.io",
                version="v1",
                namespace=cluster.namespace,
                plural="cratedbs",
                name=cluster.crd_name,
            )

            health = self._extract_health_status(crd)
            
            activity.logger.info(f"Cluster {cluster.name} health: {health}")
            
            # Handle different health statuses
            if health == "GREEN":
                return HealthCheckResult(
                    cluster_name=cluster.name,
                    health_status=health,
                    is_healthy=True,
                    checked_at=checked_at,
                )
            elif health in ["UNREACHABLE", "YELLOW", "RED"]:
                # These are temporary states that should trigger retries
                raise Exception(f"Cluster {cluster.name} health is {health}, retrying...")
            else:
                # UNKNOWN or other statuses are permanent failures
                return HealthCheckResult(
                    cluster_name=cluster.name,
                    health_status=health,
                    is_healthy=False,
                    checked_at=checked_at,
                )

        except Exception as e:
            error_msg = f"Error checking cluster health: {e}"
            activity.logger.error(error_msg)

            return HealthCheckResult(
                cluster_name=cluster.name,
                health_status="UNKNOWN",
                is_healthy=False,
                checked_at=checked_at,
                error=error_msg,
            )

    @activity.defn
    async def check_maintenance_window(self, input_data: MaintenanceWindowCheckInput) -> MaintenanceWindowCheckResult:
        """
        Check if cluster restart should proceed based on maintenance windows.
        
        Args:
            input_data: Maintenance window check input
            
        Returns:
            MaintenanceWindowCheckResult with decision and reasoning
        """
        current_time = input_data.current_time or datetime.now(timezone.utc)
        
        # Ensure current_time is timezone-aware (defensive programming for Temporal serialization)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)
        
        activity.logger.info(f"Checking maintenance window for cluster {input_data.cluster_name} at {current_time}")
        
        try:
            if not input_data.config_path:
                # No maintenance config provided - proceed without restrictions
                return MaintenanceWindowCheckResult(
                    cluster_name=input_data.cluster_name,
                    should_wait=False,
                    reason="No maintenance configuration path provided - proceeding without restrictions",
                    current_time=current_time,
                    in_maintenance_window=False
                )
            
            # Initialize maintenance window checker
            checker = MaintenanceWindowChecker(input_data.config_path)
            
            # Check if currently in maintenance window
            in_window, window_reason = checker.is_in_maintenance_window(
                input_data.cluster_name, 
                current_time
            )
            
            # Make decision about whether to wait
            should_wait, decision_reason = checker.should_wait_for_maintenance_window(
                input_data.cluster_name,
                current_time
            )
            
            # Get next window start time for additional context
            next_window_start, _ = checker.get_next_maintenance_window(
                input_data.cluster_name,
                current_time
            )
            
            # Log with appropriate level based on decision
            if should_wait and not in_window:
                activity.logger.warning(f"Cluster {input_data.cluster_name} is OUTSIDE maintenance window - "
                                      f"restart will be delayed: {decision_reason}")
            elif in_window:
                activity.logger.info(f"Cluster {input_data.cluster_name} is INSIDE maintenance window - "
                                   f"restart can proceed: {decision_reason}")
            else:
                activity.logger.info(f"Maintenance window check for {input_data.cluster_name}: "
                                   f"should_wait={should_wait}, in_window={in_window}, reason={decision_reason}")
            
            return MaintenanceWindowCheckResult(
                cluster_name=input_data.cluster_name,
                should_wait=should_wait,
                reason=decision_reason,
                next_window_start=next_window_start,
                current_time=current_time,
                in_maintenance_window=in_window
            )
            
        except FileNotFoundError as e:
            error_msg = f"Maintenance configuration file not found: {e}"
            activity.logger.warning(error_msg)
            
            return MaintenanceWindowCheckResult(
                cluster_name=input_data.cluster_name,
                should_wait=False,
                reason=f"Maintenance config file not found - proceeding without restrictions: {e}",
                current_time=current_time,
                in_maintenance_window=False
            )
            
        except Exception as e:
            error_msg = f"Error checking maintenance window: {e}"
            activity.logger.error(error_msg)
            
            # On error, proceed with restart to avoid blocking operations
            return MaintenanceWindowCheckResult(
                cluster_name=input_data.cluster_name,
                should_wait=False,
                reason=f"Error checking maintenance windows - proceeding with restart: {e}",
                current_time=current_time,
                in_maintenance_window=False
            )
