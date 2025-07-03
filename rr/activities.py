"""
Temporal activities for CrateDB Kubernetes operations.
"""

import asyncio
import json
import random
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from kubernetes import client
from kubernetes.client.exceptions import ApiException
from kubernetes import stream
from temporalio import activity

from .kubeconfig import KubeConfigHandler
from .maintenance_windows import MaintenanceWindowChecker
from .models import (
    ClusterDiscoveryInput,
    ClusterDiscoveryResult,
    ClusterValidationInput,
    ClusterValidationResult,
    CrateDBCluster,
    DecommissionInput,
    DecommissionResult,
    HealthCheckInput,
    HealthCheckResult,
    MaintenanceWindowCheckInput,
    MaintenanceWindowCheckResult,
    PodRestartInput,
    PodRestartResult,
)
from .maintenance_windows import MaintenanceWindowChecker


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
            try:
                kube_handler = KubeConfigHandler(kubeconfig)
                kube_handler.load_context(context)

                self.kube_client = client.ApiClient()
                self.apps_v1 = client.AppsV1Api(self.kube_client)
                self.core_v1 = client.CoreV1Api(self.kube_client)
                self.custom_api = client.CustomObjectsApi(self.kube_client)
            except Exception as e:
                error_msg = f"Failed to initialize Kubernetes client: {e}"
                if "ExpiredToken" in str(e) or "security token" in str(e).lower():
                    error_msg += " - Your AWS security token appears to be expired. Please refresh your credentials."
                elif "Unauthorized" in str(e):
                    error_msg += " - Authentication failed. Please check your kubeconfig and credentials."
                activity.logger.error(error_msg)
                raise Exception(error_msg)

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
            # Initialize Kubernetes client with better error handling
            try:
                self._ensure_kube_client(input_data.kubeconfig, input_data.context)
            except Exception as init_error:
                error_msg = f"Failed to initialize Kubernetes client: {init_error}"
                activity.logger.error(error_msg)
                return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])

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
                        if ns_e.status == 401:
                            error_msg = f"Authentication failed for namespace {namespace.metadata.name}. Check your Kubernetes credentials (expired AWS token?)"
                            activity.logger.error(error_msg)
                            return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])
                        elif ns_e.status != 404:  # Ignore 404s for individual namespaces
                            activity.logger.warning(f"Error querying namespace {namespace.metadata.name}: {ns_e}")

                crds = all_crds

            except ApiException as e:
                if e.status == 401:
                    error_msg = "Kubernetes authentication failed. Check your credentials and ensure AWS token is not expired."
                    activity.logger.error(error_msg)
                    return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])
                elif e.status == 404:
                    error_msg = "CrateDB CRD not found. Is the CrateDB operator installed?"
                    activity.logger.error(error_msg)
                    return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])
                raise

            clusters = []
            errors = []

            for item in crds.get("items", []):
                try:
                    cluster = await self._process_crd_item(item, input_data.cluster_names, input_data.maintenance_config_path)
                    if cluster:
                        clusters.append(cluster)
                except Exception as e:
                    error_msg = f"Error processing CRD {item.get('metadata', {}).get('name', 'unknown')}: {e}"
                    activity.logger.error(error_msg)
                    errors.append(error_msg)

            activity.logger.info(f"Found {len(clusters)} CrateDB clusters")
            return ClusterDiscoveryResult(clusters=clusters, total_found=len(clusters), errors=errors)

        except ApiException as e:
            if e.status == 401:
                error_msg = "Kubernetes authentication failed. Check your credentials and ensure AWS token is not expired."
            else:
                error_msg = f"Kubernetes API error: {e}"
            activity.logger.error(error_msg)
            return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])
        except Exception as e:
            error_msg = f"Error discovering clusters: {e}"
            activity.logger.error(error_msg)
            return ClusterDiscoveryResult(clusters=[], total_found=0, errors=[error_msg])

    async def _process_crd_item(self, item: dict, filter_names: Optional[List[str]], maintenance_config_path: Optional[str] = None) -> Optional[CrateDBCluster]:
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

        # Default values
        min_availability = "PRIMARIES"

        # Apply maintenance configuration overrides if available
        if maintenance_config_path:
            try:
                checker = MaintenanceWindowChecker(maintenance_config_path)
                config = checker.get_cluster_config(cluster_name)
                if config:
                    dc_util_timeout = config.dc_util_timeout
                    min_availability = config.min_availability
                    activity.logger.info(f"Applied maintenance config for {cluster_name}: dc_util_timeout={dc_util_timeout}, min_availability={min_availability}")
            except Exception as e:
                activity.logger.warning(f"Failed to load maintenance config for {cluster_name}: {e}")

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
            min_availability=min_availability,
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

        activity.logger.debug(f"Analyzing prestop hook for {cluster_name}")
        activity.logger.debug(f"StatefulSet has {len(sts.spec.template.spec.containers) if sts.spec.template.spec.containers else 0} containers")

        if not sts.spec.template.spec.containers:
            activity.logger.debug(f"No containers found in StatefulSet {cluster_name}")
            return has_prestop_hook, has_dc_util, dc_util_timeout

        for container in sts.spec.template.spec.containers:
            activity.logger.debug(f"Checking container: {container.name}")

            if container.name == "crate":
                activity.logger.debug(f"Found crate container for {cluster_name}")

                if container.lifecycle:
                    activity.logger.debug(f"Container has lifecycle configuration")

                    if container.lifecycle.pre_stop:
                        activity.logger.debug(f"Container has preStop hook")
                        has_prestop_hook = True

                        try:
                            # Check for decommissioning utility
                            exec_attr = getattr(container.lifecycle.pre_stop, "exec", None) or \
                                       getattr(container.lifecycle.pre_stop, "_exec", None)

                            activity.logger.debug(f"preStop exec attribute: {exec_attr}")

                            if exec_attr:
                                cmd = getattr(exec_attr, "command", None) or \
                                      getattr(exec_attr, "_command", None)

                                activity.logger.debug(f"preStop command: {cmd}")

                                if cmd:
                                    shell_command = self._extract_shell_command(cmd)
                                    activity.logger.debug(f"Extracted shell command: {shell_command}")
                                    has_dc_util, dc_util_timeout = self._check_decommission_utility(shell_command, cluster_name)
                                    activity.logger.debug(f"dc_util detection result: has_dc_util={has_dc_util}, timeout={dc_util_timeout}")
                                else:
                                    activity.logger.debug(f"No command found in preStop exec")
                            else:
                                activity.logger.debug(f"No exec attribute found in preStop hook")

                        except Exception as e:
                            activity.logger.warning(f"Error analyzing prestop hook for {cluster_name}: {e}")
                    else:
                        activity.logger.debug(f"Container has lifecycle but no preStop hook")
                else:
                    activity.logger.debug(f"Container has no lifecycle configuration")
            else:
                activity.logger.debug(f"Skipping non-crate container: {container.name}")

        activity.logger.info(f"Final prestop analysis for {cluster_name}: has_prestop_hook={has_prestop_hook}, has_dc_util={has_dc_util}, timeout={dc_util_timeout}")
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
        Restart a single pod - LEGACY METHOD for backward compatibility.
        
        This method is kept for backward compatibility but new code should use
        the state machine approach via PodRestartStateMachine.

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

            activity.logger.info(f"Starting legacy pod restart for {input_data.pod_name}")

            # CRITICAL: Validate cluster health before proceeding with pod restart
            # This prevents pods from being deleted when cluster is not GREEN
            activity.logger.info(f"Validating cluster health before restarting pod {input_data.pod_name}")
            
            # Simplified health check - just check once
            health_result = await self.check_cluster_health(
                HealthCheckInput(
                    cluster=input_data.cluster,
                    dry_run=input_data.dry_run,
                    timeout=30,
                )
            )
            
            if not health_result.is_healthy or health_result.health_status != "GREEN":
                error_msg = f"Cannot restart pod {input_data.pod_name}: cluster health is {health_result.health_status}, must be GREEN"
                activity.logger.error(error_msg)
                raise Exception(error_msg)

            # Execute decommission strategy - let Temporal handle failures and retries
            await self._execute_decommission_strategy(
                input_data.pod_name,
                input_data.namespace,
                input_data.cluster
            )

            # Delete the pod - behavior depends on cluster configuration:
            # - For Kubernetes-managed: deletion triggers preStop hook which handles decommission
            # - For manual: decommission already completed, just delete the pod
            grace_period = 30
            if input_data.cluster.has_dc_util:
                # Longer grace period for preStop hook to complete decommission
                grace_period = input_data.cluster.dc_util_timeout + 60
                activity.logger.info(f"Deleting pod {input_data.pod_name} - preStop hook will handle decommission")
            else:
                activity.logger.info(f"Manual decommission completed, now deleting pod {input_data.pod_name}")

            await asyncio.to_thread(
                self.core_v1.delete_namespaced_pod,
                name=input_data.pod_name,
                namespace=input_data.namespace,
                grace_period_seconds=grace_period
            )

            # Wait for pod to be ready (after recreation)
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

    @activity.defn
    async def decommission_pod(self, input_data: DecommissionInput) -> DecommissionResult:
        """
        Decommission a CrateDB pod using the appropriate strategy.

        This activity detects whether the cluster has Kubernetes-managed decommission
        (preStop hook with dc_util) or requires manual decommission, then executes
        the appropriate strategy.
        """
        start_time = datetime.now(timezone.utc)
        activity.logger.info(f"Starting decommission for pod {input_data.pod_name}")

        if input_data.dry_run:
            activity.logger.info(f"[DRY RUN] Would decommission pod {input_data.pod_name}")
            return DecommissionResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                strategy_used="dry_run",
                success=True,
                duration=0.0,
                started_at=start_time,
                completed_at=datetime.now(timezone.utc)
            )

        try:
            # Execute decommission strategy - let Temporal handle failures
            await self._execute_decommission_strategy(
                input_data.pod_name,
                input_data.namespace,
                input_data.cluster
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            strategy_used = "kubernetes_managed" if input_data.cluster.has_dc_util else "manual"

            activity.logger.info(f"Decommission completed for pod {input_data.pod_name} using {strategy_used} strategy")

            return DecommissionResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                strategy_used=strategy_used,
                success=True,
                duration=duration,
                started_at=start_time,
                completed_at=end_time,
                decommission_timeout=input_data.cluster.dc_util_timeout
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            error_msg = f"Decommission failed for pod {input_data.pod_name}: {str(e)}"
            activity.logger.error(error_msg)

            return DecommissionResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                strategy_used="unknown",
                success=False,
                duration=duration,
                error=error_msg,
                started_at=start_time,
                completed_at=end_time
            )

    async def _execute_decommission_strategy(self, pod_name: str, namespace: str, cluster: CrateDBCluster) -> None:
        """Execute the appropriate decommission strategy based on cluster configuration."""
        activity.logger.info(f"Analyzing decommission strategy for pod {pod_name}")
        activity.logger.info(f"Cluster config: has_prestop_hook={cluster.has_prestop_hook}, has_dc_util={cluster.has_dc_util}")

        if cluster.has_dc_util:
            # For Kubernetes-managed decommission, just delete the pod
            # The preStop hook will handle decommission automatically
            activity.logger.info(f"Using Kubernetes-managed decommission for pod {pod_name}")
            activity.logger.info(f"PreStop hook with dc_util will handle decommission automatically")
            # Nothing to do here - pod deletion in restart_pod will trigger preStop hook
        else:
            await self._execute_manual_decommission(pod_name, namespace, cluster)



    async def _execute_manual_decommission(self, pod_name: str, namespace: str, cluster: CrateDBCluster) -> None:
        """
        Execute manual decommission when no dc_util is configured.

        This executes the decommission commands and waits for the CrateDB process to exit.
        The caller is responsible for deleting the pod after this completes.
        """
        activity.logger.info(f"Using manual decommission for pod {pod_name}")
        activity.logger.info(f"No dc_util configured - executing manual decommission via CrateDB API")

        # Execute manual decommission commands
        pod_suffix = pod_name.rsplit("-", 1)[-1]

        # Configuration for manual decommission
        timeout_value = f"{cluster.dc_util_timeout}s"
        force_value = True
        min_availability = cluster.min_availability

        # SQL commands to execute
        sql_commands = [
            'set global transient "cluster.routing.allocation.enable" = "new_primaries"',
            f'set global transient "cluster.graceful_stop.timeout" = "{timeout_value}"',
            f'set global transient "cluster.graceful_stop.force" = {str(force_value).lower()}',
            f'set global transient "cluster.graceful_stop.min_availability" = "{min_availability}"',
            f"alter cluster decommission $$data-hot-{pod_suffix}$$"
        ]

        # Execute each command
        for idx, sql_cmd in enumerate(sql_commands, 1):
            activity.logger.debug(f"Executing manual decommission SQL {idx}/5: {sql_cmd}")

            # Create curl command
            json_payload = json.dumps({"stmt": sql_cmd})
            curl_cmd = f'curl --insecure -sS -H "Content-Type: application/json" -X POST https://127.0.0.1:4200/_sql -d \'{json_payload}\''

            if idx == 5:  # Final decommission command
                # For the decommission command, wait for process to exit
                curl_cmd = f"""
                curl_output=$(curl --insecure -sS -H "Content-Type: application/json" -X POST https://127.0.0.1:4200/_sql -d '{json_payload}')
                curl_exit_code=$?
                echo "DECOMMISSION_EXIT_CODE: $curl_exit_code"
                echo "DECOMMISSION_RESPONSE: $curl_output"
                if [ $curl_exit_code -eq 0 ]; then
                    echo "Waiting for CrateDB process to exit after decommission..."
                    while kill -0 1 2>/dev/null; do
                        echo "Waiting for crate to exit..."
                        sleep 0.5
                    done
                    echo "CrateDB process has exited - decommission complete"
                else
                    echo "Decommission command failed with exit code $curl_exit_code"
                    exit $curl_exit_code
                fi
                """

            # Execute command in pod - Temporal handles timeouts and retries
            resp = await self._execute_command_in_pod(pod_name, namespace, curl_cmd)

            if idx == 5:
                activity.logger.info(f"Manual decommission completed - CrateDB process has exited")
                activity.logger.info(f"Pod {pod_name} is ready for deletion and restart")
                activity.logger.debug(f"Decommission response: {resp}")
            else:
                activity.logger.debug(f"Manual decommission command {idx} completed")

        activity.logger.info(f"Manual decommission strategy completed for pod {pod_name}")

    async def _execute_command_in_pod(self, pod_name: str, namespace: str, command: str) -> str:
        """Execute a command in a pod using kubectl exec. Temporal handles timeouts and retries."""
        activity.logger.debug(f"Executing command in pod {pod_name}: {command[:100]}...")

        exec_command = ["/bin/sh", "-c", command]
        resp = await asyncio.to_thread(
            stream.stream,
            self.core_v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            container="crate",
            command=exec_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )

        activity.logger.debug(f"Command output: {resp}")
        return resp

    async def _wait_for_pod_deletion(self, pod_name: str, namespace: str, timeout: int) -> None:
        """Wait for pod to be deleted."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                await asyncio.to_thread(
                    self.core_v1.read_namespaced_pod,
                    name=pod_name,
                    namespace=namespace
                )
                activity.logger.debug(f"Pod {pod_name} still exists, waiting for deletion...")
                await asyncio.sleep(5)
            except ApiException as e:
                if e.status == 404:
                    activity.logger.info(f"Pod {pod_name} has been deleted")
                    return
                else:
                    activity.logger.warning(f"Error checking pod deletion status: {e}")
                    await asyncio.sleep(5)

        raise TimeoutError(f"Pod {pod_name} was not deleted within {timeout}s")

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
            activity.logger.debug(f"Checking health for cluster {cluster.name} (CRD: {cluster.crd_name}) in namespace {cluster.namespace}")

            crd = self.custom_api.get_namespaced_custom_object(
                group="cloud.crate.io",
                version="v1",
                namespace=cluster.namespace,
                plural="cratedbs",
                name=cluster.crd_name,
            )

            activity.logger.debug(f"Retrieved CRD status: {crd.get('status', {})}")
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
            elif health in ["YELLOW", "RED", "UNREACHABLE", "UNKNOWN"]:
                # These are temporary states that should trigger retries
                activity.logger.info(f"Cluster {cluster.name} health is {health}, retrying until GREEN...")
                raise Exception(f"Cluster {cluster.name} health is {health}, retrying...")
            else:
                # Unknown health states - return as unhealthy but don't retry
                activity.logger.error(f"Cluster {cluster.name} has unknown health status: {health}")
                return HealthCheckResult(
                    cluster_name=cluster.name,
                    health_status=health,
                    is_healthy=False,
                    checked_at=checked_at,
                )

        except Exception as e:
            error_msg = f"Error checking cluster health: {e}"
            activity.logger.error(error_msg)
            activity.logger.debug(f"Health check exception details for {cluster.name}: {type(e).__name__}: {str(e)}")

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

    @activity.defn
    async def delete_pod(self, input_data: PodRestartInput) -> bool:
        """
        Delete a pod with proper grace period.
        
        Args:
            input_data: Pod restart parameters
            
        Returns:
            True if successful
        """
        try:
            self._ensure_kube_client()
            
            if input_data.dry_run:
                activity.logger.info(f"[DRY RUN] Would delete pod {input_data.pod_name}")
                return True
            
            # Calculate grace period based on cluster configuration
            grace_period = 30
            if input_data.cluster.has_dc_util:
                # Longer grace period for preStop hook to complete decommission
                grace_period = input_data.cluster.dc_util_timeout + 60
                activity.logger.info(f"Deleting pod {input_data.pod_name} - preStop hook will handle decommission")
            else:
                activity.logger.info(f"Manual decommission completed, now deleting pod {input_data.pod_name}")
            
            await asyncio.to_thread(
                self.core_v1.delete_namespaced_pod,
                name=input_data.pod_name,
                namespace=input_data.namespace,
                grace_period_seconds=grace_period
            )
            
            activity.logger.info(f"Successfully deleted pod {input_data.pod_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to delete pod {input_data.pod_name}: {e}"
            activity.logger.error(error_msg)
            raise Exception(error_msg)

    @activity.defn
    async def wait_for_pod_ready(self, input_data: PodRestartInput) -> bool:
        """
        Wait for a pod to be ready after restart.
        
        Args:
            input_data: Pod restart parameters
            
        Returns:
            True if pod becomes ready
        """
        try:
            self._ensure_kube_client()
            
            if input_data.dry_run:
                activity.logger.info(f"[DRY RUN] Would wait for pod {input_data.pod_name} to be ready")
                await asyncio.sleep(5)  # Simulate wait time
                return True
            
            await self._wait_for_pod_ready(
                input_data.pod_name,
                input_data.namespace,
                input_data.pod_ready_timeout
            )
            
            activity.logger.info(f"Pod {input_data.pod_name} is ready")
            return True
            
        except Exception as e:
            error_msg = f"Failed waiting for pod {input_data.pod_name} to be ready: {e}"
            activity.logger.error(error_msg)
            raise Exception(error_msg)
