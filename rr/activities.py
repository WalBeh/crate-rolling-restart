"""
Temporal activities for CrateDB Kubernetes operations.
"""

import asyncio
import json
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
    ClusterRoutingResetInput,
    ClusterRoutingResetResult,
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

            # Note: Cluster routing allocation reset is now handled as a separate activity
            # in the workflow to ensure Temporal execution guarantees

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

            # Note: Cluster routing allocation reset is now handled as a separate activity
            # in the workflow to ensure it executes even if pod restart fails

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

        # Send heartbeat with initial status
        activity.heartbeat({"status": "starting", "pod": input_data.pod_name})

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
            # CRITICAL: Check cluster health before decommission
            # Only GREEN status is safe for decommission operations
            activity.logger.info(f"Checking cluster health before decommissioning pod {input_data.pod_name}")
            activity.heartbeat({"status": "health_check", "pod": input_data.pod_name})
            
            health_input = HealthCheckInput(
                cluster=input_data.cluster,
                dry_run=False,
                timeout=60
            )
            
            health_result = await self.check_cluster_health(health_input)
            activity.logger.info(f"Cluster health validated: {health_result.health_status}")
            
            # Send heartbeat before strategy analysis
            activity.heartbeat({"status": "analyzing_strategy", "pod": input_data.pod_name})

            # Execute decommission strategy - let Temporal handle failures
            await self._execute_decommission_strategy(
                input_data.pod_name,
                input_data.namespace,
                input_data.cluster
            )

            # Send heartbeat on completion
            activity.heartbeat({"status": "completed", "pod": input_data.pod_name})

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
            # Send heartbeat on failure
            activity.heartbeat({"status": "failed", "error": str(e), "pod": input_data.pod_name})

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

        # Send heartbeat during strategy execution
        activity.heartbeat({"status": "executing_strategy", "pod": pod_name})

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

        # Send heartbeat for manual decommission start
        activity.heartbeat({"status": "manual_decommission_start", "pod": pod_name})

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

            # Send heartbeat for each command
            activity.heartbeat({"status": f"executing_sql_command_{idx}", "pod": pod_name, "command": sql_cmd[:50]})

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
                # Send heartbeat for decommission completion
                activity.heartbeat({"status": "manual_decommission_completed", "pod": pod_name})
            else:
                activity.logger.debug(f"Manual decommission command {idx} completed")
                # Send heartbeat for command completion
                activity.heartbeat({"status": f"sql_command_{idx}_completed", "pod": pod_name})

        activity.logger.info(f"Manual decommission strategy completed for pod {pod_name}")

    @activity.defn
    async def reset_cluster_routing_allocation(self, input_data: ClusterRoutingResetInput) -> ClusterRoutingResetResult:
        """
        Reset cluster routing allocation setting to 'all' after manual decommission.

        This is a separate activity to ensure Temporal execution guarantees.
        It will be retried by Temporal until successful or max attempts reached.

        Args:
            input_data: Reset operation parameters

        Returns:
            ClusterRoutingResetResult with operation status

        Raises:
            Exception: If reset fails - allows Temporal to retry the activity
        """
        start_time = time.time()
        started_at = datetime.now(timezone.utc)

        activity.logger.info(f"🔄 Starting cluster routing allocation reset for pod {input_data.pod_name}")
        activity.logger.info(f"Target: {input_data.cluster.name} in {input_data.namespace}")

        # Send initial heartbeat (safe for testing)
        try:
            activity.heartbeat({
                "status": "starting_routing_reset",
                "pod": input_data.pod_name,
                "cluster": input_data.cluster.name
            })
        except RuntimeError:
            # Not in activity context (e.g., during testing)
            pass

        # Brief wait for CrateDB to be ready (reduced from 10s to 5s)
        activity.logger.info(f"⏳ Waiting for CrateDB to accept SQL connections...")

        # Send heartbeat during startup wait (safe for testing)
        try:
            activity.heartbeat({
                "status": "waiting_for_cratedb_startup",
                "pod": input_data.pod_name
            })
        except RuntimeError:
            # Not in activity context (e.g., during testing)
            pass

        await asyncio.sleep(3)  # Further reduced startup delay for 2 retry attempts

        try:
            # Send heartbeat before reset attempt (safe for testing)
            try:
                activity.heartbeat({
                    "status": "executing_routing_reset",
                    "pod": input_data.pod_name,
                    "max_attempts": 5
                })
            except RuntimeError:
                # Not in activity context (e.g., during testing)
                pass

            # Attempt the reset with timeout protection
            try:
                await asyncio.wait_for(
                    self._reset_cluster_routing_allocation(
                        input_data.pod_name,
                        input_data.namespace,
                        input_data.cluster
                    ),
                    timeout=40  # 40 second internal timeout for 2 retry attempts
                )
            except asyncio.TimeoutError:
                raise Exception(f"Routing reset timed out after 40 seconds for pod {input_data.pod_name}")

            duration = time.time() - start_time
            activity.logger.info(f"✅ Cluster routing allocation reset completed successfully in {duration:.2f}s")

            # Send completion heartbeat (safe for testing)
            try:
                activity.heartbeat({
                    "status": "routing_reset_completed",
                    "pod": input_data.pod_name,
                    "duration": duration
                })
            except RuntimeError:
                # Not in activity context (e.g., during testing)
                pass

            return ClusterRoutingResetResult(
                pod_name=input_data.pod_name,
                namespace=input_data.namespace,
                cluster_name=input_data.cluster.name,
                success=True,
                duration=duration,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=None
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed to reset cluster routing allocation for pod {input_data.pod_name}: {e}"
            activity.logger.error(f"❌ {error_msg}")

            # Log critical instructions for manual intervention
            activity.logger.error(f"🚨 CRITICAL: Cluster {input_data.cluster.name} may remain in degraded state")
            activity.logger.error(f"📋 MANUAL INTERVENTION REQUIRED:")
            activity.logger.error(f"   kubectl exec -n {input_data.namespace} {input_data.pod_name} -c crate -- \\")
            activity.logger.error(f"     curl --insecure -sS -H 'Content-Type: application/json' \\")
            activity.logger.error(f"     -X POST https://127.0.0.1:4200/_sql \\")
            activity.logger.error(f"     -d '{{\"stmt\": \"set global transient \\\"cluster.routing.allocation.enable\\\" = \\\"all\\\"\"}}'")

            # Re-raise the exception to let Temporal retry the activity
            raise Exception(error_msg) from e



    async def _reset_cluster_routing_allocation(self, pod_name: str, namespace: str, cluster: CrateDBCluster) -> None:
        """
        Reset cluster routing allocation setting to 'all' after pod restart.

        Simplified version with single attempt to reduce timeout issues.
        """
        activity.logger.info(f"🔧 Executing cluster routing allocation reset command (target pod: {pod_name})")

        sql_cmd = 'set global transient "cluster.routing.allocation.enable" = "all"'
        json_payload = json.dumps({"stmt": sql_cmd})
        curl_cmd = f'curl --insecure -sS -H "Content-Type: application/json" -X POST https://127.0.0.1:4200/_sql -d \'{json_payload}\''

        # Try target pod first
        try:
            resp = await self._execute_command_in_pod(pod_name, namespace, curl_cmd)
            activity.logger.info(f"✅ Successfully reset cluster.routing.allocation.enable = 'all' via pod {pod_name}")
            activity.logger.debug(f"Reset response: {resp}")
            return
        except Exception as e:
            activity.logger.warning(f"Failed to reset via target pod {pod_name}: {e}")
            
        # Try fallback pods from cluster
        fallback_pods = [p for p in cluster.pods if p != pod_name]
        for fallback_pod in fallback_pods:
            try:
                resp = await self._execute_command_in_pod(fallback_pod, namespace, curl_cmd)
                activity.logger.info(f"✅ Successfully reset cluster.routing.allocation.enable = 'all' via fallback pod {fallback_pod}")
                activity.logger.debug(f"Reset response: {resp}")
                return
            except Exception as e:
                activity.logger.warning(f"Failed to reset via fallback pod {fallback_pod}: {e}")
                
        # If all pods failed, raise exception
        raise Exception("All reset attempts failed")



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
        """Wait for a pod to be ready with improved error handling and heartbeat management."""
        start_time = time.time()
        pod_ready_time = None
        min_ready_duration = 20  # Minimum time pod should be ready
        last_heartbeat = start_time
        consecutive_errors = 0
        max_consecutive_errors = 3

        activity.logger.info(f"Waiting for pod {pod_name} to be ready (timeout: {timeout}s)")

        # Send initial status heartbeat
        activity.heartbeat({
            "status": "waiting_for_pod_ready_start",
            "pod": pod_name,
            "timeout": timeout,
            "min_ready_duration": min_ready_duration
        })

        while time.time() - start_time < timeout:
            current_time = time.time()
            elapsed = current_time - start_time

            # Send heartbeat every 20 seconds (well within 30 second timeout)
            if current_time - last_heartbeat >= 20:
                activity.heartbeat({
                    "status": "waiting_for_pod_ready_progress",
                    "pod": pod_name,
                    "elapsed_seconds": elapsed,
                    "timeout_seconds": timeout
                })
                last_heartbeat = current_time

            try:
                # Use asyncio.to_thread for the API call to avoid blocking
                pod = await asyncio.to_thread(
                    self.core_v1.read_namespaced_pod,
                    name=pod_name,
                    namespace=namespace
                )

                # Reset error counter on successful API call
                consecutive_errors = 0

                # Enhanced status reporting
                pod_phase = pod.status.phase if pod.status else "Unknown"
                activity.logger.debug(f"Pod {pod_name} status: phase={pod_phase}")

                if pod.status.phase == "Running":
                    ready = True
                    ready_conditions = []

                    for condition in pod.status.conditions or []:
                        if condition.type == "Ready":
                            ready_conditions.append(f"{condition.type}={condition.status}")
                            if condition.status != "True":
                                ready = False
                                break

                    activity.logger.debug(f"Pod {pod_name} conditions: {', '.join(ready_conditions) if ready_conditions else 'None'}")

                    if ready:
                        if pod_ready_time is None:
                            pod_ready_time = current_time
                            activity.logger.info(f"Pod {pod_name} is ready, waiting for stability ({min_ready_duration}s)...")
                            activity.heartbeat({
                                "status": "pod_ready_waiting_stability",
                                "pod": pod_name,
                                "stability_duration": min_ready_duration
                            })
                        elif current_time - pod_ready_time >= min_ready_duration:
                            stable_duration = current_time - pod_ready_time
                            activity.logger.info(f"Pod {pod_name} has been stable for {stable_duration:.1f}s")
                            activity.heartbeat({"status": "pod_stable", "pod": pod_name, "stable_duration": stable_duration})
                            return
                    else:
                        if pod_ready_time is not None:
                            activity.logger.debug(f"Pod {pod_name} became not ready, resetting stability timer")
                        pod_ready_time = None
                elif pod.status.phase in ["Pending", "ContainerCreating"]:
                    activity.logger.debug(f"Pod {pod_name} is {pod.status.phase}, continuing to wait...")
                    pod_ready_time = None
                elif pod.status.phase == "Failed":
                    # Pod failed - this is a terminal condition, don't retry
                    error_msg = f"Pod {pod_name} is in terminal state: Failed. This indicates the pod startup failed and needs investigation."
                    activity.logger.error(error_msg)
                    # Check pod events or status for more details
                    try:
                        if pod.status.container_statuses:
                            for container_status in pod.status.container_statuses:
                                if container_status.state and container_status.state.waiting:
                                    error_msg += f" Container {container_status.name} waiting: {container_status.state.waiting.reason}"
                                elif container_status.state and container_status.state.terminated:
                                    error_msg += f" Container {container_status.name} terminated: {container_status.state.terminated.reason}"
                    except Exception:
                        pass  # Don't fail on status parsing
                    from .state_machines import ResourceNotFoundError
                    raise ResourceNotFoundError(error_msg)
                elif pod.status.phase == "Succeeded":
                    # Pod succeeded but that's not what we want for a long-running service
                    error_msg = f"Pod {pod_name} completed successfully but should be a long-running service"
                    activity.logger.warning(error_msg)
                    from .state_machines import ConfigurationError
                    raise ConfigurationError(error_msg)
                else:
                    activity.logger.warning(f"Pod {pod_name} is in unexpected phase: {pod.status.phase}")
                    pod_ready_time = None

            except ApiException as e:
                consecutive_errors += 1
                activity.logger.warning(f"API error checking pod {pod_name} status (attempt {consecutive_errors}): {e}")

                if consecutive_errors >= max_consecutive_errors:
                    error_msg = f"Too many consecutive API errors ({consecutive_errors}) checking pod {pod_name}"
                    activity.logger.error(error_msg)
                    raise Exception(error_msg)

                pod_ready_time = None

            except asyncio.CancelledError:
                activity.logger.warning(f"Pod ready check for {pod_name} was cancelled after {elapsed:.1f}s")
                raise

            except Exception as e:
                consecutive_errors += 1
                activity.logger.warning(f"Unexpected error checking pod {pod_name} (attempt {consecutive_errors}): {e}")

                if consecutive_errors >= max_consecutive_errors:
                    error_msg = f"Too many consecutive errors ({consecutive_errors}) checking pod {pod_name}: {e}"
                    activity.logger.error(error_msg)
                    raise Exception(error_msg)

                pod_ready_time = None

            try:
                # Sleep for 5 seconds before next check (shorter interval for better responsiveness)
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                elapsed = time.time() - start_time
                activity.logger.info(f"Pod ready wait for {pod_name} was cancelled during sleep after {elapsed:.1f}s")
                activity.heartbeat({"status": "cancelled_during_sleep", "pod": pod_name, "elapsed": elapsed})
                raise

        # Timeout reached
        elapsed = time.time() - start_time
        error_msg = f"Pod {pod_name} did not become ready within {timeout} seconds (elapsed: {elapsed:.1f}s)"
        activity.logger.error(error_msg)
        raise TimeoutError(error_msg)

    @activity.defn
    async def check_cluster_health(self, input_data: HealthCheckInput) -> HealthCheckResult:
        """
        Check the health of a CrateDB cluster.

        This activity now handles different health states and raises appropriate exceptions
        for non-GREEN states, allowing Temporal's retry policy to handle retries.

        Args:
            input_data: Health check parameters

        Returns:
            HealthCheckResult with health status if GREEN

        Raises:
            Exception: For non-GREEN states (will be retried by Temporal)
        """
        cluster = input_data.cluster
        # Use None for timestamps to avoid datetime.now() issues
        checked_at = None

        # Send heartbeat for health check start
        activity.heartbeat({"status": "checking_health", "cluster": cluster.name})

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

            # Send heartbeat with current health status
            activity.heartbeat({"status": "health_checked", "cluster": cluster.name, "health": health})

            # Handle different health statuses
            if health == "GREEN":
                return HealthCheckResult(
                    cluster_name=cluster.name,
                    health_status=health,
                    is_healthy=True,
                    checked_at=checked_at,
                )
            else:
                # For non-GREEN states, raise an exception so Temporal can retry
                # This allows the retry policy to handle different states appropriately
                error_msg = f"Cluster {cluster.name} health is {health}, expected GREEN"
                activity.logger.warning(error_msg)

                # Different exception types for different states
                if health == "RED":
                    raise Exception(error_msg)
                elif health == "YELLOW":
                    raise Exception(error_msg)
                elif health == "UNKNOWN":
                    raise Exception(error_msg)
                else:
                    raise Exception(error_msg)

        except ApiException as e:
            error_msg = f"API error checking health of cluster {cluster.name}: {e}"
            activity.logger.error(error_msg)
            activity.heartbeat({"status": "health_check_failed", "cluster": cluster.name, "error": "api_error"})
            raise Exception(error_msg)

        except Exception as e:
            # Re-raise our own exceptions
            if "health is" in str(e) or "API error" in str(e):
                raise

            error_msg = f"Unexpected error checking health of cluster {cluster.name}: {e}"
            activity.logger.error(error_msg)
            activity.heartbeat({"status": "health_check_failed", "cluster": cluster.name, "error": "unexpected_error"})
            raise Exception(error_msg)

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

            # CRITICAL: Check cluster health before pod deletion
            # Only GREEN status is safe for pod deletion operations
            activity.logger.info(f"Checking cluster health before deleting pod {input_data.pod_name}")
            
            health_input = HealthCheckInput(
                cluster=input_data.cluster,
                dry_run=False,
                timeout=60
            )
            
            health_result = await self.check_cluster_health(health_input)
            activity.logger.info(f"Cluster health validated: {health_result.health_status}")

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
        Wait for a pod to be ready after restart with enhanced error handling.

        Args:
            input_data: Pod restart parameters

        Returns:
            True if pod becomes ready
        """
        try:
            self._ensure_kube_client()

            # Send initial heartbeat with timeout info
            activity.heartbeat({
                "status": "starting_pod_wait",
                "pod": input_data.pod_name,
                "timeout": input_data.pod_ready_timeout
            })

            if input_data.dry_run:
                activity.logger.info(f"[DRY RUN] Would wait for pod {input_data.pod_name} to be ready")
                await asyncio.sleep(2)  # Simulate some wait time
                return True

            activity.logger.info(f"Waiting for pod {input_data.pod_name} to be ready (timeout: {input_data.pod_ready_timeout}s)")

            await self._wait_for_pod_ready(
                input_data.pod_name,
                input_data.namespace,
                input_data.pod_ready_timeout
            )

            activity.logger.info(f"Pod {input_data.pod_name} is ready")
            activity.heartbeat({"status": "pod_ready_completed", "pod": input_data.pod_name})
            return True

        except asyncio.CancelledError:
            activity.logger.warning(f"Pod ready wait for {input_data.pod_name} was cancelled")
            activity.heartbeat({"status": "pod_ready_cancelled", "pod": input_data.pod_name})
            raise
        except TimeoutError as e:
            activity.logger.error(f"Timeout waiting for pod {input_data.pod_name}: {e}")
            activity.heartbeat({"status": "pod_ready_timeout", "pod": input_data.pod_name, "error": str(e)})
            raise
        except Exception as e:
            error_msg = f"Failed to wait for pod {input_data.pod_name}: {e}"
            activity.logger.error(error_msg)
            activity.heartbeat({"status": "pod_ready_failed", "pod": input_data.pod_name, "error": str(e)})
            raise Exception(error_msg)

    @activity.defn
    async def is_pod_on_suspended_node(self, pod_name: str, namespace: str) -> bool:
        """
        Check if a pod is running on a suspended Kubernetes node.

        Args:
            pod_name: Name of the pod to check
            namespace: Namespace of the pod

        Returns:
            True if pod is running on a suspended node, False otherwise
        """
        try:
            self._ensure_kube_client()

            # Get pod details to find which node it's running on
            pod = await asyncio.to_thread(
                self.core_v1.read_namespaced_pod,
                name=pod_name,
                namespace=namespace
            )

            if not pod.spec.node_name:
                activity.logger.warning(f"Pod {pod_name} has no assigned node")
                return False

            node_name = pod.spec.node_name
            activity.logger.info(f"Pod {pod_name} is running on node {node_name}")

            # Get node details to check if it's suspended
            node = await asyncio.to_thread(
                self.core_v1.read_node,
                name=node_name
            )

            # Check if node is suspended (has unschedulable taint or annotation)
            is_suspended = False

            # Check if node is marked as unschedulable
            if node.spec.unschedulable:
                is_suspended = True
                activity.logger.info(f"Node {node_name} is marked as unschedulable")

            # Check for common suspension taints
            if node.spec.taints:
                for taint in node.spec.taints:
                    if taint.key in [
                        "node.kubernetes.io/unschedulable",
                        "node.kubernetes.io/not-ready",
                        "node.kubernetes.io/unreachable",
                        "aws.amazon.com/spot-instance-terminating",
                        "cluster-autoscaler.kubernetes.io/scale-down-disabled",
                        "node.kubernetes.io/suspend"
                    ]:
                        is_suspended = True
                        activity.logger.info(f"Node {node_name} has suspension taint: {taint.key}={taint.value}")
                        break

            # Check for suspension annotations
            if node.metadata.annotations:
                for annotation_key in [
                    "cluster-autoscaler.kubernetes.io/scale-down-disabled",
                    "node.kubernetes.io/suspend",
                    "node.kubernetes.io/suspended"
                ]:
                    if annotation_key in node.metadata.annotations:
                        is_suspended = True
                        activity.logger.info(f"Node {node_name} has suspension annotation: {annotation_key}")
                        break

            if is_suspended:
                activity.logger.info(f"Pod {pod_name} is running on suspended node {node_name}")
            else:
                activity.logger.info(f"Pod {pod_name} is running on active node {node_name}")

            return is_suspended

        except Exception as e:
            error_msg = f"Failed to check if pod {pod_name} is on suspended node: {e}"
            activity.logger.error(error_msg)
            # Default to False to avoid blocking operations on error
            return False
