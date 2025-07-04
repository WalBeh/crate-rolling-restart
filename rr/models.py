"""
Data models for Temporal workflows and activities.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class CrateDBCluster(BaseModel):
    """Pydantic model for CrateDB cluster information."""

    name: str  # The cluster name (spec.cluster.name or metadata.name)
    namespace: str
    statefulset_name: str
    health: str
    replicas: int
    pods: List[str] = Field(default_factory=list)
    has_prestop_hook: bool = False
    has_dc_util: bool = False
    suspended: bool = False
    crd_name: str  # The CRD resource name (metadata.name)
    dc_util_timeout: int = 720  # Default timeout for dc_util in seconds
    min_availability: str = "PRIMARIES"  # PRIMARIES, NONE, or FULL


class RestartOptions(BaseModel):
    """Configuration options for cluster restart operations."""
    
    kubeconfig: Optional[str] = None
    context: Optional[str] = None
    dry_run: bool = False
    skip_hook_warning: bool = False
    output_format: str = "text"
    log_level: str = "INFO"
    pod_ready_timeout: int = 300
    health_check_timeout: int = 300
    maintenance_config_path: Optional[str] = None
    ignore_maintenance_windows: bool = False
    only_on_suspended_nodes: bool = False


class RestartResult(BaseModel):
    """Result of a cluster restart operation."""
    
    cluster: CrateDBCluster
    success: bool
    duration: float
    restarted_pods: List[str] = Field(default_factory=list)
    total_pods: int = 0
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class PodRestartResult(BaseModel):
    """Result of a single pod restart operation."""
    
    pod_name: str
    namespace: str
    success: bool
    duration: float
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class ClusterDiscoveryInput(BaseModel):
    """Input for cluster discovery activity."""
    
    cluster_names: Optional[List[str]] = None
    kubeconfig: Optional[str] = None
    context: Optional[str] = None
    maintenance_config_path: Optional[str] = None


class ClusterDiscoveryResult(BaseModel):
    """Result of cluster discovery activity."""
    
    clusters: List[CrateDBCluster] = Field(default_factory=list)
    total_found: int
    errors: List[str] = Field(default_factory=list)


class PodRestartInput(BaseModel):
    """Input for pod restart activity."""
    
    pod_name: str
    namespace: str
    cluster: CrateDBCluster
    dry_run: bool = False
    pod_ready_timeout: int = 300


class HealthCheckInput(BaseModel):
    """Input for health check activity."""
    
    cluster: CrateDBCluster
    dry_run: bool = False
    timeout: int = 300


class ClusterRoutingResetInput(BaseModel):
    """Input for cluster routing allocation reset activity."""
    
    pod_name: str
    namespace: str
    cluster: CrateDBCluster
    dry_run: bool = False


class ClusterRoutingResetResult(BaseModel):
    """Result of cluster routing allocation reset activity."""
    
    pod_name: str
    namespace: str
    cluster_name: str
    success: bool
    duration: float
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class HealthCheckResult(BaseModel):
    """Result of health check activity."""
    
    cluster_name: str
    health_status: str
    is_healthy: bool
    checked_at: Optional[datetime] = None
    error: Optional[str] = None


class MultiClusterRestartInput(BaseModel):
    """Input for multi-cluster restart workflow."""
    
    cluster_names: List[str]
    options: RestartOptions


class MultiClusterRestartResult(BaseModel):
    """Result of multi-cluster restart workflow."""
    
    results: List[RestartResult] = Field(default_factory=list)
    total_clusters: int
    successful_clusters: int
    failed_clusters: int
    total_duration: float
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class ClusterValidationInput(BaseModel):
    """Input for cluster validation activity."""
    
    cluster: CrateDBCluster
    skip_hook_warning: bool = False


class ClusterValidationResult(BaseModel):
    """Result of cluster validation activity."""
    
    cluster_name: str
    is_valid: bool
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class MaintenanceWindowCheckInput(BaseModel):
    """Input for maintenance window check activity."""
    
    cluster_name: str
    current_time: Optional[datetime] = None
    config_path: Optional[str] = None


class MaintenanceWindowCheckResult(BaseModel):
    """Result of maintenance window check activity."""
    
    cluster_name: str
    should_wait: bool
    reason: str
    next_window_start: Optional[datetime] = None
    current_time: datetime
    in_maintenance_window: bool = False


class DecommissionInput(BaseModel):
    """Input for decommission activity."""
    
    pod_name: str
    namespace: str
    cluster: CrateDBCluster
    dry_run: bool = False


class DecommissionResult(BaseModel):
    """Result of decommission activity."""
    
    pod_name: str
    namespace: str
    strategy_used: str  # "kubernetes_managed" or "manual"
    success: bool
    duration: float
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    process_exited: bool = False  # For manual decommission
    decommission_timeout: int = 720