"""
CrateDB Kubernetes Cluster Manager with Temporal workflows.

A robust, production-ready tool for managing CrateDB cluster restarts in Kubernetes
using Temporal workflows for reliability, observability, and fault tolerance.
"""

__version__ = "0.1.0"
__author__ = "CrateDB Team"
__email__ = "support@crate.io"

# Package metadata
__title__ = "rr"
__description__ = "CrateDB Kubernetes Cluster Manager with Temporal workflows"
__url__ = "https://github.com/crate/rr"
__license__ = "Apache License 2.0"

# Export main components for easier imports
from .models import (
    CrateDBCluster,
    RestartOptions,
    RestartResult,
    MultiClusterRestartResult,
)

__all__ = [
    "CrateDBCluster", 
    "RestartOptions",
    "RestartResult",
    "MultiClusterRestartResult",
    "__version__",
]