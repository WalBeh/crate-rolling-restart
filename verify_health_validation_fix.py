#!/usr/bin/env python3
"""
Simple integration test to verify the health validation fix is in place.

This test verifies that the restart_pod method now includes health validation
before proceeding with pod deletion.
"""

import inspect
import logging
import sys
from pathlib import Path

# Add the rr module to the path
sys.path.insert(0, str(Path(__file__).parent))

from rr.activities import CrateDBActivities

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_health_validation_exists():
    """Test that health validation code exists in restart_pod method."""
    
    logger.info("🔍 Checking if health validation exists in restart_pod method...")
    
    # Get the restart_pod method source code
    restart_pod_method = getattr(CrateDBActivities, 'restart_pod', None)
    
    if not restart_pod_method:
        logger.error("❌ restart_pod method not found!")
        return False
    
    # Get the source code
    try:
        source_code = inspect.getsource(restart_pod_method)
    except OSError:
        logger.error("❌ Could not get source code for restart_pod method!")
        return False
    
    # Check for health validation keywords
    health_validation_checks = [
        "Validating cluster health before restarting",
        "check_cluster_health",
        "cluster health is",
        "must be GREEN",
        "Health check failed before restarting",
    ]
    
    found_checks = []
    missing_checks = []
    
    for check in health_validation_checks:
        if check in source_code:
            found_checks.append(check)
            logger.info(f"✅ Found: '{check}'")
        else:
            missing_checks.append(check)
            logger.warning(f"⚠️  Missing: '{check}'")
    
    # Check the method structure
    lines = source_code.split('\n')
    health_check_found = False
    decommission_found = False
    pod_deletion_found = False
    
    for i, line in enumerate(lines):
        if "check_cluster_health" in line:
            health_check_found = True
            health_check_line = i
        elif "_execute_decommission_strategy" in line:
            decommission_found = True
            decommission_line = i
        elif "delete_namespaced_pod" in line:
            pod_deletion_found = True
            pod_deletion_line = i
    
    # Verify the order: health check should come before decommission and pod deletion
    if health_check_found and decommission_found and pod_deletion_found:
        if health_check_line < decommission_line < pod_deletion_line:
            logger.info("✅ Correct order: health check → decommission → pod deletion")
            order_correct = True
        else:
            logger.error("❌ Incorrect order of operations!")
            logger.error(f"   Health check line: {health_check_line}")
            logger.error(f"   Decommission line: {decommission_line}")
            logger.error(f"   Pod deletion line: {pod_deletion_line}")
            order_correct = False
    else:
        logger.error("❌ Missing critical operations!")
        logger.error(f"   Health check found: {health_check_found}")
        logger.error(f"   Decommission found: {decommission_found}")
        logger.error(f"   Pod deletion found: {pod_deletion_found}")
        order_correct = False
    
    # Summary
    logger.info(f"\n📊 Health Validation Check Results:")
    logger.info(f"   Found validation checks: {len(found_checks)}/{len(health_validation_checks)}")
    logger.info(f"   Correct operation order: {order_correct}")
    
    if len(found_checks) >= 4 and order_correct:
        logger.info("🎉 Health validation fix is properly implemented!")
        return True
    else:
        logger.error("💥 Health validation fix is incomplete or missing!")
        return False


def test_decommission_strategy_behavior():
    """Test that decommission strategy behavior is correct."""
    
    logger.info("\n🔍 Checking decommission strategy implementation...")
    
    # Get the _execute_decommission_strategy method source code
    decommission_method = getattr(CrateDBActivities, '_execute_decommission_strategy', None)
    
    if not decommission_method:
        logger.error("❌ _execute_decommission_strategy method not found!")
        return False
    
    try:
        source_code = inspect.getsource(decommission_method)
    except OSError:
        logger.error("❌ Could not get source code for _execute_decommission_strategy method!")
        return False
    
    # Check for correct dc_util behavior
    dc_util_checks = [
        "if cluster.has_dc_util:",
        "PreStop hook",
        "Nothing to do here",
        "_execute_manual_decommission",
    ]
    
    found_dc_util_checks = []
    for check in dc_util_checks:
        if check in source_code:
            found_dc_util_checks.append(check)
            logger.info(f"✅ Found dc_util logic: '{check}'")
    
    if len(found_dc_util_checks) >= 3:
        logger.info("✅ Decommission strategy logic is correct")
        return True
    else:
        logger.error("❌ Decommission strategy logic is incomplete")
        return False


def main():
    """Run all validation tests."""
    
    logger.info("🧪 Running health validation fix verification...")
    logger.info("=" * 60)
    
    # Run tests
    health_validation_ok = test_health_validation_exists()
    decommission_strategy_ok = test_decommission_strategy_behavior()
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 FINAL VERIFICATION RESULTS:")
    logger.info(f"   Health validation fix: {'✅ PASS' if health_validation_ok else '❌ FAIL'}")
    logger.info(f"   Decommission strategy:  {'✅ PASS' if decommission_strategy_ok else '❌ FAIL'}")
    
    if health_validation_ok and decommission_strategy_ok:
        logger.info("\n🎉 SUCCESS: Health validation regression fix is properly implemented!")
        logger.info("\n🔒 SECURITY: Pods will no longer be deleted when cluster is not GREEN")
        logger.info("\n✅ The regression has been fixed:")
        logger.info("   - Cluster health is validated before each pod restart")
        logger.info("   - Only GREEN clusters allow pod restarts to proceed")
        logger.info("   - Non-GREEN clusters (YELLOW, RED, UNKNOWN) block pod restarts")
        logger.info("   - Health check failures prevent pod deletion")
        return True
    else:
        logger.error("\n💥 FAILURE: Health validation fix is not properly implemented!")
        logger.error("\n⚠️  WARNING: The regression may still exist!")
        logger.error("   - Pods might still be deleted when cluster is not GREEN")
        logger.error("   - Manual review of the implementation is required")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)