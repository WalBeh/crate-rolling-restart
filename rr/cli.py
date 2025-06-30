"""
CLI interface for CrateDB Kubernetes Manager with Temporal workflows.
"""

import asyncio
import os
import sys
from datetime import datetime

import click
from loguru import logger
from rich.console import Console
from rich.table import Table

from .models import RestartOptions
from .temporal_client import TemporalClient

console = Console()


def setup_logging(log_level: str) -> str:
    """
    Set up logging configuration.

    Args:
        log_level: Log level to use

    Returns:
        The log level that was set
    """
    logger.remove()  # Remove default handler

    # Format string depends on log level
    if log_level == "DEBUG":
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
    else:
        format_string = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"

    logger.add(
        sys.stderr,
        level=log_level,
        format=format_string,
        backtrace=log_level == "DEBUG",  # Only show tracebacks in DEBUG mode
        diagnose=log_level == "DEBUG",  # Only show variables in DEBUG mode
    )

    return log_level


def generate_report(result, output_format: str = "text") -> str:
    """
    Generate a report of restart results.

    Args:
        result: MultiClusterRestartResult object
        output_format: Output format (text, json, yaml)

    Returns:
        Report string
    """
    if output_format == "json":
        import json
        
        report_data = {
            "summary": {
                "total_clusters": result.total_clusters,
                "successful_clusters": result.successful_clusters,
                "failed_clusters": result.failed_clusters,
                "total_duration": result.total_duration,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat(),
            },
            "clusters": [
                {
                    "cluster": {
                        "name": r.cluster.name,
                        "namespace": r.cluster.namespace,
                        "health": r.cluster.health,
                        "replicas": r.cluster.replicas,
                        "has_prestop_hook": r.cluster.has_prestop_hook,
                        "has_dc_util": r.cluster.has_dc_util,
                    },
                    "success": r.success,
                    "duration": r.duration,
                    "restarted_pods": r.restarted_pods,
                    "total_pods": r.total_pods,
                    "error": r.error,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                }
                for r in result.results
            ]
        }
        return json.dumps(report_data, indent=2)

    elif output_format == "yaml":
        import yaml
        
        report_data = {
            "summary": {
                "total_clusters": result.total_clusters,
                "successful_clusters": result.successful_clusters,
                "failed_clusters": result.failed_clusters,
                "total_duration": result.total_duration,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat(),
            },
            "clusters": [
                {
                    "cluster": {
                        "name": r.cluster.name,
                        "namespace": r.cluster.namespace,
                        "health": r.cluster.health,
                        "replicas": r.cluster.replicas,
                        "has_prestop_hook": r.cluster.has_prestop_hook,
                        "has_dc_util": r.cluster.has_dc_util,
                    },
                    "success": r.success,
                    "duration": r.duration,
                    "restarted_pods": r.restarted_pods,
                    "total_pods": r.total_pods,
                    "error": r.error,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                }
                for r in result.results
            ]
        }
        return yaml.dump(report_data, default_flow_style=False)

    else:  # text format
        # Create summary table
        summary_table = Table(title="Restart Summary", show_header=True, header_style="bold magenta")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        summary_table.add_row("Total Clusters", str(result.total_clusters))
        summary_table.add_row("Successful", str(result.successful_clusters))
        summary_table.add_row("Failed", str(result.failed_clusters))
        summary_table.add_row("Total Duration", f"{result.total_duration:.2f}s")
        summary_table.add_row("Started At", result.started_at.strftime("%Y-%m-%d %H:%M:%S"))
        summary_table.add_row("Completed At", result.completed_at.strftime("%Y-%m-%d %H:%M:%S"))
        
        # Create details table
        details_table = Table(title="Cluster Details", show_header=True, header_style="bold magenta")
        details_table.add_column("Cluster", style="cyan")
        details_table.add_column("Namespace", style="blue")
        details_table.add_column("Success", style="green")
        details_table.add_column("Duration (s)", style="yellow")
        details_table.add_column("Pods Restarted", style="green")
        details_table.add_column("Error", style="red")
        
        for r in result.results:
            status = "[green]✓[/green]" if r.success else "[red]✗[/red]"
            details_table.add_row(
                r.cluster.name,
                r.cluster.namespace,
                status,
                f"{r.duration:.2f}",
                f"{len(r.restarted_pods)}/{r.total_pods}",
                r.error or ""
            )
        
        # Render tables to string
        from io import StringIO
        temp_console = Console(file=StringIO(), width=120)
        temp_console.print(summary_table)
        temp_console.print("\n")
        temp_console.print(details_table)
        return temp_console.file.getvalue()


async def async_main(cluster_names, kubeconfig, context, dry_run, skip_hook_warning, 
                    output_format, log_level, temporal_address, task_queue, async_execution):
    """Async main function that handles the Temporal workflow execution."""
    current_log_level = setup_logging(log_level)

    try:
        # Safety check: detect if --dry-run might have been placed incorrectly
        if not dry_run and cluster_names:
            # Check if any cluster name contains variations of dry-run flags
            dry_run_variations = [
                "--dry-run", "dry-run", "--dry", "dry", 
                "--dryrun", "dryrun", "-dry-run", "-dry",
                "--test", "test", "--simulate", "simulate"
            ]
            for cluster_name in cluster_names:
                if cluster_name.lower() in [v.lower() for v in dry_run_variations]:
                    logger.error("[CLI] ERROR: Detected potential dry-run flag in cluster names!")
                    logger.error(f"[CLI] Found '{cluster_name}' which looks like a misplaced option.")
                    logger.error("[CLI] The --dry-run flag must come BEFORE cluster names.")
                    logger.error("[CLI] Correct usage: rr --context xxx --dry-run cluster1")
                    logger.error("[CLI] Incorrect usage: rr --context xxx cluster1 --dry-run")
                    logger.error("[CLI] This prevents accidental real restarts when you intended a dry run.")
                    sys.exit(1)
            
            # Additional check for any argument starting with '--' or '-'
            for cluster_name in cluster_names:
                if cluster_name.startswith('-'):
                    logger.error(f"[CLI] ERROR: Found '{cluster_name}' in cluster names - this looks like a misplaced option!")
                    logger.error("[CLI] All options must come BEFORE cluster names.")
                    logger.error("[CLI] Run 'rr --help' to see the correct usage.")
                    sys.exit(1)

        # Check if cluster_names are provided
        if not cluster_names:
            logger.error("[CLI] No cluster names specified. You must specify at least one cluster name or 'all'.")
            sys.exit(1)

        # Handle the 'all' special case
        restart_all = False
        cluster_names_list = list(cluster_names)

        if len(cluster_names_list) == 1 and cluster_names_list[0].lower() == "all":
            if not dry_run:
                # Ask for confirmation
                console.print("[yellow]WARNING: You are about to restart ALL CrateDB clusters.[/yellow]")
                confirmation = click.prompt("Are you sure you want to proceed?", type=click.Choice(["y", "n"]), default="n")
                if confirmation.lower() != "y":
                    logger.info("[CLI] Operation cancelled by user")
                    sys.exit(0)
            restart_all = True
            # Reset cluster_names to None to find all clusters
            cluster_names_list = None

        # Create restart options
        options = RestartOptions(
            kubeconfig=kubeconfig,
            context=context,
            dry_run=dry_run,
            skip_hook_warning=skip_hook_warning,
            output_format=output_format,
            log_level=log_level,
        )

        # Connect to Temporal
        logger.info(f"[CLI] Connecting to Temporal server at {temporal_address}")
        async with TemporalClient(temporal_address, task_queue) as temporal_client:
            
            # First, discover clusters to validate they exist
            logger.info("[CLI] Discovering CrateDB clusters...")
            discovery_result = await temporal_client.discover_clusters(
                cluster_names=cluster_names_list,
                kubeconfig=kubeconfig,
                context=context,
            )
            
            if discovery_result.errors:
                for error in discovery_result.errors:
                    logger.error(f"[CLI] Discovery error: {error}")
            
            if not discovery_result.clusters:
                if restart_all:
                    logger.error("[CLI] No CrateDB clusters found in the cluster")
                else:
                    logger.error(f"[CLI] No CrateDB clusters found with names: {', '.join(cluster_names)}")
                sys.exit(1)

            logger.info(f"[CLI] Found {len(discovery_result.clusters)} CrateDB clusters")
            for cluster in discovery_result.clusters:
                logger.info(
                    f"[{cluster.name[:8]}] Cluster: {cluster.name}, Namespace: {cluster.namespace}, "
                    f"Health: {cluster.health}, Pods: {len(cluster.pods)}, "
                    f"Has PreStop Hook: {cluster.has_prestop_hook}, "
                    f"Has DC Util: {cluster.has_dc_util}"
                )

            # Issue warnings first
            if not skip_hook_warning:
                for cluster in discovery_result.clusters:
                    if not cluster.has_prestop_hook:
                        logger.warning(f"[{cluster.name[:8]}] No prestop hook detected for cluster {cluster.name}.")
                    elif not cluster.has_dc_util:
                        logger.warning(
                            f"[{cluster.name[:8]}] Prestop hook detected but no decommissioning utility (dc_util/dc-util) found for cluster {cluster.name}. "
                            f"This might be a detection issue. Use --log-level DEBUG for more details."
                        )

            # Execute restart workflow
            if async_execution:
                logger.info("[CLI] Starting cluster restart workflow asynchronously...")
                workflow_handle = await temporal_client.restart_clusters(
                    cluster_names=[c.name for c in discovery_result.clusters],
                    options=options,
                    wait_for_completion=False,
                )
                
                console.print(f"[green]Workflow started successfully![/green]")
                console.print(f"Workflow ID: {workflow_handle.id}")
                console.print(f"You can check the status using: rr status {workflow_handle.id}")
                sys.exit(0)
            else:
                logger.info("[CLI] Starting cluster restart workflow...")
                cluster_names_for_restart = [c.name for c in discovery_result.clusters]
                logger.info(f"[CLI] Passing cluster names to restart workflow: {cluster_names_for_restart}")
                result = await temporal_client.restart_clusters(
                    cluster_names=cluster_names_for_restart,
                    options=options,
                    wait_for_completion=True,
                )

                # Generate and print report
                report = generate_report(result, output_format)
                console.print(report)

                # Check for failures
                if result.failed_clusters > 0:
                    logger.warning(f"[CLI] {result.failed_clusters} cluster(s) failed to restart")
                    sys.exit(1)

                logger.success(f"[CLI] Successfully restarted {result.successful_clusters} cluster(s)")
                sys.exit(0)

    except Exception as e:
        # Create a simplified error message without tracebacks for non-DEBUG mode
        error_msg = str(e)
        if hasattr(e, "__module__") and e.__module__ != "builtins":
            error_type = e.__class__.__name__
            error_msg = f"{error_type}: {error_msg}"

        logger.error(f"Error: {error_msg}")

        # Only show detailed traceback in DEBUG mode
        if current_log_level == "DEBUG":
            logger.exception("Detailed traceback:")
        sys.exit(1)


@click.group(invoke_without_command=True)
@click.pass_context
@click.argument("cluster_names", nargs=-1)
@click.option(
    "--kubeconfig",
    help="Path to kubeconfig file",
    default=None,
)
@click.option(
    "--context",
    help="Kubernetes context to use",
    required=True,
    envvar="K8S_CONTEXT",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Only show what would be done without actually making changes",
)
@click.option(
    "--skip-hook-warning",
    is_flag=True,
    help="Skip warning about missing prestop hook or decommissioning utility",
)
@click.option(
    "--output-format",
    type=click.Choice(["text", "json", "yaml"]),
    default="text",
    help="Output format for the report",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="Log level",
)
@click.option(
    "--temporal-address",
    default="localhost:7233",
    help="Temporal server address",
    envvar="TEMPORAL_ADDRESS",
)
@click.option(
    "--task-queue",
    default="cratedb-operations",
    help="Temporal task queue name",
    envvar="TEMPORAL_TASK_QUEUE",
)
@click.option(
    "--async",
    "async_execution",
    is_flag=True,
    help="Start workflow asynchronously and return immediately",
)
def cli(ctx, cluster_names, kubeconfig, context, dry_run, skip_hook_warning, 
        output_format, log_level, temporal_address, task_queue, async_execution):
    """CrateDB Kubernetes Cluster Manager with Temporal workflows.

    CLUSTER_NAMES: Space-separated list of CrateDB cluster names to restart.
    Use 'all' to restart all clusters (requires confirmation).
    Must specify at least one cluster name or 'all'.
    
    The --context flag is REQUIRED and specifies which Kubernetes context to use.
    
    Examples:
      rr --context prod cluster1 cluster2        # Restart specific clusters
      rr --context prod all                      # Restart all clusters (with confirmation)
      rr --context prod --dry-run cluster1       # Show what would be done (options before cluster names)
      rr --context prod --async cluster1         # Start restart asynchronously
      
    Note: All options (--dry-run, --async, etc.) must come BEFORE cluster names.
    
    IMPORTANT: If you intend to do a dry run, make sure --dry-run comes before cluster names!
    The system will detect and prevent common mistakes like placing --dry-run at the end.
    """

    if ctx.invoked_subcommand is None:
        # This is the main restart command
        asyncio.run(async_main(
            cluster_names, kubeconfig, context, dry_run, skip_hook_warning,
            output_format, log_level, temporal_address, task_queue, async_execution
        ))


@cli.command()
@click.argument("workflow_id")
@click.option(
    "--temporal-address",
    default="localhost:7233",
    help="Temporal server address",
    envvar="TEMPORAL_ADDRESS",
)
@click.option(
    "--task-queue",
    default="cratedb-operations",
    help="Temporal task queue name",
    envvar="TEMPORAL_TASK_QUEUE",
)
def status(workflow_id, temporal_address, task_queue):
    """Check the status of a workflow."""
    async def check_status():
        async with TemporalClient(temporal_address, task_queue) as temporal_client:
            try:
                status_info = await temporal_client.get_workflow_status(workflow_id)
                
                table = Table(title=f"Workflow Status: {workflow_id}", show_header=True, header_style="bold magenta")
                table.add_column("Attribute", style="cyan")
                table.add_column("Value", style="green")
                
                table.add_row("Workflow ID", status_info["workflow_id"])
                table.add_row("Status", status_info["status"])
                table.add_row("Run ID", status_info["run_id"])
                table.add_row("Workflow Type", status_info["workflow_type"])
                table.add_row("Task Queue", status_info["task_queue"])
                table.add_row("Start Time", str(status_info["start_time"]))
                table.add_row("Execution Time", str(status_info["execution_time"]))
                table.add_row("Close Time", str(status_info["close_time"]) if status_info["close_time"] else "Running")
                
                console.print(table)
                
            except Exception as e:
                console.print(f"[red]Error checking workflow status: {e}[/red]")
                sys.exit(1)
    
    asyncio.run(check_status())


@cli.command()
@click.option(
    "--limit",
    default=10,
    help="Maximum number of workflows to show",
)
@click.option(
    "--temporal-address",
    default="localhost:7233",
    help="Temporal server address",
    envvar="TEMPORAL_ADDRESS",
)
@click.option(
    "--task-queue",
    default="cratedb-operations",
    help="Temporal task queue name",
    envvar="TEMPORAL_TASK_QUEUE",
)
def list_workflows(limit, temporal_address, task_queue):
    """List recent workflows."""
    async def list_wf():
        async with TemporalClient(temporal_address, task_queue) as temporal_client:
            try:
                workflows = await temporal_client.list_workflows(limit)
                
                if not workflows:
                    console.print("No workflows found.")
                    return
                
                table = Table(title="Recent Workflows", show_header=True, header_style="bold magenta")
                table.add_column("Workflow ID", style="cyan")
                table.add_column("Type", style="blue")
                table.add_column("Status", style="green")
                table.add_column("Start Time", style="yellow")
                table.add_column("Duration", style="magenta")
                
                for wf in workflows:
                    duration = "Running"
                    if wf["close_time"]:
                        start = wf["start_time"]
                        end = wf["close_time"]
                        if start and end:
                            duration = str(end - start)
                    
                    table.add_row(
                        wf["workflow_id"][:40] + "..." if len(wf["workflow_id"]) > 40 else wf["workflow_id"],
                        wf["workflow_type"].split(".")[-1],  # Just the class name
                        wf["status"],
                        str(wf["start_time"]),
                        duration
                    )
                
                console.print(table)
                
            except Exception as e:
                console.print(f"[red]Error listing workflows: {e}[/red]")
                sys.exit(1)
    
    asyncio.run(list_wf())


@cli.command()
@click.argument("workflow_id")
@click.option(
    "--temporal-address",
    default="localhost:7233",
    help="Temporal server address",
    envvar="TEMPORAL_ADDRESS",
)
@click.option(
    "--task-queue",
    default="cratedb-operations",
    help="Temporal task queue name",
    envvar="TEMPORAL_TASK_QUEUE",
)
def cancel(workflow_id, temporal_address, task_queue):
    """Cancel a running workflow."""
    async def cancel_wf():
        async with TemporalClient(temporal_address, task_queue) as temporal_client:
            try:
                await temporal_client.cancel_workflow(workflow_id)
                console.print(f"[green]Successfully cancelled workflow: {workflow_id}[/green]")
                
            except Exception as e:
                console.print(f"[red]Error cancelling workflow: {e}[/red]")
                sys.exit(1)
    
    asyncio.run(cancel_wf())


if __name__ == "__main__":
    cli()