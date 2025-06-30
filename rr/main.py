"""
Main entry point for CrateDB Kubernetes Manager with Temporal workflows.
"""

from .cli import cli

def main():
    """Main entry point that delegates to the CLI."""
    cli()

if __name__ == "__main__":
    main()