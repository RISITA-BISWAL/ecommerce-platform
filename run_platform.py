"""
Main Execution Script for Unified E-Commerce Data Platform (Milestone 8).

Provides entrypoint for platform CLI execution.
Usage:
    python run_platform.py --all
    python run_platform.py --generate
    python run_platform.py --audit
    python run_platform.py --pipeline
    python run_platform.py --analytics
    python run_platform.py --orchestrate
    python run_platform.py --spark
    python run_platform.py --report
"""

import sys
from src.cli import EcommercePlatformCLI

if __name__ == "__main__":
    cli = EcommercePlatformCLI()
    cli.main(sys.argv[1:])
