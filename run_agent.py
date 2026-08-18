"""
Standalone Runner Script for Agentic Data Engineering Assistant (Milestone 11).

Provides entrypoint for interacting with the platform agent via single-query or interactive REPL mode.

Usage:
    python run_agent.py --query "What is the total revenue by product category?"
    python run_agent.py --interactive
"""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.agent import DataPlatformAgent


def run_interactive_agent():
    """Run interactive REPL session with the Agentic Data Engineering Assistant."""
    agent = DataPlatformAgent()
    print("============================================================")
    print("[Agentic Data Engineering Assistant - Milestone 11]")
    print("============================================================")
    print("Type your question in plain English, or type 'exit' or 'quit' to quit.\n")

    while True:
        try:
            prompt = input("User > ").strip()
            if not prompt:
                continue
            if prompt.lower() in ("exit", "quit", "q"):
                print("Goodbye!")
                break

            response = agent.ask(prompt)
            print(f"\n[Agent Response]\n{response}\n")
            print("-" * 60)
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye!")
            break


def main():
    parser = argparse.ArgumentParser(
        description="Agentic Data Engineering Assistant CLI (Milestone 11)"
    )
    parser.add_argument(
        "-q", "--query", type=str, help="Natural-language question to ask the agent"
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true", help="Launch interactive REPL mode"
    )

    args, unknown = parser.parse_known_args()

    # If extra positional arguments exist (e.g. `python run_agent.py "What is revenue?"`)
    if not args.query and not args.interactive and unknown:
        args.query = " ".join(unknown)

    agent = DataPlatformAgent()

    if args.interactive:
        run_interactive_agent()
    elif args.query:
        print(f"[Query]: {args.query}\n")
        response = agent.ask(args.query)
        print(response)
    else:
        # Default to interactive mode if no arguments provided
        run_interactive_agent()


if __name__ == "__main__":
    main()
