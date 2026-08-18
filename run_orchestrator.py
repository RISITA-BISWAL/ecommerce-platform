from src.orchestrator import build_ecommerce_dag

def main():
    print("==================================================================")
    print("        NATIVE WORKFLOW ORCHESTRATOR - E-COMMERCE DAG         ")
    print("==================================================================\n")

    orch = build_ecommerce_dag()
    summary = orch.run_dag()

    print("\n------------------------------------------------------------------")
    print("ORCHESTRATION EXECUTION SUMMARY:")
    print(f"  Total Tasks  : {summary['total_tasks']}")
    print(f"  Overall Pass : {summary['passed']}")
    print(f"  State Counts : {summary['state_counts']}")
    print("------------------------------------------------------------------")
    print("State persisted to: 'data/processed/orchestrator_state.json'")
    print("==================================================================")

if __name__ == "__main__":
    main()
