# src/main.py
from src.transactions import enroll_student
from src.executor import execute_chains_in_parallel
from src.nodes import initialize_nodes

if __name__ == "__main__":
    initialize_nodes()
    # Example transactions
    chains = [
        enroll_student(s=8, c=101),
        enroll_student(s=9, c=102),
    ]

    results = execute_chains_in_parallel(chains)
    print("Execution results:", results)
