# src/main.py
from transactions import enroll_student, assign_advisor, add_professor, add_student
from executor import execute_chains_in_parallel
from nodes import initialize_nodes

if __name__ == "__main__":
    initialize_nodes()
    # Example transactions
    chains = [
       enroll_student(5, "Elise", 101, []),
       assign_advisor(6, "Fred", [], [], "Baldi"),
       add_professor(504, "Ihler", [], 4),
       add_student(103, 7, "Greg", [], [])
    ]

    results = execute_chains_in_parallel(chains)
    print("Execution results:", results)
