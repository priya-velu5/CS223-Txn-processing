# # src/main.py

from transactions import enroll_student, assign_advisor, add_professor, add_student
from executor import execute_chains_in_parallel_with_nodes, export_metrics
from nodes import initialize_nodes

if __name__ == "__main__":
    # Initialize data
    initialize_nodes()

    # Define chains
    chains = [
       enroll_student(5, "Elise", 101, []),
       assign_advisor(6, "Fred", [], [], "Baldi"),
       add_professor(504, "Ihler", [], 4),
       add_student(103, 7, "Greg", [], [])
    ]

    # Execute chains
    results = execute_chains_in_parallel_with_nodes(chains)
    print("Execution results:", results)

    export_metrics()
