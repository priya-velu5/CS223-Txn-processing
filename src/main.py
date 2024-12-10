# # src/main.py

from transactions import enroll_student, assign_advisor, add_professor, add_student, enroll_existing_student
from executor import execute_chains, export_metrics
from nodes import initialize_nodes

if __name__ == "__main__":
   # Initialize data
   initialize_nodes()

   # Define chains
   chains = [
      enroll_student(5, "Elise", 101, []),
      assign_advisor(6, "Fred", [], [], "Baldi"),
      enroll_existing_student(2, 103),  # SC-cycle causing transaction
      add_professor(504, "Ihler", [], 4),
      add_student(103, 7, "Greg", [], [])
   ]

   # Execute chains
   # results = execute_chains_in_parallel_with_nodes(chains)
   results = execute_chains(chains)
   print("Execution results:", results)

   export_metrics()
