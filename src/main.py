# # src/main.py

from transactions import enroll_student, assign_advisor, add_professor, add_student, remove_student_from_classes
from executor import execute_chains, export_metrics
from nodes import initialize_nodes

if __name__ == "__main__":
   # Initialize data
   initialize_nodes()

   # Define chains
   chains = [
      # enroll_student(5, "Elise", 101, []),
      # assign_advisor(6, "Fred", [], [], "Baldi"),
      # add_professor(504, "Ihler", [], 4),
      add_student(102, 7, "Greg", [], []),
      remove_student_from_classes(3),  # SC-cycle causing transaction
   ]

   # Execute chains
   # results = execute_chains_in_parallel_with_nodes(chains)
   results = execute_chains(chains)
   print("Execution results:", results)

   export_metrics()
