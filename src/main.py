# src/main.py

from src.nodes import initialize_nodes
from src.transactions import enroll_student
from src.executor import execute_transaction_chain

if __name__ == "__main__":
    # Initialize node data
    initialize_nodes()

    # Example transaction
    tx = enroll_student(s=8, c="101")
    execute_transaction_chain(tx)
