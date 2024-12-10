# src/executor.py

from concurrent.futures import ThreadPoolExecutor
from locks import LockManager
from concurrent.futures import ThreadPoolExecutor
from nodes import get_node_for_resource
import time
import pandas as pd

# Global variable to store the last node executed for each chain
last_node_for_chain = {}
hop_metrics = []
node_metrics_data = {
    "node1": {"hops": 0, "execution_time": 0.0},
    "node2": {"hops": 0, "execution_time": 0.0},
    "node3": {"hops": 0, "execution_time": 0.0},
}

# Thread pools for nodes
node_executors = {
    "node1": ThreadPoolExecutor(max_workers=5),
    "node2": ThreadPoolExecutor(max_workers=5),
    "node3": ThreadPoolExecutor(max_workers=5),
}

lock_manager = LockManager()

def simulate_latency(chain_id, current_node):
    """
    Simulate latency between hops if they are on different nodes within the same transaction (chain).
    """
    global last_node_for_chain

    # Retrieve the last node for the current chain (transaction)
    last_node = last_node_for_chain.get(chain_id, None)
    
    if last_node and last_node != current_node:
        print(f"Simulating latency between {last_node} and {current_node}...")
        time.sleep(1)  # Simulate a 1-second delay between different nodes (adjust as needed)
    
    # Update the last node for the current chain
    last_node_for_chain[chain_id] = current_node

def execute_hop_node(hop, node_name, chain_id, hop_id):
    """
    Execute a single hop in the thread pool of the given node.
    """
    # Simulate latency if the hop is on a different node from the last hop within the same chain
    simulate_latency(chain_id, node_name)
    start_time = time.perf_counter()
    print(f"Executing hop on resource: {hop['resource']} in node: {node_name}")
    result = hop["action"]()
    end_time = time.perf_counter()  # End time for the hop

    # Calculate hop execution time
    hop_execution_time = end_time - start_time

    # Store metrics for this hop
    hop_metrics.append({
        "Transaction ID": chain_id,
        "Hop ID": hop_id,
        "Resource": hop["resource"],
        "Node": node_name,
        "Execution Time (s)": hop_execution_time,
    })
    # Update node-level metrics
    node_metrics_data[node_name]["hops"] += 1
    node_metrics_data[node_name]["execution_time"] += hop_execution_time


    print(f"Hop on resource {hop['resource']} completed in {hop_execution_time:.4f} seconds.")
    return result

def execute_hop(hop, locks_acquired): 
    """
    Execute a single hop with locking. If the hop fails, return False.
    """
    resource = hop["resource"]
    
    # Acquire a lock for the resource
    lock_manager.acquire(resource)
    locks_acquired.append(resource)
    
    try:
        success = hop["action"]()
        print("success", success)
        if not success:
            print(f"Hop failed on resource: {resource}")
            raise Exception(f"Hop failed on resource: {resource}")
    except Exception as e:
        print(f"Error during hop execution: {e}")
        raise  # Propagate the exception to stop the chain
    return True


# def execute_chain_with_node_pools(chain, chain_id):
#     """
#     Execute a transaction chain by submitting hops to node-specific thread pools.
#     """
#     results = []
#     start_time = time.perf_counter()  # Start time for the transaction

#     try:
#         for hop in chain:
#             node_name = get_node_for_resource(hop["resource"])  # Determine the node for this hop
#             future = node_executors[node_name].submit(execute_hop_node, hop, node_name, chain_id)
#             results.append(future.result())

#             # If any hop fails, stop the chain
#             if not results[-1]:
#                 print(f"Chain failed at hop: {hop['resource']}")
#                 return False
#         print("Chain executed successfully.")
#         return True
#     except Exception as e:
#         print(f"Error during chain execution: {e}")
#         return False
def execute_chain_with_node_pools(chain, chain_id):
    """
    Execute a transaction chain by submitting hops to node-specific thread pools.
    """
    results = []
    start_time = time.perf_counter()  # Start time for the transaction
    try:
        for hop_id, hop in enumerate(chain):
            node_name = get_node_for_resource(hop["resource"])  # Determine the node for this hop
            future = node_executors[node_name].submit(execute_hop_node, hop, node_name, chain_id, hop_id)
            results.append(future.result())

            # If any hop fails, stop the chain
            if not results[-1]:
                print(f"Chain failed at hop: {hop['resource']}")
                return False
        print("Chain executed successfully.")
        return True
    finally:
        end_time = time.perf_counter()  # End time for the transaction
        transaction_latency = end_time - start_time
        print(f"Transaction {chain_id} completed in {transaction_latency:.4f} seconds.")


def execute_chains_in_parallel_with_nodes(chains):
    """
    Execute multiple transaction chains in parallel, isolating execution by nodes.
    """
    with ThreadPoolExecutor() as chain_executor:
        futures = [chain_executor.submit(execute_chain_with_node_pools, chain, chain_id) for chain_id, chain in enumerate(chains)]
        results = [future.result() for future in futures]
    return results

node_metrics = []

def collect_node_metrics():
    """
    Aggregate node-level metrics into a DataFrame.
    """
    for node, metrics in node_metrics_data.items():
        node_metrics.append({
            "Node": node,
            "Total Hops": metrics["hops"],
            "Total Execution Time (s)": metrics["execution_time"]
        })
    return pd.DataFrame(node_metrics)

def export_metrics():
    """
    Export hop and node metrics to CSV files.
    """
    # Convert hop metrics to DataFrame
    hop_metrics_df = pd.DataFrame(hop_metrics)
    hop_metrics_df.to_csv("hop_metrics.csv", index=False)
    print("Hop metrics exported to hop_metrics.csv.")

    # Convert node metrics to DataFrame
    node_metrics_df = collect_node_metrics()
    node_metrics_df.to_csv("node_metrics.csv", index=False)
    print("Node metrics exported to node_metrics.csv.")
