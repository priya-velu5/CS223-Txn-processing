# src/executor.py

from concurrent.futures import ThreadPoolExecutor
from locks import LockManager
from concurrent.futures import ThreadPoolExecutor
from nodes import get_node_for_resource
import time
import pandas as pd
import networkx as nx

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

# Two queues: ready and delayed
ready_queue = []
delayed_queue = []

sc_graph = nx.DiGraph()


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

def execute_chains(chains):
    """
    Add chains to queues and process them, handling SC-Graph conflicts and delays.
    """
    results = {}

    for chain_id, chain in enumerate(chains):
        # Add chain to SC-Graph
        if add_chain_to_sc_graph(chain_id, chain):
            ready_queue.append((chain_id, chain))
        else:
            delayed_queue.append((chain_id, chain))

    # Process ready queue
    while ready_queue:
        chain_id, chain = ready_queue.pop(0)
        try:
            execute_chain_with_node_pools(chain, chain_id)
            results[chain_id] = True
        except Exception as e:
            results[chain_id] = False
            print(f"Chain {chain_id} failed with error {e}")

    # Process delayed queue
    reevaluate_delayed_queue()
    while delayed_queue:
        for chain_id, chain in delayed_queue[:]:
            if add_chain_to_sc_graph(chain_id, chain):
                delayed_queue.remove((chain_id, chain))
                try:
                    execute_chain_with_node_pools(chain, chain_id)
                    results[chain_id] = True
                except Exception as e:
                    results[chain_id] = False
                    print(f"Delayed chain {chain_id} failed with error {e}")

    return results


def reevaluate_delayed_queue():
    """
    Reevaluate delayed transactions to check if they can be moved to the ready queue.
    """
    for chain_id, chain in delayed_queue[:]:
        if add_chain_to_sc_graph(chain_id, chain):
            delayed_queue.remove((chain_id, chain))
            ready_queue.append((chain_id, chain))

def remove_transaction_from_graph(transaction_id):
    """
    Remove all nodes and edges of a transaction from the SC-Graph.
    """
    nodes_to_remove = [node for node in sc_graph.nodes if node.startswith(f"{transaction_id}-")]
    sc_graph.remove_nodes_from(nodes_to_remove)
    print(f"Removed transaction {transaction_id} from SC-Graph.")

def detect_cycle():
    """
    Detect if there is a cycle in the SC-Graph.
    Return True if a cycle is detected, else False.
    """
    try:
        cycle_nodes = list(nx.find_cycle(sc_graph, orientation="original"))
        cycle_weight = sum(sc_graph.edges[edge]["weight"] for edge in cycle_nodes)
        if cycle_weight > 0:  # SC-cycle (contains at least one S-edge)
            return True, cycle_nodes
        else:
            print("Cycle detected but ignored (C-cycle only).")
            return False, None
    except nx.exception.NetworkXNoCycle:
        return False, None

def has_conflict(hop, existing_node):
    """
    Determine if a conflict exists between the current hop and an existing node.
    For now, assume all hops on the same resource conflict.
    """
    resource = hop["resource"]
    _, existing_hop_id = existing_node.split("-")
    # Example: Conflict if accessing the same resource
    return resource in existing_hop_id

def add_chain_to_sc_graph(chain_id, chain):
    """
    Add a transaction chain to the SC-Graph and check for cycles.
    """
    global sc_graph
    previous_node = None  # Keep track of the last node for S-edges
    
    for hop_id, hop in enumerate(chain):
        current_node = f"{chain_id}-{hop_id}"  # Unique node identifier (transaction ID and hop ID)
        sc_graph.add_node(current_node)
        
        # Add S-edge for sequential hops in the same transaction
        if previous_node:
            sc_graph.add_edge(previous_node, current_node, weight=1)  # S-edge (weight = 1)
        
        # Check for conflicts with other transactions
        for existing_node in sc_graph.nodes:
            existing_transaction, _ = existing_node.split("-")
            if existing_transaction != str(chain_id):  # Conflict with a different transaction
                if has_conflict(hop, existing_node):  # Define `has_conflict`
                    sc_graph.add_edge(existing_node, current_node, weight=0)  # C-edge (weight = 0)
        
        previous_node = current_node  # Update the last node

        # Check for cycles after adding the edge
        has_cycle, cycles = detect_cycle()
        if has_cycle:
            print(f"Cycle detected: {cycles}")
            remove_transaction_from_graph(chain_id)  # Remove this transaction
            delayed_queue.append((chain_id, chain))  # Move to delayed queue
            return False
    
    print(f"Transaction {chain_id} successfully added to SC-Graph.")
    return True



def execute_chain_with_node_pools(chain, chain_id):
    """
    Execute a transaction chain by submitting hops to node-specific thread pools.
    """
    start_time = time.perf_counter()
    try:
        for hop_id, hop in enumerate(chain):
            node_name = get_node_for_resource(hop["resource"])
            future = node_executors[node_name].submit(execute_hop_node, hop, node_name, chain_id, hop_id)
            future.result()
        print(f"Chain {chain_id} executed successfully.")
    except Exception as e:
        print(f"Error during chain execution: {e}")
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