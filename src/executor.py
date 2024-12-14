# src/executor.py

from concurrent.futures import ThreadPoolExecutor
from locks import LockManager
from concurrent.futures import ThreadPoolExecutor
from nodes import get_node_for_resource
import time
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt


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

sc_graph = nx.Graph()


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
    results = {}

    for chain_id, chain in enumerate(chains):
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
            delayed_queue.append((chain_id, chain))

    # Process delayed queue with proper delay
    while delayed_queue:
        chain_id, chain = delayed_queue.pop(0)
        try:
            execute_chain_with_node_pools(chain, chain_id)
            results[chain_id] = True
        except Exception as e:
            results[chain_id] = False
            print(f"Chain {chain_id} failed with error {e}")
    
    return results


def remove_transaction_from_graph(transaction_id):
    """
    Remove all nodes and edges of a transaction from the SC-Graph.
    """
    global sc_graph
    nodes_to_remove = [node for node in sc_graph.nodes if node.startswith(f"T{transaction_id+1}-")]
    sc_graph.remove_nodes_from(nodes_to_remove)
    print(f"Removed transaction {transaction_id+1} from SC-Graph.")


def detect_cycle():
    """
    Detect if there is a cycle in the SC-Graph.
    Return True, [cycle_nodes] if a cycle is detected, else False, [].
    """
    global sc_graph
    # print(sc_graph.edges(data=True))
    try:
        cycle_nodes = list(nx.simple_cycles(sc_graph))
        cycle_weight = sum(sc_graph[edge[0]][edge[1]].get("weight") for edge in cycle_nodes)
        print(cycle_nodes)
        if cycle_weight > 0:    # contains at least one S-edge (SC-cycle)
            print(f"SC-cycle detected with total weight {cycle_weight}")
            return True, cycle_nodes
        if len(cycle_nodes) == 0:
            print("NO CYCLES FOUND")
            return False, []
        else:
            print("JUST C-CYCLE")
            return False, []
    except nx.exception.NetworkXNoCycle:
        print("NO CYCLES FOUND")
        return False, []
    

def has_conflict(node_table, node_operation, current_table, current_operation):
    """
    Determine if a conflict exists between the current hop and an existing node.
    For now, assume all hops on the same resource conflict.
    """
    if node_table == current_table and node_operation == 'read' and (current_operation == 'insert' or current_operation == "update"):
        return True
    elif node_table == current_table and (node_operation == 'insert' or node_operation == "update") and current_operation == 'read':
        return True
    elif node_table == current_table and node_operation == 'update' and current_operation == 'update':
        return True
    else:
        return False

def add_chain_to_sc_graph(chain_id, chain):
    """
    Add a transaction chain to the SC-Graph and check for cycles.
    """
    global sc_graph

    # Keep track of the last node for S-edges
    previous_node1 = None
    previous_node2 = None

    for _, hop in enumerate(chain):
        current_node1 = f"T{chain_id+1}-1.{hop['resource'].split('-')[0]}.{hop['operation']}"  # Unique node identifier (transaction-duplicate.table.operation)
        sc_graph.add_node(current_node1)
        print(f"NODE {current_node1}")
        # duplicate node
        current_node2 = f"T{chain_id+1}-2.{hop['resource'].split('-')[0]}.{hop['operation']}"
        sc_graph.add_node(current_node2)
        print(f"NODE {current_node2}")
        
        # Add S-edge for sequential hops in the same transaction
        if previous_node1 and previous_node2:
            sc_graph.add_edge(previous_node1, current_node1, weight=1)  # S-edge (weight = 1)
            print(f"    S-EDGE {previous_node1}=={current_node1}")
            sc_graph.add_edge(previous_node2, current_node2, weight=1)
            print(f"    S-EDGE {previous_node2}=={current_node2}")
        
        # Check for conflicts with other transactions
        for node in sc_graph.nodes:
            if node != current_node1:
                _, node_table, node_operation  = node.split(".")
                _, current_table1, current_operation1  = current_node1.split(".")

                # Check for conflicts between nodes (no self-loops, no duplicate edges)
                if has_conflict(node_table, node_operation, current_table1, current_operation1):
                    if not sc_graph.has_edge(node, current_node1) and not sc_graph.has_edge(current_node1, node):
                        sc_graph.add_edge(node, current_node1, weight=0)  # C-edge
                        print(f"    C-EDGE {node}--{current_node1}")
            
            if node != current_node2:
                _, node_table, node_operation  = node.split(".")
                _, current_table2, current_operation2  = current_node2.split(".")

                if has_conflict(node_table, node_operation, current_table2, current_operation2):
                    if not sc_graph.has_edge(node, current_node2) and not sc_graph.has_edge(current_node2, node):
                        sc_graph.add_edge(node, current_node2, weight=0)  # C-edge
                        print(f"    C-EDGE {node}--{current_node2}")

        
        previous_node1 = current_node1
        previous_node2 = current_node2

        # Check for cycles after adding the edge
        has_cycle, cycles = detect_cycle()
        if has_cycle:
            print(f"Cycle detected: {cycles}")
            remove_transaction_from_graph(chain_id)
            delayed_queue.append((chain_id, chain))
            return False
    
    print(f"Transaction {chain_id+1} successfully added to SC-Graph.")
    nx.draw(sc_graph, with_labels=True)
    plt.show()
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
        print(f"Chain {chain_id+1} executed successfully.")
    except Exception as e:
        print(f"Error during chain execution: {e}")
    finally:
        end_time = time.perf_counter()  # End time for the transaction
        transaction_latency = end_time - start_time
        print(f"Transaction {chain_id+1} completed in {transaction_latency:.4f} seconds.")


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