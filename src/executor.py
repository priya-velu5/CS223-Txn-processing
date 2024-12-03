# src/executor.py

from concurrent.futures import ThreadPoolExecutor
from locks import LockManager
from concurrent.futures import ThreadPoolExecutor
from nodes import get_node_for_resource
import time

# Global variable to store the last node executed
last_node = None


# Thread pools for nodes
node_executors = {
    "node1": ThreadPoolExecutor(max_workers=5),
    "node2": ThreadPoolExecutor(max_workers=5),
    "node3": ThreadPoolExecutor(max_workers=5),
}

lock_manager = LockManager()


def execute_hop_node(hop, node_name):
    """
    Execute a single hop in the thread pool of the given node.
    """
    print(f"Executing hop on resource: {hop['resource']} in node: {node_name}")
    return hop["action"]()

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


def execute_chain_with_node_pools(chain):
    """
    Execute a transaction chain by submitting hops to node-specific thread pools.
    """
    results = []
    try:
        for hop in chain:
            node_name = get_node_for_resource(hop["resource"])  # Determine the node for this hop
            future = node_executors[node_name].submit(execute_hop_node, hop, node_name)
            results.append(future.result())

            # If any hop fails, stop the chain
            if not results[-1]:
                print(f"Chain failed at hop: {hop['resource']}")
                return False
        print("Chain executed successfully.")
        return True
    except Exception as e:
        print(f"Error during chain execution: {e}")
        return False


def execute_chains_in_parallel_with_nodes(chains):
    """
    Execute multiple transaction chains in parallel, isolating execution by nodes.
    """
    with ThreadPoolExecutor() as chain_executor:
        futures = [chain_executor.submit(execute_chain_with_node_pools, chain) for chain in chains]
        results = [future.result() for future in futures]
    return results
