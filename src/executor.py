# src/executor.py

from concurrent.futures import ThreadPoolExecutor
from src.locks import LockManager

lock_manager = LockManager()

def execute_hop(hop, locks_acquired):
    """
    Execute a single hop with locking. If the hop fails, return False.
    """
    resource = hop["resource"]
    
    # Acquire a lock for the resource
    lock_manager.acquire(resource)
    locks_acquired.append(resource)
    
    try:
        # Execute the action defined in the hop
        success = hop["action"]()
        if not success:
            print(f"Hop failed on resource: {resource}")
            raise Exception(f"Hop failed on resource: {resource}")
    except Exception as e:
        print(f"Error during hop execution: {e}")
        raise  # Propagate the exception to the chain executor
    return True

def execute_chain(chain):
    """
    Execute a transaction chain, ensuring all-or-nothing atomicity.
    If any hop fails, the entire chain stops and rolls back.
    """
    locks_acquired = []
    try:
        for hop in chain:
            success = execute_hop(hop, locks_acquired)
            if not success:
                # Stop chain execution on failure
                raise Exception("Transaction chain failed.")
        print("Transaction chain completed successfully.")
        return True
    except Exception as e:
        print(f"Chain execution stopped: {e}")
        lock_manager.release_all(locks_acquired)  # Rollback locks
        return False
    finally:
        lock_manager.release_all(locks_acquired)  # Ensure cleanup


def execute_chains_in_parallel(chains):
    """
    Execute multiple chains in parallel.
    """
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(execute_chain, chain) for chain in chains]
        results = [future.result() for future in futures]
    return results
