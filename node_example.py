"""
example_usage.py - Example usage of the RaftNode implementation

This demonstrates how to set up a simple 3-node Raft cluster and perform operations.
"""

import asyncio
from node_metadata import NodeMetadata
from node import RaftNode


async def main():
    """Run a simple 3-node Raft cluster."""
    
    # Define cluster nodes
    nodes_metadata = [
        NodeMetadata("localhost", 5001),
        NodeMetadata("localhost", 5002),
        NodeMetadata("localhost", 5003),
    ]
    
    # Create Raft nodes
    nodes = [RaftNode(metadata, nodes_metadata) for metadata in nodes_metadata]
    
    # Start all nodes
    print("Starting Raft cluster...")
    for node in nodes:
        await node.start()
    
    # Give cluster time to elect a leader
    await asyncio.sleep(5)
    
    # Print states
    for i, node in enumerate(nodes):
        state = node.get_state()
        print(f"\nNode {i+1} ({node._get_node_id()}):")
        print(f"  State: {state['state']}")
        print(f"  Term: {state['term']}")
        print(f"  Log Length: {state['log_length']}")
    
    # Find the leader
    leader = None
    for node in nodes:
        if node.state.value == "leader":
            leader = node
            break
    
    if leader:
        print(f"\n✓ Leader elected: {leader._get_node_id()}")
        
        # Append some entries
        print("\nAppending entries to leader...")
        await leader.append_entry("SET name Alice")
        await leader.append_entry("SET age 30")
        await leader.append_entry("GET name")
        
        # Give time for replication
        await asyncio.sleep(2)
        
        # Print final states
        print("\n--- Final State Machine States ---")
        for i, node in enumerate(nodes):
            state = node.get_state()
            print(f"\nNode {i+1}:")
            print(f"  State Machine: {state['state_machine']}")
    else:
        print("✗ No leader elected after timeout")
    
    # Stop all nodes
    print("\nShutting down cluster...")
    for node in nodes:
        await node.stop()
    
    print("Done!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")