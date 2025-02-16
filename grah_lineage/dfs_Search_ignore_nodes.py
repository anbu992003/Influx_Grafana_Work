import networkx as nx

def dfs_ignore_nodes(graph, start_node, ignore_nodes=None):
    """Perform DFS on a graph while ignoring specified nodes.

    Args:
        graph (nx.Graph): The graph to traverse.
        start_node (any): The starting node for DFS.
        ignore_nodes (set, optional): Nodes to ignore during traversal. Defaults to an empty set.

    Yields:
        any: Nodes visited during DFS traversal.
    """
    if ignore_nodes is None:
        ignore_nodes = set()  # Default to an empty set

    visited = set()
    stack = [start_node]

    while stack:
        node = stack.pop()
        if node in visited or node in ignore_nodes:
            continue  # Skip ignored nodes
        visited.add(node)
        yield node  # Process the node

        # Push neighbors onto the stack in reverse order for correct DFS order
        stack.extend(reversed(list(graph.neighbors(node))))

# Example Graph
G = nx.Graph()
edges = [(1, 2), (1, 3), (2, 4), (3, 5), (5, 6)]
G.add_edges_from(edges)

# Run DFS with and without ignored nodes
dfs_result_default = list(dfs_ignore_nodes(G, start_node=1))  # No ignored nodes
dfs_result_ignored = list(dfs_ignore_nodes(G, start_node=1, ignore_nodes={3, 5}))

print("DFS Traversal (No Ignored Nodes):", dfs_result_default)
print("DFS Traversal (Ignoring Nodes 3 & 5):", dfs_result_ignored)
