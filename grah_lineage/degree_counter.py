import networkx as nx

# Create a directed graph
G = nx.DiGraph()

# Add nodes and edges to the graph (example)
G.add_edges_from([(1, 2), (1, 3), (2, 4), (3, 4), (4, 5)])

def find_all_neighbors(G, start_node):
    # Dictionary to store nodes with their in-degree and out-degree
    nodes_info = {}

    # Initialize the set of visited nodes and the queue with the start node
    visited = set()
    queue = [start_node]

    while queue:
        current_node = queue.pop(0)
        
        if current_node not in visited:
            visited.add(current_node)

            # Get the in-degree and out-degree of the current node
            in_degree = G.in_degree(current_node)
            out_degree = G.out_degree(current_node)

            # Add the node information to the dictionary
            nodes_info[current_node] = {'in_degree': in_degree, 'out_degree': out_degree}

            # Add neighbors to the queue
            for neighbor in G.successors(current_node):
                if neighbor not in visited:
                    queue.append(neighbor)

    return nodes_info

# Define the start node
start_node = 1

# Find all neighbors and their in-degree and out-degree
result = find_all_neighbors(G, start_node)

# Print the result
for node, info in result.items():
    print(f"Node {node}: In-Degree = {info['in_degree']}, Out-Degree = {info['out_degree']}")
