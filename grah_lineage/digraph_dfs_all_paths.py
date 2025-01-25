import networkx as nx
import matplotlib.pyplot as plt

# Create a directed graph
G = nx.DiGraph()
# Add edges to the graph
G.add_edges_from([(1, 2), (2, 3), (3, 4), (4, 2), (4, 5), (5, 6), (5, 2), (4, 7), (5, 8)])

def find_all_paths_iterative(graph, start_node, max_depth=1000):
    paths = []
    stack = [(start_node, [start_node], 0)]  # (current_node, current_path, depth)

    while stack:
        current_node, current_path, depth = stack.pop()

        # Stop if the depth exceeds the maximum depth
        if depth > max_depth:
            continue

        # If current node has no outgoing edges (leaf node)
        if not list(graph.successors(current_node)):
            paths.append(current_path)
        else:
            # Iterate over all neighbors
            for neighbor in graph.successors(current_node):
                if neighbor not in current_path:  # Avoid cycles
                    stack.append((neighbor, current_path + [neighbor], depth + 1))
    
    return paths

# Find all possible complete paths starting from node 1
start_node = 1
all_paths = find_all_paths_iterative(G, start_node)

# Print all complete paths
print("All Complete Paths:")
for path in all_paths:
    print(" -> ".join(map(str, path)))

# Draw the graph
plt.figure(figsize=(8, 6))  # Set the figure size
nx.draw(G, with_labels=True, node_size=700, node_color='skyblue', 
        font_size=15, font_color='darkblue', edge_color='gray')

# Show the plot
plt.title('NetworkX Graph Visualization')
plt.show()
