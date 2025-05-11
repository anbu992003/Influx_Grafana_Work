import networkx as nx
import matplotlib.pyplot as plt
from itertools import product

# Create a directed graph
G = nx.DiGraph()

# Add 7 nodes labeled A to G
nodes = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
G.add_nodes_from(nodes)

# Add dense connections (all possible edges, excluding self-loops)
'''for u, v in product(nodes, nodes):
    if u != v:  # Exclude self-loops
        G.add_edge(u, v)'''
for i in range(0,len(nodes)):
    for j in range(i+1,len(nodes)):
        G.add_edge(nodes[i], nodes[j])


# Depth-First Search to find all paths from A to G
def dfs_paths(graph, start, end, path=None, all_paths=None):
    if path is None:
        path = [start]
    if all_paths is None:
        all_paths = []
    
    if start == end:
        all_paths.append(path[:])
    else:
        for neighbor in graph.neighbors(start):
            if neighbor not in path:  # Avoid cycles
                path.append(neighbor)
                dfs_paths(graph, neighbor, end, path, all_paths)
                path.pop()
    
    return all_paths

# Find all paths from A to G
paths = dfs_paths(G, 'A', 'G')

# Print the paths
print("All paths from A to G:")
for i, path in enumerate(paths, 1):
    print(f"Path {i}: {' -> '.join(path)}")

# Visualize the digraph
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(G, seed=42)  # Fixed layout for reproducibility
nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=500, font_size=12, font_weight='bold', arrowsize=15, edge_color='gray')
plt.title("Directed Graph with Nodes A-G and Dense Connections")

# Save the plot to a file
plt.savefig('digraph.png', format='png', dpi=300, bbox_inches='tight')