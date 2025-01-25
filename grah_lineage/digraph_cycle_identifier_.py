import networkx as nx
import matplotlib.pyplot as plt

# Create a directed graph
G = nx.DiGraph()
G.add_edges_from([(1, 2), (2, 3), (3, 4), (4, 2), (4, 5),(5,6),(5,2),(4, 7),(5,8)])

# Find all cycles in the graph
cycles = list(nx.simple_cycles(G))

print("Cycles in the graph:", cycles)


# Perform Depth First Search (DFS)
dfs_edges = list(nx.dfs_edges(G, source=1))  # You can specify the starting node
dfs_nodes = list(nx.dfs_preorder_nodes(G, source=1))

print("DFS Edges:", dfs_edges)
print("DFS Nodes:", dfs_nodes)



# Draw the graph
plt.figure(figsize=(8, 6))  # Optional: Set the figure size
nx.draw(G, with_labels=True, node_size=700, node_color='skyblue', 
        font_size=15, font_color='darkblue', edge_color='gray')

# Show the plot
plt.title('NetworkX Graph Visualization')
plt.show()
