import networkx as nx
import matplotlib.pyplot as plt

# Create a directed graph
G = nx.DiGraph()

# Add edges with optional labels (weights or attributes)
G.add_edge("A", "B", weight=5)
G.add_edge("A", "C", weight=3)
G.add_edge("B", "D", weight=2)
G.add_edge("C", "D", weight=4)
G.add_edge("D", "E", weight=1)

# Position the nodes using spring layout for better visualization
pos = nx.spring_layout(G)

# Draw the nodes and edges
plt.figure(figsize=(8, 6))
nx.draw(
    G, pos, with_labels=True, node_color="skyblue", 
    node_size=2000, font_size=15, font_color="darkblue", 
    edge_color="gray", arrowsize=20
)

# Draw edge labels (e.g., weights)
edge_labels = nx.get_edge_attributes(G, 'weight')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="red")

# Add title and display the plot
plt.title("Directed Graph Visualization", fontsize=16)
plt.show()
