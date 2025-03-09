import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from io import StringIO

# Function to generate lineage visualization for the feed
def visualize_lineage(G):
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='skyblue', font_size=10, font_weight='bold', arrows=True)
    plt.show()


# Example main graph
G = nx.DiGraph()
edges = [("A", "B"), ("B", "C"), ("C", "D"), ("E", "F"), ("F", "G"),("D","C"),("C","B")]
G.add_edges_from(edges)

# Define the nodes for the subgraph
nodes_to_extract = ["A", "B", "C"]

# Extract the subgraph
subgraph = G.subgraph(nodes_to_extract)

# Print the edges of the subgraph
print("Subgraph edges:", subgraph.edges())

visualize_lineage(G)

visualize_lineage(subgraph)

subgraph2 = nx.bfs_tree(G,"B")
visualize_lineage(subgraph2)

subgraph3 = G.subgraph(list(subgraph2.nodes()))
visualize_lineage(subgraph3)
