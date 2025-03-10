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
G.nodes["B"]["ait"]="GRA1"
G.nodes["D"]["ait"]="GRA2"
G.nodes["E"]["ait"]="GRA3"
print("G")
print("B - {}, D - {}, E - {}".format(G.nodes["B"]["ait"],G.nodes["D"]["ait"],G.nodes["E"]["ait"]))

# Define the nodes for the subgraph
nodes_to_extract = ["A", "B", "C"]

# Extract the subgraph
subgraph = G.subgraph(nodes_to_extract)
print("subgraph")
print("B - {}, D - {}, E - {}".format(G.nodes["B"]["ait"],G.nodes["D"]["ait"],G.nodes["E"]["ait"]))

# Print the edges of the subgraph
#print("Subgraph edges:", subgraph.edges())

visualize_lineage(G)

visualize_lineage(subgraph)

subgraph2 = nx.bfs_tree(G,"B")
print("subgraph2")
print("B - {}, D - {}, E - {}".format(G.nodes["B"]["ait"],G.nodes["D"]["ait"],G.nodes["E"]["ait"]))
visualize_lineage(subgraph2)

subgraph3 = G.subgraph(list(subgraph2.nodes()))
print("subgraph3")
print("B - {}, D - {}, E - {}".format(G.nodes["B"]["ait"],G.nodes["D"]["ait"],G.nodes["E"]["ait"]))
visualize_lineage(subgraph3)
