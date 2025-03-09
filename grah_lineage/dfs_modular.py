import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from io import StringIO

# Define the table data as a string
table_data = """
src_Server	src_datbase	src_feedpath	src_feed	src_pde	dst_Server	dst_datbase	dst_feedpath	dst_feed	dst_pde
Abc	def	ghi	jkl	mno	pqr	stu	vwx	yza	bcd
cde	fgh	ijk	lmn	opq	rst	uvw	xyz	abc	def
"""

# Read the data into a pandas DataFrame
data = StringIO(table_data)
df = pd.read_csv(data, sep='\t')
#print(df.columns)
#exit(-1)

# Function to create a graph
def create_graph(df):
    G = nx.DiGraph()
    for index, row in df.iterrows():
        src_node = f"{row['src_Server']}|{row['src_datbase']}|{row['src_feedpath']}|{row['src_feed']}|{row['src_pde']}"
        dst_node = f"{row['dst_Server']}|{row['dst_datbase']}|{row['dst_feedpath']}|{row['dst_feed']}|{row['dst_pde']}"
        G.add_edge(src_node, dst_node)
    return G

# Function to identify orphan nodes (nodes with no predecessors)
def identify_orphans(G):
    orphans = [node for node in G.nodes if G.in_degree(node) == 0]
    return orphans

# Function to identify cycles in the graph
def identify_cycles(G):
    try:
        cycles = list(nx.find_cycle(G, orientation='original'))
    except nx.NetworkXNoCycle:
        cycles = []
    return cycles

# Function to print the lineage using depth-first search for a given src_pde
def print_lineage_dfs(G, start_pde):
    lineage = []
    for node in G.nodes:
        if node.endswith(start_pde):
            lineage = list(nx.dfs_edges(G, source=node))
            break
    return lineage

# Function to generate lineage visualization for the feed
def visualize_lineage(G):
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='skyblue', font_size=10, font_weight='bold', arrows=True)
    plt.show()


# Create graph
graph = create_graph(df)

# Identify orphans
orphans = identify_orphans(graph)
print("Orphans:", orphans)

# Identify cycles
cycles = identify_cycles(graph)
print("Cycles:", cycles)

# Print lineage using DFS for a given src_pde
src_pde = 'mno'  # Example src_pde
lineage = print_lineage_dfs(graph, src_pde)
print("Lineage for src_pde 'mno':", lineage)

# Visualize lineage
visualize_lineage(graph)
