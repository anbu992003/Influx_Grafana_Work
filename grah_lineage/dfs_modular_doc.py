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

# Function to find leaf nodes for a given node
def find_leaf_nodes(G, start_node):
    """
    Finds all leaf nodes (nodes with no outgoing edges) that are descendants of a given start node.

    Args:
        G (networkx.DiGraph): The directed graph.
        start_node (str): The node to start the search from.

    Returns:
        list: A list of leaf node names.
    """
    leaf_nodes = []
    for node in nx.descendants(G, start_node):
        if G.out_degree(node) == 0:
            leaf_nodes.append(node)
    return leaf_nodes

# Function to create a graph
def create_graph(df):
    """
    Creates a directed graph from a pandas DataFrame representing source and destination nodes.

    Args:
        df (pandas.DataFrame): DataFrame containing source and destination node information.

    Returns:
        networkx.DiGraph: A directed graph representing the relationships in the DataFrame.
    """
    G = nx.DiGraph()
    for index, row in df.iterrows():
        src_node = f"{row['src_Server']}|{row['src_datbase']}|{row['src_feedpath']}|{row['src_feed']}|{row['src_pde']}"
        dst_node = f"{row['dst_Server']}|{row['dst_datbase']}|{row['dst_feedpath']}|{row['dst_feed']}|{row['dst_pde']}"
        G.add_edge(src_node, dst_node)
    return G

# Function to identify orphan nodes (nodes with no predecessors)
def identify_orphans(G):
    """
    Identifies nodes in a directed graph that have no incoming edges (orphan nodes).

    Args:
        G (networkx.DiGraph): The directed graph.

    Returns:
        list: A list of orphan node names.
    """
    orphans = [node for node in G.nodes if G.in_degree(node) == 0]
    return orphans

# Function to identify cycles in the graph
def identify_cycles(G):
    """
    Identifies cycles in a directed graph.

    Args:
        G (networkx.DiGraph): The directed graph.

    Returns:
        list: A list of cycles, where each cycle is a list of edges. Returns an empty list if no cycles are found.
    """
    try:
        cycles = list(nx.find_cycle(G, orientation='original'))
    except nx.NetworkXNoCycle:
        cycles = []
    return cycles

# Function to print the lineage using depth-first search for a given src_pde
def print_lineage_dfs(G, start_pde):
    """
    Performs a depth-first search to find the lineage starting from a node containing the specified src_pde.

    Args:
        G (networkx.DiGraph): The directed graph.
        start_pde (str): The src_pde to search for in node names.

    Returns:
        list: A list of edges representing the lineage, or an empty list if the start_pde is not found.
    """
    lineage = []
    for node in G.nodes:
        if node.endswith(start_pde):
            lineage = list(nx.dfs_edges(G, source=node))
            break
    return lineage

# Function to generate lineage visualization for the feed
def visualize_lineage(G):
    """
    Visualizes the directed graph using matplotlib.

    Args:
        G (networkx.DiGraph): The directed graph to visualize.
    """
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color='skyblue', font_size=10, font_weight='bold', arrows=True)
    plt.show()

def are_graphs_disjoint(G1, G2):
    """
    Checks if two graphs are disjoint (i.e., no common nodes).
    
    Args:
        G1 (networkx.Graph or networkx.DiGraph): First graph.
        G2 (networkx.Graph or networkx.DiGraph): Second graph.
    
    Returns:
        bool: True if disjoint, False otherwise.
    """
    return set(G1.nodes).isdisjoint(set(G2.nodes))


def merge_graphs(G1, G2):
    """
    Merges two graphs into a single graph.
    
    Args:
        G1 (networkx.Graph or networkx.DiGraph): First graph.
        G2 (networkx.Graph or networkx.DiGraph): Second graph.
    
    Returns:
        networkx.Graph or networkx.DiGraph: The merged graph.
    """
    return nx.compose(G1, G2)
#merged_graph = merge_graphs(G1, G2)
#print("Merged Graph Nodes with Attributes:", merged_graph.nodes(data=True))


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
