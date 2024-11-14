import networkx as nx
import matplotlib.pyplot as plt

# Function to add edges and create graph
def create_graph(edges):
    G = nx.DiGraph()  # Directed graph
    G.add_edges_from(edges)
    return G

# Function to perform DFS and find all paths
def dfs_paths(graph, start, end, path=None):
    if path is None:
        path = [start]
    if start == end:
        return [path]
    paths = []
    for node in graph.neighbors(start):
        if node not in path:
            new_paths = dfs_paths(graph, node, end, path + [node])
            for p in new_paths:
                paths.append(p)
    return paths

# Function to visualize the graph
def visualize_graph(graph, paths=None):
    pos = nx.spring_layout(graph)  # Positions for all nodes
    plt.figure(figsize=(8, 6))
    
    # Draw nodes and edges
    nx.draw(graph, pos, with_labels=True, node_color='skyblue', node_size=2000, font_size=15, font_weight='bold', arrows=True)
    
    # Highlight paths if provided
    if paths:
        for path in paths:
            edges_in_path = [(path[i], path[i+1]) for i in range(len(path)-1)]
            nx.draw_networkx_edges(graph, pos, edgelist=edges_in_path, edge_color='red', width=2)
    
    plt.show()

# Define the edges of the graph (example)
edges = [
    ('A', 'B'), ('A', 'C'), ('B', 'D'), ('C', 'D'), ('C', 'E'), ('D', 'E'), ('E', 'F')
]

# Create the graph
graph = create_graph(edges)

# Define start and end nodes
start_node = 'A'
end_node = 'F'

# Perform DFS to find all paths from start to end node
paths = dfs_paths(graph, start_node, end_node)

# Print all paths
print("All paths from {} to {}:".format(start_node, end_node))
for path in paths:
    print(" -> ".join(path))

# Visualize the graph and highlight paths
visualize_graph(graph, paths)
