import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# Sample data for sttm
sttm_data = {
    'src': [1, 2, 3, 4, 5, 5, 4, 5],
    'tgt': [2, 3, 4, 2, 6, 2, 7, 8],
    'trans1': ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8'],
    'trans2': ['U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'U7', 'U8']
}
sttm = pd.DataFrame(sttm_data)

# Sample data for addl_Details
addl_details_data = {
    'node': [1, 2, 3, 4, 5, 6, 7, 8],
    'bus_Elem': ['BE1', 'BE2', 'BE3', 'BE4', 'BE5', 'BE6', 'BE7', 'BE8'],
    'pde_Elem': ['PE1', 'PE2', 'PE3', 'PE4', 'PE5', 'PE6', 'PE7', 'PE8'],
    'desc': ['Desc1', 'Desc2', 'Desc3', 'Desc4', 'Desc5', 'Desc6', 'Desc7', 'Desc8']
}
addl_Details = pd.DataFrame(addl_details_data)

# Create a directed graph from sttm
G = nx.DiGraph()
for _, row in sttm.iterrows():
    G.add_edge(row['src'], row['tgt'], trans1=row['trans1'], trans2=row['trans2'])

# Function to find all paths
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

# Find all paths starting from node 1
start_node = 5
all_paths = find_all_paths_iterative(G, start_node)

# Create the resulting DataFrame
rows = []
for path in all_paths:
    row = {}
    row['path'] = " -> ".join(map(str, path))
    row['levels'] = len(path) - 1
    row['start'] = path[0]
    row['end'] = path[-1]
    
    # Add level columns
    for i, node in enumerate(path, 1):
        row[f'level{i}'] = node
        
        # Match with addl_Details
        details = addl_Details[addl_Details['node'] == node].iloc[0]
        row[f'bus_Elem{i}'] = details['bus_Elem']
        row[f'pde_Elem{i}'] = details['pde_Elem']
        row[f'desc{i}'] = details['desc']
    
    # Add transaction columns
    for i in range(len(path) - 1):
        edge_data = G[path[i]][path[i + 1]]
        row[f'trans{i + 1}'] = f"{edge_data['trans1']} -> {edge_data['trans2']}"
    
    rows.append(row)

# Create DataFrame from rows
result_df = pd.DataFrame(rows)

# Display the DataFrame
print("\nGenerated DataFrame:")
print(result_df)

# Draw the graph
plt.figure(figsize=(8, 6))  # Set the figure size
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_size=700, node_color='skyblue', 
        font_size=12, font_color='darkblue', edge_color='gray')
edge_labels = {(u, v): f"{d['trans1']}/{d['trans2']}" for u, v, d in G.edges(data=True)}
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red')

# Show the plot
plt.title('NetworkX Graph Visualization')
plt.show()
