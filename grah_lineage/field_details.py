
#Perform distinct record counts.
#Create a new DataFrame with unique values.
#Create a NetworkX directed graph.
#Implement a depth-first search (DFS) with batch writing for paths.
#Create a new DataFrame of unique nodes with their properties.


import pandas as pd
import networkx as nx
import csv

# Sample DataFrame for demonstration
data = {
    'src': [1, 2, 3, 4, 5, 1],
    'tgt': [2, 3, 4, 5, 6, 6],
    'trans1': ['T1', 'T2', 'T3', 'T4', 'T5', 'T1'],
    'trans2': ['U1', 'U2', 'U3', 'U4', 'U5', 'U1'],
    'sAit': ['A1', 'A2', 'A3', 'A4', 'A5', 'A1'],
    'tAit': ['B1', 'B2', 'B3', 'B4', 'B5', 'B1'],
    'colA': [10, 20, 30, 40, 50, 10],
    'colB': [15, 25, 35, 45, 55, 15],
    'colC': [100, 200, 300, 400, 500, 100],
}
sttm = pd.DataFrame(data)

# 1. Distinct record counts
distinct_counts = {
    'src,tgt,trans1,trans2,sAit,tAit': sttm[['src', 'tgt', 'trans1', 'trans2', 'sAit', 'tAit']].drop_duplicates().shape[0],
    'src,tgt,trans1,trans2': sttm[['src', 'tgt', 'trans1', 'trans2']].drop_duplicates().shape[0],
    'src,tgt,sAit,tAit': sttm[['src', 'tgt', 'sAit', 'tAit']].drop_duplicates().shape[0],
    'src,tgt': sttm[['src', 'tgt']].drop_duplicates().shape[0]
}

print("Distinct Record Counts:")
for key, value in distinct_counts.items():
    print(f"{key}: {value}")

# 2. New DataFrame with unique values
unique_df = sttm[['src', 'tgt', 'trans1', 'trans2', 'sAit', 'tAit']].drop_duplicates()
print("\nUnique DataFrame:")
print(unique_df)

# 3. Create a NetworkX directed graph
G = nx.DiGraph()
for _, row in sttm[['src', 'tgt']].drop_duplicates().iterrows():
    G.add_edge(row['src'], row['tgt'])

# 4. Depth First Search and batch writing
def find_all_paths_iterative(graph, start_node, max_depth=1000):
    stack = [(start_node, [start_node], 0)]
    while stack:
        current_node, path, depth = stack.pop()
        if depth > max_depth:
            continue
        if not list(graph.successors(current_node)):
            yield path
        else:
            for neighbor in graph.successors(current_node):
                if neighbor not in path:
                    stack.append((neighbor, path + [neighbor], depth + 1))

output_file = "paths_output.csv"
start_nodes = list(G.nodes)
batch_size = 100  # Adjust batch size as needed
batch = []

with open(output_file, mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['path', 'levels', 'start', 'end'])  # Header row
    for start_node in start_nodes:
        for path in find_all_paths_iterative(G, start_node):
            batch.append([path, len(path) - 1, path[0], path[-1]])
            if len(batch) >= batch_size:
                writer.writerows(batch)
                batch = []
    if batch:  # Write remaining rows
        writer.writerows(batch)

print(f"\nPaths written to {output_file}")

# 5. New DataFrame of unique nodes with properties
node_data = []
for node in G.nodes:
    indegree = G.in_degree(node)
    outdegree = G.out_degree(node)
    distinct_indegree = len(set([pred for pred in G.predecessors(node)]))
    distinct_outdegree = len(set([succ for succ in G.successors(node)]))
    
    # Determine position
    is_start = all(pred not in G.nodes for pred in G.predecessors(node))
    is_end = all(succ not in G.nodes for succ in G.successors(node))
    position = "Intermediate"
    if is_start:
        position = "Start"
    elif is_end:
        position = "End"
    
    node_data.append({
        'node': node,
        'indegree': indegree,
        'outdegree': outdegree,
        'distinctIndegreeCount': distinct_indegree,
        'distinctOutDegreeCount': distinct_outdegree,
        'Position': position
    })

nodes_df = pd.DataFrame(node_data)
print("\nNode Properties DataFrame:")
print(nodes_df)

# Save the nodes DataFrame to a file
nodes_output_file = "nodes_output.csv"
nodes_df.to_csv(nodes_output_file, index=False)
print(f"\nNode properties written to {nodes_output_file}")
