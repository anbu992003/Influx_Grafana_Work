import pandas as pd
import csv
from collections import defaultdict

class Digraph:
    def __init__(self):
        self.adj_list = defaultdict(list)
        self.in_degree = defaultdict(int)

    def add_edge(self, src, tgt):
        self.adj_list[src].append(tgt)
        self.in_degree[tgt] += 1  # Track in-degree of target node
        if src not in self.in_degree:
            self.in_degree[src] = 0  # Ensure source is in the in-degree dictionary

    def get_starting_nodes(self):
        return [node for node in self.in_degree if self.in_degree[node] == 0]
        
    def iterative_dfs(graph, start):
        stack = [(start, f"{start}", 0)]  # (node, path, depth)
        results = []

        while stack:
            node, path, depth = stack.pop()
            results.append((path, depth))

            for neighbor in graph.adj_list.get(node, []):
                stack.append((neighbor, f"{path} -> {neighbor}", depth + 1))

        return results

    def dfs(self, node, path, depth, results):
        results.append((path, depth))
        for neighbor in self.adj_list[node]:
            self.dfs(neighbor, f"{path} -> {neighbor}", depth + 1, results)

    def get_lineage_paths(self):
        starting_nodes = self.get_starting_nodes()
        lineage_data = []

        for node in starting_nodes:
            self.dfs(node, node, 0, lineage_data)

        return lineage_data

# Read CSV and construct the graph
graph = Digraph()
df = pd.read_csv("input.csv")

for _, row in df.iterrows():
    graph.add_edge(row["src"], row["tgt"])

# Get lineage paths
lineage_paths = graph.get_lineage_paths()

# Write to CSV
output_filename = "lineage_paths.csv"
with open(output_filename, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Lineage Path", "Depth"])
    writer.writerows(lineage_paths)

print(f"Lineage paths successfully written to {output_filename}.")