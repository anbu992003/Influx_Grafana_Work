from collections import deque

class Digraph:
    def __init__(self):
        self.adj_list = {}

    def add_edge(self, u, v):
        if u not in self.adj_list:
            self.adj_list[u] = []
        self.adj_list[u].append(v)

    def bfs(self, start, end):
        queue = deque([(start, 0)])  # (node, level)
        visited = {start}
        level_info = {}

        while queue:
            node, level = queue.popleft()
            
            # Track level information
            if level not in level_info:
                level_info[level] = []
            level_info[level].append(node)
            
            # If we reach the end node, stop search
            if node == end:
                break

            if node in self.adj_list:
                for neighbor in self.adj_list[node]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, level + 1))

        # Print level information with out-degree
        for lvl, nodes in level_info.items():
            out_degree_info = [f"{node}\t{len(self.adj_list.get(node, []))}" for node in nodes]
            print(f"Level {lvl}:\t" + "\t".join(out_degree_info))

# Example Usage
graph = Digraph()
edges = [
    ('A', 'B'), ('A', 'C'), ('B', 'D'), ('B', 'E'),
    ('C', 'F'), ('E', 'G'), ('F', 'H'), ('G', 'I')
]

for u, v in edges:
    graph.add_edge(u, v)

start_node = 'A'
end_node = 'H'
graph.bfs(start_node, end_node)