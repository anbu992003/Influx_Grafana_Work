import ast
import os
import sys
import json
import logging
import re
from urllib.parse import urlparse
import networkx as nx

logging.basicConfig(level=logging.INFO)

class ETLVisitor(ast.NodeVisitor):
    def __init__(self, script_path):
        self.script_path = script_path
        self.graph = nx.DiGraph()
        self.dataframes = {}  # var_name -> {'source': str, 'columns': dict col -> node_id, 'all': str}
        self.connections = {}  # var_name -> db_name
        self.import_aliases = {}  # alias -> module
        self.func_aliases = {}  # name -> full module.func

    def visit_Import(self, node):
        for alias in node.names:
            self.import_aliases[alias.asname or alias.name] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        module = node.module
        for alias in node.names:
            full = f"{module}.{alias.name}"
            name = alias.asname or alias.name
            self.func_aliases[name] = full
        self.generic_visit(node)

    def visit_Assign(self, node):
        target = node.targets[0]
        value = node.value
        if isinstance(target, ast.Name):
            var = target.id
            if isinstance(value, ast.Call):
                self.handle_call_assign(var, value, node.lineno)
        elif isinstance(target, ast.Subscript):
            if isinstance(target.value, ast.Name) and target.value.id in self.dataframes:
                df_var = target.value.id
                if isinstance(target.slice, ast.Index) and isinstance(target.slice.value, ast.Constant):
                    col = target.slice.value.value
                    sources = self.analyze_expr(node.value, df_var)
                    mem_node = f"mem://{self.script_path}#{node.lineno}:{col}"
                    self.graph.add_node(mem_node)
                    for source_node in sources:
                        self.graph.add_edge(source_node, mem_node, label=self.get_expr_label(node.value))
                    self.dataframes[df_var]['columns'][col] = mem_node
        self.generic_visit(node)

    def handle_call_assign(self, var, call, lineno):
        func = call.func
        if isinstance(func, ast.Attribute):
            module_alias = None
            method = None
            if isinstance(func.value, ast.Name):
                module_alias = func.value.id
                method = func.attr
            if module_alias in self.import_aliases:
                module = self.import_aliases[module_alias]
                if module == 'pandas':
                    if method.startswith('read_'):
                        self.handle_pandas_read(var, method, call.args, lineno)
                elif module == 'sqlalchemy':
                    if method == 'create_engine':
                        if call.args and isinstance(call.args[0], ast.Constant):
                            url = call.args[0].value
                            parsed = urlparse(url)
                            db_name = parsed.path.lstrip('/') if parsed.path else 'unknown'
                            self.connections[var] = db_name

    def handle_pandas_read(self, var, method, args, lineno):
        if not args or not isinstance(args[0], ast.Constant):
            logging.warning(f"Non-constant read arg in {self.script_path}:{lineno}, skipping")
            return
        path_or_sql = args[0].value
        source_base = None
        if method in {'read_csv', 'read_excel', 'read_parquet'}:
            filepath = path_or_sql
            source_base = f"file://{filepath}"
        elif method == 'read_sql':
            if len(args) < 2 or not isinstance(args[1], ast.Name):
                logging.warning(f"Invalid read_sql args in {self.script_path}:{lineno}, skipping")
                return
            con_var = args[1].id
            db = self.connections.get(con_var, 'unknown')
            table = self.parse_sql_table(path_or_sql)
            if not table:
                logging.warning(f"Could not parse table from SQL in {self.script_path}:{lineno}")
                return
            source_base = f"db://{db}/{table}"
        if source_base:
            all_node = f"{source_base}:*"
            self.graph.add_node(all_node)
            self.dataframes[var] = {'source': source_base, 'columns': {}, 'all': all_node}

    def visit_Call(self, node):
        func = node.func
        if isinstance(func, ast.Attribute):
            if isinstance(func.value, ast.Name):
                var = func.value.id
                if var in self.dataframes:
                    method = func.attr
                    if method in {'to_csv', 'to_excel', 'to_parquet'}:
                        if node.args and isinstance(node.args[0], ast.Constant):
                            outpath = node.args[0].value
                            out_base = f"file://{outpath}"
                            self.add_write_edges(var, out_base, node.lineno)
                    elif method == 'to_sql':
                        if len(node.args) >= 2 and isinstance(node.args[0], ast.Constant) and isinstance(node.args[1], ast.Name):
                            table = node.args[0].value
                            con_var = node.args[1].id
                            db = self.connections.get(con_var, 'unknown')
                            out_base = f"db://{db}/{table}"
                            self.add_write_edges(var, out_base, node.lineno)
        self.generic_visit(node)

    def add_write_edges(self, df_var, out_base, lineno):
        df = self.dataframes[df_var]
        if 'all' in df:
            out_node = f"{out_base}:*"
            self.graph.add_node(out_node)
            self.graph.add_edge(df['all'], out_node, label='write')
        for col, source_node in df['columns'].items():
            out_node = f"{out_base}:{col}"
            self.graph.add_node(out_node)
            self.graph.add_edge(source_node, out_node, label='write')

    def analyze_expr(self, expr, df_var):
        sources = set()
        if isinstance(expr, ast.BinOp):
            sources.update(self.analyze_expr(expr.left, df_var))
            sources.update(self.analyze_expr(expr.right, df_var))
        elif isinstance(expr, ast.Subscript):
            if isinstance(expr.value, ast.Name) and expr.value.id == df_var:
                if isinstance(expr.slice, ast.Index) and isinstance(expr.slice.value, ast.Constant):
                    col = expr.slice.value.value
                    col_node = self.dataframes[df_var]['columns'].get(col, f"{self.dataframes[df_var]['source']}:{col}")
                    self.graph.add_node(col_node)
                    sources.add(col_node)
        elif isinstance(expr, ast.Call):
            sources.add(self.dataframes[df_var]['all'])
        return sources

    def get_expr_label(self, expr):
        if isinstance(expr, ast.BinOp):
            op_class = type(expr.op).__name__.lower()
            return op_class
        elif isinstance(expr, ast.Call):
            return 'call'
        return 'transformation'

    def parse_sql_table(self, sql):
        match = re.search(r'FROM\s+([\w\.]+)', sql, re.IGNORECASE)
        return match.group(1) if match else None

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python analyzer.py /path/to/codebase")
        sys.exit(1)
    codebase_dir = sys.argv[1]
    global_graph = nx.DiGraph()
    for root, _, files in os.walk(codebase_dir):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                rel_path = os.path.relpath(filepath, codebase_dir)
                with open(filepath, 'r') as f:
                    code = f.read()
                try:
                    tree = ast.parse(code)
                    visitor = ETLVisitor(rel_path)
                    visitor.visit(tree)
                    global_graph = nx.compose(global_graph, visitor.graph)
                except Exception as e:
                    logging.error(f"Error processing {filepath}: {e}")
    if not nx.is_directed_acyclic_graph(global_graph):
        logging.warning("Graph contains cycles, but proceeding as-is (ETL assumed DAG-like)")
    nodes = list(global_graph.nodes)
    edges = [{"from": u, "to": v, "label": d.get('label', '')} for u, v, d in global_graph.edges(data=True)]
    with open('data_flow_dag.json', 'w') as f:
        json.dump({"nodes": nodes, "edges": edges}, f, indent=4)
    if not nodes and not edges:
        logging.info("No ETL patterns found; empty DAG outputted.")