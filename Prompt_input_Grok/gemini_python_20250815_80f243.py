import os
import ast
import json
import argparse
import networkx as nx
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ETLAnalyzer(ast.NodeVisitor):
    """
    An AST visitor to analyze a single Python file for ETL operations.
    It identifies data sources, transformations, and sinks, and builds a per-file DAG.
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.graph = nx.DiGraph()
        self.variables = {}  # Tracks variable assignments, especially for DataFrames and columns
        self.imports = {}  # Tracks imported modules to identify library calls
        self.output_sinks = []
        self.input_sources = []
        self.current_func_name = None # To handle function scopes

    def _add_node(self, node_str):
        if node_str not in self.graph:
            self.graph.add_node(node_str)

    def _add_edge(self, source, target, label='transform'):
        self._add_node(source)
        self._add_node(target)
        self.graph.add_edge(source, target, label=label)

    def visit_Import(self, node):
        for alias in node.names:
            if alias.asname:
                self.imports[alias.asname] = alias.name
            else:
                self.imports[alias.name] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        module_name = node.module
        for alias in node.names:
            if alias.asname:
                self.imports[alias.asname] = f"{module_name}.{alias.name}"
            else:
                self.imports[alias.name] = f"{module_name}.{alias.name}"
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        # Enter function scope, save current state
        original_func_name = self.current_func_name
        self.current_func_name = node.name
        original_variables = self.variables.copy()
        self.variables = {}  # New scope
        self.generic_visit(node)
        # Exit function scope, restore state
        self.current_func_name = original_func_name
        self.variables = original_variables

    def visit_Assign(self, node):
        # Handle assignments, focusing on DataFrame and file-related variables
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target_name = node.targets[0].id
            # Simple constant propagation
            if isinstance(node.value, ast.Constant):
                self.variables[target_name] = node.value.value
            elif isinstance(node.value, ast.Call):
                self.handle_call(node.value, target_name)
            elif isinstance(node.value, ast.BinOp):
                self.handle_binop(node.value, target_name)
            elif isinstance(node.value, ast.Attribute) and isinstance(node.value.ctx, ast.Load):
                if isinstance(node.value.value, ast.Name):
                    var_name = node.value.value.id
                    if var_name in self.variables:
                        self.variables[target_name] = self.variables[var_name]
            elif isinstance(node.value, (ast.List, ast.Dict)):
                 self.variables[target_name] = node.value # Store the AST node for later inspection

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Subscript):
            self.handle_subscript_assign(node.target, node.op, node.value)

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call):
            self.handle_call(node.value)
        self.generic_visit(node)

    def visit_Subscript(self, node):
        self.generic_visit(node)

    def handle_call(self, call_node, assign_target=None):
        if isinstance(call_node.func, ast.Attribute):
            func_name = call_node.func.attr
            if isinstance(call_node.func.value, ast.Name):
                module_or_var = call_node.func.value.id
                module = self.imports.get(module_or_var)
                
                # Input operations (Source)
                if module in ('pandas', 'sqlalchemy', 'psycopg2', 'sqlite3', 'open'):
                    if func_name in ('read_csv', 'read_excel', 'read_parquet'):
                        self.handle_file_read(call_node, assign_target, func_name)
                    elif func_name in ('read_sql', 'read_sql_table', 'read_sql_query'):
                        self.handle_db_read(call_node, assign_target, func_name)
                    elif func_name == 'connect':
                        # Track connection object
                        if assign_target:
                            self.variables[assign_target] = {'type': 'db_connection'}

                # Output operations (Sink)
                elif func_name in ('to_csv', 'to_excel', 'to_parquet'):
                    self.handle_file_write(call_node, module_or_var, func_name)
                elif func_name == 'to_sql':
                    self.handle_db_write(call_node, module_or_var, func_name)

                # Transformation logic
                elif func_name in ('merge', 'join', 'concat'):
                    self.handle_merge_join(call_node, assign_target, func_name, module_or_var)
                elif func_name in ('groupby'):
                    # Store the groupby object
                    if assign_target and module_or_var in self.variables:
                         self.variables[assign_target] = {'type': 'groupby', 'source': module_or_var, 'keys': self.get_call_args(call_node)}
                elif func_name in ('apply', 'agg', 'transform'):
                    self.handle_apply(call_node, assign_target, func_name, module_or_var)
                elif func_name == 'query':
                    self.handle_query(call_node, assign_target, func_name, module_or_var)
                
            elif isinstance(call_node.func.value, ast.Subscript) and call_node.func.value.value.id in self.variables:
                # Handle df.groupby(...).agg(...) or other chained calls
                source_obj = call_node.func.value.value.id
                if self.variables.get(source_obj, {}).get('type') == 'groupby' and func_name == 'agg':
                    self.handle_groupby_agg(call_node, assign_target)
            elif isinstance(call_node.func.value, ast.Call):
                # Handle nested calls, e.g., func1(func2())
                self.handle_call(call_node.func.value, 'temp_var')
                self.handle_call(call_node, assign_target)

    def handle_subscript_assign(self, target, op, value):
        if isinstance(target.value, ast.Name) and isinstance(target.slice, ast.Constant):
            var_name = target.value.id
            if var_name in self.variables and isinstance(self.variables[var_name], dict) and 'columns' in self.variables[var_name]:
                new_col_name = target.slice.value
                source_cols = self.get_source_columns_from_expression(value)
                new_node = f'file://{self.file_path}:<intermediate>:{new_col_name}'
                self.variables[var_name]['columns'].add(new_col_name)

                for source_col in source_cols:
                    source_node = f'file://{self.file_path}:<intermediate>:{source_col}'
                    self._add_edge(source_node, new_node, label='arithmetic')

    def get_source_columns_from_expression(self, node):
        cols = set()
        if isinstance(node, ast.BinOp):
            cols.update(self.get_source_columns_from_expression(node.left))
            cols.update(self.get_source_columns_from_expression(node.right))
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            if isinstance(node.value, ast.Name) and node.value.id in self.variables:
                cols.add(node.slice.value)
        return cols

    def get_call_args(self, call_node):
        args_dict = {}
        for arg in call_node.args:
            if isinstance(arg, ast.Constant):
                args_dict[len(args_dict)] = arg.value
        for kwarg in call_node.keywords:
            if isinstance(kwarg.value, ast.Constant):
                args_dict[kwarg.arg] = kwarg.value.value
        return args_dict

    def handle_file_read(self, call_node, assign_target, func_name):
        args = self.get_call_args(call_node)
        file_path_or_var = args.get(0) or args.get('filepath_or_buffer')
        if file_path_or_var:
            file_path = self.variables.get(file_path_or_var, file_path_or_var)
            source_node = f'file://{file_path}:*'
            self.input_sources.append(source_node)
            if assign_target:
                self.variables[assign_target] = {'type': 'dataframe', 'source': source_node, 'columns': set()}

    def handle_db_read(self, call_node, assign_target, func_name):
        args = self.get_call_args(call_node)
        sql_query_or_var = args.get(0)
        con_var = args.get(1)
        sql_query = self.variables.get(sql_query_or_var, sql_query_or_var)
        table_name = None
        if isinstance(sql_query, str):
            match = re.search(r'FROM\s+([a-zA-Z0-9_]+)', sql_query, re.IGNORECASE)
            if match:
                table_name = match.group(1)

        db_name = None # Simple parser assumes no db name in query, maybe in connection string
        if con_var and con_var in self.variables and self.variables[con_var]['type'] == 'db_connection':
            db_name = 'unknown_db' # Placeholder
        
        if table_name:
            source_node = f'db://{db_name or "unknown_db"}/{table_name}:*'
            self.input_sources.append(source_node)
            if assign_target:
                self.variables[assign_target] = {'type': 'dataframe', 'source': source_node, 'columns': set()}

    def handle_file_write(self, call_node, var_name, func_name):
        args = self.get_call_args(call_node)
        file_path_or_var = args.get(0) or args.get('path_or_buf')
        if file_path_or_var:
            file_path = self.variables.get(file_path_or_var, file_path_or_var)
            source_obj = self.variables.get(var_name)
            if source_obj and isinstance(source_obj, dict) and 'source' in source_obj:
                source_node = source_obj['source']
                target_node = f'file://{file_path}:*'
                self.output_sinks.append(target_node)
                self._add_edge(source_node, target_node, label=func_name)

    def handle_db_write(self, call_node, var_name, func_name):
        args = self.get_call_args(call_node)
        table_name_or_var = args.get(0)
        con_var = args.get(1)
        table_name = self.variables.get(table_name_or_var, table_name_or_var)
        db_name = 'unknown_db'
        
        if table_name:
            source_obj = self.variables.get(var_name)
            if source_obj and isinstance(source_obj, dict) and 'source' in source_obj:
                source_node = source_obj['source']
                target_node = f'db://{db_name}/{table_name}:*'
                self.output_sinks.append(target_node)
                self._add_edge(source_node, target_node, label=func_name)

    def handle_merge_join(self, call_node, assign_target, func_name, module_or_var):
        args = self.get_call_args(call_node)
        left_df_var = args.get(0)
        right_df_var = args.get(1)

        left_df_source = self.variables.get(left_df_var, {}).get('source')
        right_df_source = self.variables.get(right_df_var, {}).get('source')
        
        if left_df_source and right_df_source and assign_target:
            self.variables[assign_target] = {'type': 'dataframe', 'source': f'{left_df_source}_{right_df_source}', 'columns': set()}
            merged_source = self.variables[assign_target]['source']
            self._add_edge(left_df_source, merged_source, label=f'merge_{func_name}')
            self._add_edge(right_df_source, merged_source, label=f'merge_{func_name}')
            
    def handle_apply(self, call_node, assign_target, func_name, var_name):
        # A simple model: an apply on a DataFrame means a transformation on all columns.
        # This is a simplification but captures the essence.
        source_obj = self.variables.get(var_name, {})
        if source_obj and isinstance(source_obj, dict) and 'source' in source_obj and assign_target:
            source_node = source_obj['source']
            target_node = f'file://{self.file_path}:<intermediate>:*'
            self._add_edge(source_node, target_node, label=func_name)
            self.variables[assign_target] = {'type': 'dataframe', 'source': target_node, 'columns': set()}

    def handle_groupby_agg(self, call_node, assign_target):
        # Simplification: a groupby-agg means all original columns are aggregated into new ones.
        # The new columns are derived from the old ones.
        source_obj = self.variables.get(call_node.func.value.value.id, {})
        if source_obj and isinstance(source_obj, dict) and 'source' in source_obj and assign_target:
            source_node = source_obj['source']
            target_node = f'file://{self.file_path}:<intermediate>:*'
            self._add_edge(source_node, target_node, label='groupby_agg')
            self.variables[assign_target] = {'type': 'dataframe', 'source': target_node, 'columns': set()}

    def handle_query(self, call_node, assign_target, func_name, var_name):
        source_obj = self.variables.get(var_name, {})
        if source_obj and isinstance(source_obj, dict) and 'source' in source_obj and assign_target:
            source_node = source_obj['source']
            target_node = f'file://{self.file_path}:<intermediate>:*'
            self._add_edge(source_node, target_node, label=func_name)
            self.variables[assign_target] = {'type': 'dataframe', 'source': target_node, 'columns': set()}

    def get_dag(self):
        return self.graph, self.input_sources, self.output_sinks

def build_global_dag(file_dags):
    """
    Combines individual file DAGs into a single global DAG and infers connections.
    """
    global_dag = nx.DiGraph()
    input_sinks = {} # Maps a source node to its file
    output_sources = {} # Maps a sink node to its file
    
    for file_path, (dag, inputs, outputs) in file_dags.items():
        # Add all nodes and edges from the per-file DAGs
        for node in dag.nodes:
            global_dag.add_node(node)
        for u, v, data in dag.edges(data=True):
            global_dag.add_edge(u, v, label=data['label'])

        # Store input/output nodes for inter-file connection inference
        for input_node in inputs:
            if input_node not in input_sinks:
                input_sinks[input_node] = []
            input_sinks[input_node].append(file_path)
        
        for output_node in outputs:
            if output_node not in output_sources:
                output_sources[output_node] = []
            output_sources[output_node].append(file_path)

    # Infer connections
    for output_node, output_files in output_sources.items():
        for input_node, input_files in input_sinks.items():
            # Check if an output from one script matches an input to another
            if output_node.rsplit(':', 1)[0] == input_node.rsplit(':', 1)[0]:
                for output_file in output_files:
                    for input_file in input_files:
                        if output_file != input_file: # Don't link a file to itself
                            # Find all edges originating from the input_node's file and add edges from the output_node's file
                            for u, v, data in global_dag.edges(data=True):
                                if u == input_node:
                                    global_dag.add_edge(output_node, v, label='inferred_flow')

    # Convert to JSON format
    nodes = list(global_dag.nodes())
    edges = [{"from": u, "to": v, "label": data.get('label', 'transform')} for u, v, data in global_dag.edges(data=True)]

    return {"nodes": nodes, "edges": edges}

def main():
    parser = argparse.ArgumentParser(description="Analyze ETL codebase and build a data flow DAG.")
    parser.add_argument("codebase_path", help="Path to the ETL project directory.")
    args = parser.parse_args()

    codebase_path = args.codebase_path
    if not os.path.isdir(codebase_path):
        logging.error(f"Provided path is not a valid directory: {codebase_path}")
        return

    logging.info(f"Starting analysis of codebase at: {codebase_path}")
    file_dags = {}

    for root, _, files in os.walk(codebase_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                logging.info(f"Processing file: {file_path}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        source_code = f.read()
                    
                    tree = ast.parse(source_code, filename=file_path)
                    analyzer = ETLAnalyzer(file_path)
                    analyzer.visit(tree)
                    
                    file_dag, file_inputs, file_outputs = analyzer.get_dag()
                    if file_dag.nodes:
                        file_dags[file_path] = (file_dag, file_inputs, file_outputs)
                        logging.info(f"Found {len(file_dag.nodes)} nodes and {len(file_dag.edges)} edges in {file_path}")

                except SyntaxError as e:
                    logging.warning(f"Could not parse file {file_path} due to syntax error: {e}")
                except Exception as e:
                    logging.error(f"An unexpected error occurred while processing {file_path}: {e}")

    if not file_dags:
        logging.warning("No ETL patterns were found in the provided codebase.")
        final_dag = {"nodes": [], "edges": []}
    else:
        logging.info("Building global DAG from individual file DAGs.")
        final_dag = build_global_dag(file_dags)
        logging.info(f"Global DAG built with {len(final_dag['nodes'])} nodes and {len(final_dag['edges'])} edges.")

    output_file_path = "data_flow_dag.json"
    with open(output_file_path, 'w') as f:
        json.dump(final_dag, f, indent=4)
    
    logging.info(f"Final DAG written to {output_file_path}")
    logging.info("Analysis complete.")

if __name__ == "__main__":
    main()