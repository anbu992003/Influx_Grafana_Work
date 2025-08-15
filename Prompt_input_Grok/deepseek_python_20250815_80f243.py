import ast
import os
import sys
import json
import re
from collections import defaultdict
import logging
from typing import Dict, List, Set, Tuple, Optional, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class ETLAnalyzer(ast.NodeVisitor):
    def __init__(self):
        self.imports = set()
        self.file_reads = []
        self.file_writes = []
        self.db_reads = []
        self.db_writes = []
        self.transformations = []
        self.current_file = ""
        self.symbol_table = {}
        self.function_defs = {}
        self.dag_nodes = set()
        self.dag_edges = []
        
    def visit_Import(self, node):
        for alias in node.names:
            self.imports.add(alias.name)
        self.generic_visit(node)
        
    def visit_ImportFrom(self, node):
        module = node.module
        for alias in node.names:
            self.imports.add(f"{module}.{alias.name}")
        self.generic_visit(node)
        
    def visit_Call(self, node):
        # Check for file read operations
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in ('read_csv', 'read_excel', 'read_parquet', 'read_json'):
                if self._is_pandas_call(node.func):
                    filepath = self._get_filepath_from_args(node)
                    if filepath:
                        self.file_reads.append({
                            'filepath': filepath,
                            'method': node.func.attr,
                            'lineno': node.lineno,
                            'assign_target': self._get_assign_target(node)
                        })
            
            # Check for file write operations
            elif node.func.attr in ('to_csv', 'to_excel', 'to_parquet', 'to_json'):
                if self._is_pandas_call(node.func):
                    filepath = self._get_filepath_from_args(node)
                    if filepath:
                        self.file_writes.append({
                            'filepath': filepath,
                            'method': node.func.attr,
                            'lineno': node.lineno,
                            'source': self._get_dataframe_source(node.func.value)
                        })
        
        # Check for database operations
        elif isinstance(node.func, ast.Attribute) and node.func.attr == 'read_sql':
            if self._is_pandas_call(node.func):
                query, conn = self._get_sql_query_and_conn(node)
                if query and conn:
                    tables = self._extract_tables_from_query(query)
                    for table in tables:
                        self.db_reads.append({
                            'table': table,
                            'connection': conn,
                            'lineno': node.lineno,
                            'assign_target': self._get_assign_target(node)
                        })
        
        elif isinstance(node.func, ast.Attribute) and node.func.attr == 'to_sql':
            if self._is_pandas_call(node.func):
                table_name = self._get_table_name_from_args(node)
                conn = self._get_connection_from_args(node)
                if table_name and conn:
                    self.db_writes.append({
                        'table': table_name,
                        'connection': conn,
                        'lineno': node.lineno,
                        'source': self._get_dataframe_source(node.func.value)
                    })
        
        # Check for open() calls
        elif isinstance(node.func, ast.Name) and node.func.id == 'open':
            filepath = self._get_filepath_from_args(node)
            if filepath:
                mode = 'r'  # default
                for kw in node.keywords:
                    if kw.arg == 'mode' and isinstance(kw.value, ast.Str):
                        mode = kw.value.s
                
                if 'r' in mode:
                    self.file_reads.append({
                        'filepath': filepath,
                        'method': 'open',
                        'lineno': node.lineno,
                        'assign_target': self._get_assign_target(node)
                    })
                elif 'w' in mode or 'a' in mode:
                    self.file_writes.append({
                        'filepath': filepath,
                        'method': 'open',
                        'lineno': node.lineno,
                        'source': None  # harder to track for open()
                    })
        
        # Check for DataFrame transformations
        self._check_for_transformations(node)
        
        self.generic_visit(node)
    
    def visit_Assign(self, node):
        # Track variable assignments that might be DataFrames
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target_name = node.targets[0].id
            if isinstance(node.value, ast.Call):
                # Check if this is a DataFrame creation
                if (isinstance(node.value.func, ast.Attribute) and 
                    self._is_pandas_call(node.value.func)):
                    self.symbol_table[target_name] = {
                        'type': 'DataFrame',
                        'source': self._get_call_source(node.value)
                    }
                elif (isinstance(node.value.func, ast.Name) and 
                      node.value.func.id in self.function_defs):
                    # Handle function calls that return DataFrames
                    self.symbol_table[target_name] = {
                        'type': 'DataFrame',
                        'source': f"func:{node.value.func.id}"
                    }
            elif isinstance(node.value, ast.Subscript):
                # Handle DataFrame column assignments
                if (isinstance(node.value.value, ast.Name) and 
                    node.value.value.id in self.symbol_table and
                    self.symbol_table[node.value.value.id]['type'] == 'DataFrame'):
                    col_name = self._get_column_name(node.value)
                    if col_name:
                        self.transformations.append({
                            'type': 'column_assignment',
                            'target': f"{node.value.value.id}:{col_name}",
                            'source': self._get_expr_source(node.value),
                            'lineno': node.lineno
                        })
        
        self.generic_visit(node)
    
    def visit_FunctionDef(self, node):
        # Store function definitions for later analysis
        self.function_defs[node.name] = node
        self.generic_visit(node)
    
    def _is_pandas_call(self, func_node):
        # Check if a function call is from pandas
        if isinstance(func_node, ast.Attribute):
            if isinstance(func_node.value, ast.Name):
                return func_node.value.id == 'pd' and 'pandas' in self.imports
            elif isinstance(func_node.value, ast.Attribute):
                return self._is_pandas_call(func_node.value)
        return False
    
    def _get_filepath_from_args(self, node):
        # Extract filepath from function arguments (simple cases)
        if node.args and isinstance(node.args[0], ast.Str):
            return node.args[0].s
        elif node.args and isinstance(node.args[0], ast.Name):
            # Try to get from symbol table (very simple constant propagation)
            var_name = node.args[0].id
            if var_name in self.symbol_table and 'value' in self.symbol_table[var_name]:
                return self.symbol_table[var_name]['value']
        return None
    
    def _get_sql_query_and_conn(self, node):
        # Extract SQL query and connection from read_sql arguments
        query = None
        conn = None
        
        if len(node.args) >= 1:
            if isinstance(node.args[0], ast.Str):
                query = node.args[0].s
            elif (isinstance(node.args[0], ast.Name) and 
                 node.args[0].id in self.symbol_table and
                 'value' in self.symbol_table[node.args[0].id]):
                query = self.symbol_table[node.args[0].id]['value']
        
        if len(node.args) >= 2:
            if isinstance(node.args[1], ast.Name):
                conn = node.args[1].id
            elif isinstance(node.args[1], ast.Attribute):
                conn = self._get_attribute_chain(node.args[1])
        
        return query, conn
    
    def _extract_tables_from_query(self, query):
        # Very basic SQL table extraction
        tables = set()
        # Simple SELECT FROM pattern
        from_match = re.search(r'FROM\s+([\w\.]+)', query, re.IGNORECASE)
        if from_match:
            tables.add(from_match.group(1))
        # Handle JOIN clauses
        join_matches = re.finditer(r'JOIN\s+([\w\.]+)', query, re.IGNORECASE)
        for match in join_matches:
            tables.add(match.group(1))
        return tables
    
    def _get_table_name_from_args(self, node):
        # Extract table name from to_sql arguments
        if len(node.args) >= 1 and isinstance(node.args[0], ast.Str):
            return node.args[0].s
        elif len(node.args) >= 1 and isinstance(node.args[0], ast.Name):
            var_name = node.args[0].id
            if var_name in self.symbol_table and 'value' in self.symbol_table[var_name]:
                return self.symbol_table[var_name]['value']
        return None
    
    def _get_connection_from_args(self, node):
        # Extract connection from to_sql arguments
        if len(node.args) >= 2 and isinstance(node.args[1], ast.Name):
            return node.args[1].id
        elif len(node.args) >= 2 and isinstance(node.args[1], ast.Attribute):
            return self._get_attribute_chain(node.args[1])
        return None
    
    def _get_assign_target(self, node):
        # Get the variable name this call is assigned to (if any)
        parent = getattr(node, 'parent', None)
        if isinstance(parent, ast.Assign) and len(parent.targets) == 1:
            if isinstance(parent.targets[0], ast.Name):
                return parent.targets[0].id
        return None
    
    def _get_dataframe_source(self, node):
        # Get the source of a DataFrame (variable name or chain)
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return self._get_attribute_chain(node)
        return None
    
    def _get_attribute_chain(self, node):
        # Convert attribute chain to string (e.g., df.col -> "df.col")
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_attribute_chain(node.value)}.{node.attr}"
        return None
    
    def _get_column_name(self, node):
        # Get column name from subscript
        if isinstance(node.slice, ast.Index):  # Python 3.8 and earlier
            slice_value = node.slice.value
        else:  # Python 3.9+
            slice_value = node.slice
        
        if isinstance(slice_value, ast.Str):
            return slice_value.s
        elif isinstance(slice_value, ast.Name):
            return slice_value.id
        return None
    
    def _get_expr_source(self, node):
        # Get source columns from an expression
        if isinstance(node, ast.Name):
            return [node.id]
        elif isinstance(node, ast.Attribute):
            return [self._get_attribute_chain(node)]
        elif isinstance(node, ast.Subscript):
            base = self._get_expr_source(node.value)
            col = self._get_column_name(node)
            if col and base:
                return [f"{base[0]}:{col}"]
        elif isinstance(node, ast.BinOp):
            left = self._get_expr_source(node.left)
            right = self._get_expr_source(node.right)
            return (left or []) + (right or [])
        elif isinstance(node, ast.Call):
            args = []
            for arg in node.args:
                args.extend(self._get_expr_source(arg) or [])
            return args
        return None
    
    def _check_for_transformations(self, node):
        # Check for common DataFrame transformation patterns
        if isinstance(node, ast.Call):
            # Check for pd.merge()
            if (isinstance(node.func, ast.Attribute) and 
                node.func.attr == 'merge' and 
                self._is_pandas_call(node.func)):
                left, right = None, None
                if len(node.args) >= 2:
                    left = self._get_dataframe_source(node.args[0])
                    right = self._get_dataframe_source(node.args[1])
                
                if left and right:
                    target = self._get_assign_target(node)
                    if target:
                        self.transformations.append({
                            'type': 'merge',
                            'target': target,
                            'sources': [left, right],
                            'lineno': node.lineno
                        })
            
            # Check for df.groupby()
            elif (isinstance(node.func, ast.Attribute) and 
                  node.func.attr == 'groupby' and 
                  isinstance(node.func.value, (ast.Name, ast.Attribute))):
                df_source = self._get_dataframe_source(node.func.value)
                if df_source:
                    target = self._get_assign_target(node)
                    if target:
                        self.transformations.append({
                            'type': 'groupby',
                            'target': target,
                            'source': df_source,
                            'lineno': node.lineno
                        })
        
        # Check for column operations
        elif (isinstance(node, ast.Assign) and 
              isinstance(node.targets[0], ast.Subscript) and 
              isinstance(node.targets[0].value, ast.Name) and 
              node.targets[0].value.id in self.symbol_table and
              self.symbol_table[node.targets[0].value.id]['type'] == 'DataFrame'):
            col_name = self._get_column_name(node.targets[0])
            if col_name:
                sources = self._get_expr_source(node.value)
                if sources:
                    self.transformations.append({
                        'type': 'column_transform',
                        'target': f"{node.targets[0].value.id}:{col_name}",
                        'sources': sources,
                        'lineno': node.lineno
                    })
    
    def build_dag(self):
        # Build DAG from collected information
        
        # Add file read nodes
        for read in self.file_reads:
            node_id = f"file://{read['filepath']}:*"
            self.dag_nodes.add(node_id)
            if read['assign_target']:
                self.dag_edges.append({
                    'from': node_id,
                    'to': read['assign_target'],
                    'label': 'read'
                })
        
        # Add database read nodes
        for read in self.db_reads:
            node_id = f"db://{read['connection']}/{read['table']}:*"
            self.dag_nodes.add(node_id)
            if read['assign_target']:
                self.dag_edges.append({
                    'from': node_id,
                    'to': read['assign_target'],
                    'label': 'read'
                })
        
        # Add transformation edges
        for trans in self.transformations:
            if trans['type'] in ('column_transform', 'column_assignment'):
                for source in trans['sources']:
                    self.dag_edges.append({
                        'from': source,
                        'to': trans['target'],
                        'label': trans['type']
                    })
            elif trans['type'] == 'merge':
                for source in trans['sources']:
                    self.dag_edges.append({
                        'from': source,
                        'to': trans['target'],
                        'label': 'merge'
                    })
            elif trans['type'] == 'groupby':
                self.dag_edges.append({
                    'from': trans['source'],
                    'to': trans['target'],
                    'label': 'groupby'
                })
        
        # Add file write nodes
        for write in self.file_writes:
            node_id = f"file://{write['filepath']}:*"
            self.dag_nodes.add(node_id)
            if write['source']:
                self.dag_edges.append({
                    'from': write['source'],
                    'to': node_id,
                    'label': 'write'
                })
        
        # Add database write nodes
        for write in self.db_writes:
            node_id = f"db://{write['connection']}/{write['table']}:*"
            self.dag_nodes.add(node_id)
            if write['source']:
                self.dag_edges.append({
                    'from': write['source'],
                    'to': node_id,
                    'label': 'write'
                })
        
        return {
            'nodes': list(self.dag_nodes),
            'edges': self.dag_edges
        }

def parse_python_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        tree = ast.parse(content)
        
        # Add parent references for easier traversal
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                child.parent = node
        
        analyzer = ETLAnalyzer()
        analyzer.current_file = filepath
        analyzer.visit(tree)
        return analyzer.build_dag()
    except Exception as e:
        logger.warning(f"Failed to parse {filepath}: {str(e)}")
        return {'nodes': [], 'edges': []}

def merge_dags(dags):
    merged_nodes = set()
    merged_edges = []
    
    for dag in dags:
        merged_nodes.update(dag['nodes'])
        merged_edges.extend(dag['edges'])
    
    return {
        'nodes': list(merged_nodes),
        'edges': merged_edges
    }

def analyze_codebase(directory):
    dags = []
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                logger.info(f"Analyzing {filepath}")
                dag = parse_python_file(filepath)
                if dag['nodes'] or dag['edges']:
                    dags.append(dag)
    
    if not dags:
        logger.warning("No ETL patterns found in the codebase")
        return {'nodes': [], 'edges': []}
    
    return merge_dags(dags)

def save_dag_to_json(dag, output_file='data_flow_dag.json'):
    with open(output_file, 'w') as f:
        json.dump(dag, f, indent=2)
    logger.info(f"DAG saved to {output_file}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python analyzer.py <path_to_codebase>")
        sys.exit(1)
    
    codebase_path = sys.argv[1]
    if not os.path.isdir(codebase_path):
        print(f"Error: {codebase_path} is not a valid directory")
        sys.exit(1)
    
    dag = analyze_codebase(codebase_path)
    save_dag_to_json(dag)

if __name__ == '__main__':
    main()