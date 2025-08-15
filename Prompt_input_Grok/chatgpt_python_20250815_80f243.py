#!/usr/bin/env python3
"""
analyzer.py — Static ETL data-flow analyzer using Python AST

Usage:
    python analyzer.py /path/to/codebase

Outputs:
    data_flow_dag.json — global DAG with "nodes" and "edges"
"""

import ast
import json
import logging
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

# Try to use networkx if available; otherwise fall back to a simple graph holder.
try:
    import networkx as nx  # type: ignore
except Exception:  # pragma: no cover
    nx = None


# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
LOGGER = logging.getLogger("etl_ast_analyzer")


# ---------- Helpers for node formatting ----------
def file_node(path: str, field: str = "*") -> str:
    norm = os.path.abspath(path)
    return f"file://{norm}:{field}"


def db_node(db: str, table: str, col: str = "*") -> str:
    return f"db://{db}/{table}:{col}"


def edge_dict(src: str, dst: str, label: str) -> Dict[str, str]:
    return {"from": src, "to": dst, "label": label}


# ---------- SQL parsing helpers ----------
FROM_TABLE_REGEX = re.compile(r"\bfrom\s+([a-zA-Z0-9_.\"`]+)", flags=re.IGNORECASE)
INSERT_INTO_REGEX = re.compile(r"\binsert\s+into\s+([a-zA-Z0-9_.\"`]+)", flags=re.IGNORECASE)
UPDATE_TABLE_REGEX = re.compile(r"\bupdate\s+([a-zA-Z0-9_.\"`]+)", flags=re.IGNORECASE)

DBNAME_FROM_URL = re.compile(r"^[a-zA-Z+]+://[^/]+/([^?]+)")


def extract_table_from_sql(sql: str) -> Optional[str]:
    """Very basic SQL table extractor for SELECT ... FROM <table>."""
    if not sql or not isinstance(sql, str):
        return None
    m = FROM_TABLE_REGEX.search(sql)
    if m:
        tbl = m.group(1).strip().strip('`"')
        return tbl
    return None


def extract_write_table_from_sql(sql: str) -> Optional[str]:
    """Extract table for INSERT INTO or UPDATE."""
    if not sql or not isinstance(sql, str):
        return None
    m1 = INSERT_INTO_REGEX.search(sql)
    if m1:
        return m1.group(1).strip().strip('`"')
    m2 = UPDATE_TABLE_REGEX.search(sql)
    if m2:
        return m2.group(1).strip().strip('`"')
    return None


def extract_dbname_from_url(url: str) -> Optional[str]:
    """Return db name from SQLAlchemy URL-like string."""
    m = DBNAME_FROM_URL.match(url)
    if m:
        return m.group(1)
    return None


# ---------- Constant / simple value resolver ----------
class ConstResolver:
    """
    Tracks simple variable -> constant string values.
    Supports string constants, f-strings with constants only, os.path.join of constants,
    and string concatenation.
    """
    def __init__(self):
        self.consts: Dict[str, Any] = {}  # variable -> constant (str, int, etc.)

    def set_const(self, name: str, value: Any) -> None:
        self.consts[name] = value

    def get_const(self, node: ast.AST) -> Optional[Any]:
        """Try to resolve node to a constant value (string/int/float)."""
        try:
            if isinstance(node, ast.Constant):
                return node.value
            if isinstance(node, ast.JoinedStr):  # f-string
                # Only support f-strings where values are constants we already know.
                parts = []
                for v in node.values:
                    if isinstance(v, ast.Str):  # Python <3.8 compat (ast.Constant in 3.8+)
                        parts.append(v.s)
                    elif isinstance(v, ast.Constant):
                        parts.append(str(v.value))
                    elif isinstance(v, ast.FormattedValue):
                        inner = self.get_const(v.value)
                        if inner is None:
                            return None
                        parts.append(str(inner))
                    else:
                        return None
                return "".join(parts)
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
                left = self.get_const(node.left)
                right = self.get_const(node.right)
                if isinstance(left, str) and isinstance(right, str):
                    return left + right
                if (isinstance(left, (int, float))) and (isinstance(right, (int, float))):
                    return left + right
                return None
            if isinstance(node, ast.Call):
                # Support os.path.join with all constant args
                if isinstance(node.func, ast.Attribute) and node.func.attr == "join":
                    if isinstance(node.func.value, ast.Attribute):
                        if getattr(node.func.value, "attr", "") == "path":
                            if isinstance(getattr(node.func.value, "value", None), ast.Name) and getattr(node.func.value.value, "id", "") == "os":
                                parts = []
                                for a in node.args:
                                    v = self.get_const(a)
                                    if v is None:
                                        return None
                                    parts.append(str(v))
                                return os.path.join(*parts)
                # str.format with constants
                if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Constant) and node.func.attr == "format":
                    fmt = node.func.value.value
                    if isinstance(fmt, str):
                        params = []
                        for a in node.args:
                            v = self.get_const(a)
                            if v is None:
                                return None
                            params.append(v)
                        try:
                            return fmt.format(*params)
                        except Exception:
                            return None
                # str() of constant
                if isinstance(node.func, ast.Name) and node.func.id == "str" and node.args:
                    v = self.get_const(node.args[0])
                    if v is not None:
                        return str(v)
            if isinstance(node, ast.Name):
                return self.consts.get(node.id)
        except Exception:
            return None
        return None


# ---------- Main AST ETL Analyzer ----------
class ETLAnalyzer(ast.NodeVisitor):
    def __init__(self, filename: str):
        self.filename = filename
        # Import tracking
        self.pandas_aliases: Set[str] = set()
        self.os_aliases: Set[str] = set()
        self.sqlalchemy_aliases: Set[str] = set()
        self.psycopg2_used = False
        self.sqlite3_used = False

        # Resolvers & symbol tables
        self.consts = ConstResolver()
        self.df_vars: Set[str] = set()
        self.df_cols: Dict[str, Set[str]] = defaultdict(set)  # df_var -> columns seen/created
        self.engine_vars: Dict[str, str] = {}  # var -> sqlalchemy URL string (if const)
        self.conn_vars: Dict[str, str] = {}    # var -> db path/name (sqlite or url)
        self.table_vars: Dict[str, str] = {}   # var -> table name (const)

        # Nodes and edges for this script
        self.nodes: Set[str] = set()
        self.edges: List[Dict[str, str]] = []

        # Per-script IO tracking for later cross-file connection
        self.inputs_by_key: Dict[Tuple[str, str], Set[str]] = defaultdict(set)   # (type, key)-> set(node)
        self.outputs_by_key: Dict[Tuple[str, str], Set[str]] = defaultdict(set)  # (type, key)-> set(node)

        # Lightweight stack of function defs (we do visit inside functions)
        self.current_function_stack: List[str] = []

    # ---- Utilities ----

    def add_edge(self, src: str, dst: str, label: str) -> None:
        self.nodes.add(src)
        self.nodes.add(dst)
        self.edges.append(edge_dict(src, dst, label))

    def record_file_read(self, path: str) -> None:
        n = file_node(path, "*")
        self.nodes.add(n)
        self.inputs_by_key[("file", os.path.abspath(path))].add(n)

    def record_file_write(self, path: str) -> None:
        n = file_node(path, "*")
        self.nodes.add(n)
        self.outputs_by_key[("file", os.path.abspath(path))].add(n)

    def record_db_read(self, dbname: str, table: str) -> None:
        n = db_node(dbname, table, "*")
        self.nodes.add(n)
        self.inputs_by_key[("db", f"{dbname}/{table}")].add(n)

    def record_db_write(self, dbname: str, table: str) -> None:
        n = db_node(dbname, table, "*")
        self.nodes.add(n)
        self.outputs_by_key[("db", f"{dbname}/{table}")].add(n)

    def resolve_str(self, node: ast.AST) -> Optional[str]:
        v = self.consts.get_const(node)
        return v if isinstance(v, str) else None

    def guess_binop_label(self, op: ast.AST) -> str:
        return {
            ast.Add: "addition",
            ast.Sub: "subtraction",
            ast.Mult: "multiplication",
            ast.Div: "division",
            ast.Mod: "modulo",
            ast.Pow: "power",
        }.get(type(op), "binop")

    def infer_dbname_from_con(self, con_node: Optional[ast.AST]) -> Optional[str]:
        if con_node is None:
            return None
        # con may be Name (engine var) or Constant(url) or Call(create_engine(...))
        if isinstance(con_node, ast.Name):
            nm = con_node.id
            # engine var or sqlite conn var
            if nm in self.engine_vars:
                url = self.engine_vars[nm]
                dbn = extract_dbname_from_url(url) or url
                return dbn
            if nm in self.conn_vars:
                return self.conn_vars[nm]
            # or maybe const resolver knows it
            v = self.consts.consts.get(nm)
            if isinstance(v, str):
                return extract_dbname_from_url(v) or v
        elif isinstance(con_node, ast.Constant) and isinstance(con_node.value, str):
            return extract_dbname_from_url(con_node.value) or con_node.value
        # create_engine("...") directly
        if isinstance(con_node, ast.Call) and isinstance(con_node.func, ast.Name) and con_node.func.id == "create_engine" and con_node.args:
            url = self.resolve_str(con_node.args[0])
            if url:
                return extract_dbname_from_url(url) or url
        return None

    # ---- Import handling ----
    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "pandas":
                self.pandas_aliases.add(alias.asname or "pandas")
            if alias.name == "os":
                self.os_aliases.add(alias.asname or "os")
            if alias.name.startswith("sqlalchemy"):
                self.sqlalchemy_aliases.add(alias.asname or alias.name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "pandas":
            self.pandas_aliases.add("pandas")  # generic
        if node.module == "sqlalchemy":
            self.sqlalchemy_aliases.add("sqlalchemy")

    # ---- Assignments (track constants, df vars, tables) ----
    def visit_Assign(self, node: ast.Assign) -> None:
        # Track constants
        val_const = self.consts.get_const(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                if val_const is not None:
                    self.consts.set_const(target.id, val_const)

            # Track DataFrame creations from reads
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
                call = node.value
                func = call.func
                # pandas.read_* returns df
                func_name = self._qualified_func_name(func)
                if func_name and (func_name.endswith("read_csv") or func_name.endswith("read_excel")
                                  or func_name.endswith("read_parquet") or func_name.endswith("read_json")
                                  or func_name.endswith("read_feather") or func_name.endswith("read_table")):
                    self.df_vars.add(target.id)
                    # try resolve path from first arg or 'filepath_or_buffer' kw
                    path = None
                    if call.args:
                        path = self.resolve_str(call.args[0])
                    if not path:
                        for kw in call.keywords or []:
                            if kw.arg in ("filepath_or_buffer", "path", "path_or_buf"):
                                path = self.resolve_str(kw.value)
                                break
                    if path:
                        self.record_file_read(path)
                        # DataFrame node as an intermediate
                        self.add_edge(file_node(path, "*"), f"{self.filename}:{target.id}", "read_file")
                        self.nodes.add(f"{self.filename}:{target.id}")
                    return

                # pandas.read_sql
                if func_name and func_name.endswith("read_sql"):
                    self.df_vars.add(target.id)
                    sql_txt = None
                    con_node = None
                    if call.args:
                        # read_sql(sql, con, ...)
                        if len(call.args) >= 1:
                            sql_txt = self.resolve_str(call.args[0])
                        if len(call.args) >= 2:
                            con_node = call.args[1]
                    for kw in call.keywords or []:
                        if kw.arg == "sql" and sql_txt is None:
                            sql_txt = self.resolve_str(kw.value)
                        if kw.arg == "con" and con_node is None:
                            con_node = kw.value
                    table = extract_table_from_sql(sql_txt or "")
                    dbn = self.infer_dbname_from_con(con_node) or "unknown_db"
                    if table:
                        self.record_db_read(dbn, table)
                        self.add_edge(db_node(dbn, table, "*"), f"{self.filename}:{target.id}", "read_db")
                        self.nodes.add(f"{self.filename}:{target.id}")
                    return

                # sqlalchemy create_engine
                if isinstance(func, ast.Name) and func.id == "create_engine" and call.args:
                    url = self.resolve_str(call.args[0])
                    if url:
                        self.engine_vars[target.id] = url
                        self.consts.set_const(target.id, url)
                        return

                # sqlite3.connect('file.db')
                if isinstance(func, ast.Attribute) and func.attr == "connect":
                    base = func.value
                    base_name = getattr(base, "id", None)
                    if base_name == "sqlite3" and call.args:
                        dbp = self.resolve_str(call.args[0])
                        if dbp:
                            self.conn_vars[target.id] = dbp
                            self.consts.set_const(target.id, dbp)
                            return

                # df = pd.merge(left, right, ...)
                if func_name and func_name.endswith("merge") and call.args:
                    self.df_vars.add(target.id)
                    # Try capture edges from left/right frames
                    left_src = self._name_or_df(call, "left", pos=0)
                    right_src = self._name_or_df(call, "right", pos=1)
                    if left_src:
                        self.add_edge(f"{self.filename}:{left_src}", f"{self.filename}:{target.id}", "merge")
                    if right_src:
                        self.add_edge(f"{self.filename}:{right_src}", f"{self.filename}:{target.id}", "merge")
                    self.nodes.add(f"{self.filename}:{target.id}")
                    return

        # Track "table name" constants for to_sql
        # e.g., table_name = "customers"
        for target in node.targets:
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                self.table_vars[target.id] = node.value.value

        # Assignment to df['new_col'] = expr
        if isinstance(node.targets[0], ast.Subscript):
            tgt = node.targets[0]
            df_name, col = self._subscript_to_df_col(tgt)
            if df_name and col:
                self.df_vars.add(df_name)
                self.df_cols[df_name].add(col)
                # Find source columns in value expr
                src_cols = self._collect_df_columns_in_expr(node.value)
                if not src_cols:
                    # at least link df * to new col
                    self.add_edge(f"{self.filename}:{df_name}", f"{self.filename}:{df_name}[{col}]", "assignment")
                else:
                    for (sdf, scol) in src_cols:
                        self.add_edge(f"{self.filename}:{sdf}[{scol}]", f"{self.filename}:{df_name}[{col}]",
                                      self._label_for_expr(node.value))
                # Register nodes
                self.nodes.add(f"{self.filename}:{df_name}")
                self.nodes.add(f"{self.filename}:{df_name}[{col}]")

        self.generic_visit(node)

    # ---- Function definitions (we still walk into them) ----
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.current_function_stack.append(node.name)
        self.generic_visit(node)
        self.current_function_stack.pop()

    # ---- Subscript access: df['col'] read usage ----
    def visit_Subscript(self, node: ast.Subscript) -> None:
        df_name, col = self._subscript_to_df_col(node)
        if df_name and col:
            self.df_vars.add(df_name)
            # Register column usage
            self.df_cols[df_name].add(col)
        self.generic_visit(node)

    # ---- Call handling: reads/writes, to_sql, open, apply, groupby ----
    def visit_Call(self, node: ast.Call) -> None:
        func_name = self._qualified_func_name(node.func)

        # open('path', mode)
        if isinstance(node.func, ast.Name) and node.func.id == "open":
            path = self.resolve_str(node.args[0]) if node.args else None
            mode = None
            if len(node.args) >= 2:
                mode = self.resolve_str(node.args[1])
            for kw in node.keywords or []:
                if kw.arg == "mode":
                    mode = self.resolve_str(kw.value)
            if path:
                if mode and "w" in mode:
                    self.record_file_write(path)
                    self.add_edge(f"{self.filename}:open()", file_node(path, "*"), "open_write")
                else:
                    self.record_file_read(path)
                    self.add_edge(file_node(path, "*"), f"{self.filename}:open()", "open_read")

        # df.to_csv / to_parquet / to_excel
        if isinstance(node.func, ast.Attribute):
            owner = node.func.value
            attr = node.func.attr

            # DataFrame writes
            if attr in ("to_csv", "to_parquet", "to_excel", "to_json", "to_feather", "to_table"):
                df_name = getattr(owner, "id", None) or self._expr_to_df_name(owner)
                path = None
                if node.args:
                    path = self.resolve_str(node.args[0])
                if not path:
                    for kw in node.keywords or []:
                        if kw.arg in ("path", "path_or_buf", "excel_writer"):
                            path = self.resolve_str(kw.value)
                            break
                if df_name and path:
                    self.record_file_write(path)
                    # link df columns to file *
                    self.add_edge(f"{self.filename}:{df_name}", file_node(path, "*"), "write_file")

            # DataFrame to_sql
            if attr == "to_sql":
                df_name = getattr(owner, "id", None) or self._expr_to_df_name(owner)
                table = None
                con_node = None
                if node.args:
                    if len(node.args) >= 1:
                        table = self.resolve_str(node.args[0]) or self.table_vars.get(self._name_of(node.args[0]) or "", None)
                    if len(node.args) >= 2:
                        con_node = node.args[1]
                for kw in node.keywords or []:
                    if kw.arg == "name" and table is None:
                        table = self.resolve_str(kw.value) or self.table_vars.get(self._name_of(kw.value) or "", None)
                    if kw.arg == "con" and con_node is None:
                        con_node = kw.value
                dbn = self.infer_dbname_from_con(con_node) or "unknown_db"
                if df_name and table:
                    self.record_db_write(dbn, table)
                    self.add_edge(f"{self.filename}:{df_name}", db_node(dbn, table, "*"), "write_db")

        # pandas.merge(...) not assigned (in-place usage)
        if func_name and func_name.endswith("merge") and node.args:
            # If assigned handled in visit_Assign; otherwise, still register relation
            left_src = self._name_or_df(node, "left", pos=0)
            right_src = self._name_or_df(node, "right", pos=1)
            if left_src and right_src:
                # Create a synthetic merged node
                merged = f"{self.filename}:merge_result@{node.lineno}"
                self.nodes.add(merged)
                self.add_edge(f"{self.filename}:{left_src}", merged, "merge")
                self.add_edge(f"{self.filename}:{right_src}", merged, "merge")

        # pandas.read_sql(...) not assigned (e.g., used immediately)
        if func_name and func_name.endswith("read_sql"):
            sql_txt = None
            con_node = None
            if node.args:
                if len(node.args) >= 1:
                    sql_txt = self.resolve_str(node.args[0])
                if len(node.args) >= 2:
                    con_node = node.args[1]
            for kw in node.keywords or []:
                if kw.arg == "sql" and sql_txt is None:
                    sql_txt = self.resolve_str(kw.value)
                if kw.arg == "con" and con_node is None:
                    con_node = kw.value
            table = extract_table_from_sql(sql_txt or "")
            dbn = self.infer_dbname_from_con(con_node) or "unknown_db"
            if table:
                self.record_db_read(dbn, table)

        # Raw SQL INSERT/UPDATE via cursor.execute("INSERT INTO ...")
        if isinstance(node.func, ast.Attribute) and node.func.attr in ("execute", "executescript"):
            if node.args:
                sql_txt = self.resolve_str(node.args[0])
                table = extract_write_table_from_sql(sql_txt or "")
                if table:
                    # try to infer db name from known connection var on owner
                    dbn = "unknown_db"
                    owner = node.func.value
                    owner_name = self._name_of(owner)
                    if owner_name and owner_name in self.conn_vars:
                        dbn = self.conn_vars[owner_name]
                    self.record_db_write(dbn, table)

        # .apply / groupby / assign
        if isinstance(node.func, ast.Attribute):
            owner = node.func.value
            attr = node.func.attr
            # df['col'].apply(func)
            if attr == "apply":
                df_name, col = self._subscript_to_df_col(owner)
                if df_name and col:
                    # consider it transforms that column, create derived col equal to same
                    self.df_cols[df_name].add(col)
                    self.add_edge(f"{self.filename}:{df_name}[{col}]", f"{self.filename}:{df_name}[{col}]",
                                  "apply_func")
            # df.groupby([...]).agg({...})
            if attr in ("agg", "aggregate"):
                # owner should be groupby(..)
                gb_label = "groupby_agg"
                src_cols = self._collect_df_columns_in_expr(owner)
                target_node = f"{self.filename}:groupby_result@{getattr(node, 'lineno', 'x')}"
                self.nodes.add(target_node)
                for (sdf, scol) in src_cols:
                    self.add_edge(f"{self.filename}:{sdf}[{scol}]", target_node, gb_label)

        self.generic_visit(node)

    # ---- Utility AST helpers ----
    def _name_or_df(self, call: ast.Call, kw_name: str, pos: int) -> Optional[str]:
        """Return df var name from kw or positional arg if it looks like a df variable."""
        node = None
        if len(call.args) > pos:
            node = call.args[pos]
        for kw in call.keywords or []:
            if kw.arg == kw_name:
                node = kw.value
                break
        return self._expr_to_df_name(node) if node is not None else None

    def _expr_to_df_name(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Name):
            return node.id
        # attribute could be method chain like df.assign(...).something
        if isinstance(node, ast.Attribute):
            # try to find root name
            cur = node
            while isinstance(cur, ast.Attribute):
                cur = cur.value
            if isinstance(cur, ast.Name):
                return cur.id
        return None

    def _qualified_func_name(self, func: ast.AST) -> Optional[str]:
        # Return dotted name for calls like pd.read_csv -> "pd.read_csv"
        parts = []
        cur = func
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
            parts.reverse()
            return ".".join(parts)
        if isinstance(cur, ast.Name):
            return cur.id
        return None

    def _subscript_to_df_col(self, node: ast.AST) -> Tuple[Optional[str], Optional[str]]:
        """
        Convert df['col'] style Subscript to (df_name, 'col').
        Supports basic string constant keys.
        """
        if not isinstance(node, ast.Subscript):
            return (None, None)
        # base
        df_name = None
        if isinstance(node.value, ast.Name):
            df_name = node.value.id
        elif isinstance(node.value, ast.Attribute):
            # something like obj.df['col']; try to peel root Name
            root = node.value
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name):
                df_name = root.id

        # slice/key
        col = None
        key = getattr(node, "slice", None)
        # Python 3.9+: ast.Index removed; in 3.8 it's ast.Index(value=...)
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            col = key.value
        elif hasattr(ast, "Index") and isinstance(key, ast.Index) and isinstance(key.value, ast.Constant) and isinstance(key.value.value, str):  # type: ignore
            col = key.value.value  # type: ignore
        else:
            # Try resolver if it's a Name/Constant
            if hasattr(key, "value"):
                v = getattr(key, "value", None)
                sval = self.resolve_str(v) if v is not None else None
                if sval:
                    col = sval
            else:
                sval = self.resolve_str(key)  # in case
                if sval:
                    col = sval

        return (df_name, col)

    def _collect_df_columns_in_expr(self, node: ast.AST) -> Set[Tuple[str, str]]:
        """
        Walk expression and collect any df['col'] usages.
        """
        cols: Set[Tuple[str, str]] = set()

        class ColVisitor(ast.NodeVisitor):
            def visit_Subscript(self, sn: ast.Subscript) -> None:  # type: ignore
                dfn, cc = self_outer._subscript_to_df_col(sn)
                if dfn and cc:
                    cols.add((dfn, cc))
                self.generic_visit(sn)

        self_outer = self
        ColVisitor().visit(node)
        return cols

    def _label_for_expr(self, node: ast.AST) -> str:
        if isinstance(node, ast.BinOp):
            return self.guess_binop_label(node.op)
        if isinstance(node, ast.Call):
            q = self._qualified_func_name(node.func) or "call"
            if q.endswith("apply"):
                return "apply_func"
            return "call"
        return "assignment"

    def _name_of(self, node: ast.AST) -> Optional[str]:
        return node.id if isinstance(node, ast.Name) else None


# ---------- Directory traversal and aggregation ----------
def parse_python_file(path: str) -> Optional[ast.AST]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            code = f.read()
        return ast.parse(code, filename=path)
    except SyntaxError as e:
        LOGGER.warning("Syntax error in %s: %s", path, e)
    except Exception as e:
        LOGGER.warning("Failed to read/parse %s: %s", path, e)
    return None


def analyze_file(pyfile: str) -> ETLAnalyzer:
    tree = parse_python_file(pyfile)
    analyzer = ETLAnalyzer(pyfile)
    if tree is None:
        return analyzer
    try:
        analyzer.visit(tree)
    except Exception as e:
        LOGGER.warning("AST visit failed for %s: %s", pyfile, e)
    return analyzer


def collect_py_files(root: str) -> List[str]:
    out = []
    for base, _, files in os.walk(root):
        for fn in files:
            if fn.endswith(".py"):
                out.append(os.path.join(base, fn))
    return out


# ---------- Global DAG building ----------
def build_global_dag(analyzers: List[ETLAnalyzer]) -> Tuple[Set[str], List[Dict[str, str]]]:
    nodes: Set[str] = set()
    edges: List[Dict[str, str]] = []

    # Union of per-script
    for a in analyzers:
        nodes.update(a.nodes)
        edges.extend(a.edges)

    # Build maps for connections across scripts
    outputs_map: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    inputs_map: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for a in analyzers:
        for k, vs in a.outputs_by_key.items():
            outputs_map[k].update(vs)
        for k, vs in a.inputs_by_key.items():
            inputs_map[k].update(vs)

    # Connect matching outputs -> inputs
    for key in inputs_map.keys() & outputs_map.keys():
        src_nodes = outputs_map[key]
        dst_nodes = inputs_map[key]
        for s in src_nodes:
            for d in dst_nodes:
                # Avoid self-loop duplicates
                if s != d:
                    edges.append(edge_dict(s, d, "cross_script"))

    # Optionally, ensure acyclicity using networkx (warn on cycles)
    if nx is not None:
        G = nx.DiGraph()
        G.add_nodes_from(nodes)
        G.add_edges_from([(e["from"], e["to"], {"label": e["label"]}) for e in edges])
        try:
            cycles = list(nx.find_cycle(G, orientation="original"))  # type: ignore
            if cycles:
                LOGGER.warning("Potential cycles detected across scripts (ETL is expected to be DAG-like): %s", cycles[:5])
        except nx.exception.NetworkXNoCycle:
            pass
    else:
        # No cycle check without networkx
        pass

    return nodes, edges


# ---------- Main ----------
def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python analyzer.py /path/to/codebase", file=sys.stderr)
        sys.exit(1)

    root = sys.argv[1]
    if not os.path.isdir(root):
        print(f"Error: {root} is not a directory.", file=sys.stderr)
        sys.exit(2)

    py_files = collect_py_files(root)
    if not py_files:
        LOGGER.info("No Python files found under %s. Producing empty DAG.", root)
        write_output(set(), [])
        return

    analyzers: List[ETLAnalyzer] = []
    for pf in py_files:
        LOGGER.info("Analyzing %s", pf)
        analyzers.append(analyze_file(pf))

    nodes, edges = build_global_dag(analyzers)

    if not nodes and not edges:
        LOGGER.info("No ETL patterns found. Producing empty DAG.")

    write_output(nodes, edges)


def write_output(nodes: Set[str], edges: List[Dict[str, str]]) -> None:
    data = {
        "nodes": sorted(nodes),
        "edges": edges,
    }
    out_path = os.path.abspath("data_flow_dag.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        LOGGER.info("Wrote DAG to %s", out_path)
    except Exception as e:
        LOGGER.error("Failed to write output JSON: %s", e)
        print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
