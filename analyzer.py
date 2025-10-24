#!/usr/bin/env python3
# analyzer.py (robust: handles int/str concat, f-strings, jdbc dbtable, simple var resolution)
"""
Enhanced static PySpark lineage analyzer for MVP:
- Two-pass approach: gather simple variable constants (including f-strings that reference config/db)
  then resolve call sites using those simple vars.
- Detects spark.read.table, spark.table, spark.sql, .write.saveAsTable/.insertInto/.save
- Detects JDBC pattern: .option("dbtable", <value>) -> treat as read of that value
- Outputs lineage_output.json and lineage_graph.png
"""
import ast
import os
import json
import yaml
import networkx as nx
import matplotlib.pyplot as plt

PROJECT_DIR = "."
OUTPUT_JSON = "lineage_output.json"
GRAPH_PNG = "lineage_graph.png"

# ----------------- helpers -----------------
def load_config(path="config.yml"):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            try:
                return yaml.safe_load(fh) or {}
            except Exception:
                return {}
    return {}

def call_name(node):
    """Reconstruct dotted name like spark.read.table or df.write.saveAsTable"""
    if isinstance(node, ast.Attribute):
        parts = []
        cur = node
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
        return ".".join(reversed(parts))
    if isinstance(node, ast.Name):
        return node.id
    return ""

def describe_node(node):
    if node is None:
        return None
    if isinstance(node, ast.Constant):
        return ("const", node.value)
    if isinstance(node, ast.Name):
        return ("name", node.id)
    if isinstance(node, ast.JoinedStr):
        # f-string: collect parts (constants or Names)
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(("const", v.value))
            elif isinstance(v, ast.FormattedValue):
                inner = v.value
                if isinstance(inner, ast.Name):
                    parts.append(("name", inner.id))
                else:
                    parts.append(("expr", ast.dump(inner)))
            else:
                parts.append(("expr", ast.dump(v)))
        return ("fstring", parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = describe_node(node.left)
        right = describe_node(node.right)
        return ("concat", left, right)
    if isinstance(node, ast.Subscript):
        return ("subscript", ast.dump(node))
    return ("other", ast.dump(node))

def eval_simple_node_desc(desc, simple_vars, config):
    """
    Try to evaluate a described node into a string using simple_vars and config.
    desc: output of describe_node
    Returns a string or None.
    """
    if desc is None:
        return None
    kind = desc[0]
    if kind == "const":
        return desc[1]
    if kind == "name":
        name = desc[1]
        if name in simple_vars:
            val = simple_vars[name]
            if isinstance(val, (str, int, float)):
                return val
            return None
        if name == "db":
            val = config.get("env", {}).get("db")
            if isinstance(val, (str, int, float)):
                return val
            return None
        return None
    if kind == "fstring":
        parts = []
        ok = True
        for p in desc[1]:
            pkind = p[0]
            if pkind == "const":
                parts.append(str(p[1]))
            elif pkind == "name":
                val = simple_vars.get(p[1])
                if val is None and p[1] == "db":
                    val = config.get("env", {}).get("db")
                if val is None or not isinstance(val, (str, int, float)):
                    ok = False
                    break
                parts.append(str(val))
            else:
                ok = False
                break
        if ok:
            return "".join(parts)
        return None
    if kind == "concat":
        left = eval_simple_node_desc(desc[1], simple_vars, config)
        right = eval_simple_node_desc(desc[2], simple_vars, config)
        if left is not None and right is not None:
            return str(left) + str(right)
        return None
    return None

# ----------------- Analyzer -----------------
class SimpleCollector(ast.NodeVisitor):
    def __init__(self, config):
        self.config = config or {}
        self.simple_vars = {}

    def visit_Assign(self, node):
        if len(node.targets) != 1:
            return self.generic_visit(node)
        tgt = node.targets[0]
        if not isinstance(tgt, ast.Name):
            return self.generic_visit(node)
        name = tgt.id
        val_desc = describe_node(node.value)

        if isinstance(node.value, ast.Call):
            fn = call_name(node.value.func)
            if fn.endswith("yaml.safe_load") or fn.endswith("safe_load"):
                self.simple_vars[name] = self.config
                return

        evaled = eval_simple_node_desc(val_desc, self.simple_vars, self.config)
        if evaled is not None:
            self.simple_vars[name] = evaled
            return

        if val_desc and val_desc[0] == "name":
            other = val_desc[1]
            if other in self.simple_vars:
                self.simple_vars[name] = self.simple_vars[other]
                return

        return

class CallScanner(ast.NodeVisitor):
    def __init__(self, simple_vars, config):
        self.simple_vars = simple_vars
        self.config = config or {}
        self.reads = []
        self.writes = []
        self.jdbc_dbtable_by_lineno = {}

    def visit_Call(self, node):
        cn = call_name(node.func)

        if cn.endswith(".option") or cn == "option":
            if len(node.args) >= 2:
                first = describe_node(node.args[0])
                second = describe_node(node.args[1])
                first_val = eval_simple_node_desc(first, self.simple_vars, self.config)
                if first_val is None and first and first[0] == "const":
                    first_val = first[1]
                if first_val and str(first_val).lower() == "dbtable":
                    second_val = eval_simple_node_desc(second, self.simple_vars, self.config)
                    if second_val is None and second and second[0] == "name":
                        nm = second[1]
                        second_val = self.simple_vars.get(nm)
                    if second_val:
                        lineno = getattr(node, "lineno", None)
                        if lineno:
                            self.jdbc_dbtable_by_lineno[lineno] = second_val
                            self.reads.append((lineno, str(second_val)))

        if cn.endswith(".read.table") or cn.endswith(".table"):
            table_desc = describe_node(node.args[0]) if node.args else None
            table_val = eval_simple_node_desc(table_desc, self.simple_vars, self.config)
            if table_val is None and table_desc and table_desc[0] == "name":
                nm = table_desc[1]
                table_val = self.simple_vars.get(nm)
            if table_val:
                self.reads.append((getattr(node, "lineno", None), str(table_val)))

        if cn.endswith(".load") or cn == "load":
            lineno = getattr(node, "lineno", None)
            if lineno:
                val = self.jdbc_dbtable_by_lineno.get(lineno)
                if not val:
                    for l in range(lineno-3, lineno+1):
                        if l in self.jdbc_dbtable_by_lineno:
                            val = self.jdbc_dbtable_by_lineno[l]
                            break
                if val:
                    self.reads.append((lineno, str(val)))

        if cn.endswith(".sql"):
            if node.args:
                first = describe_node(node.args[0])
                sql_text = None
                if first and first[0] == "const":
                    sql_text = first[1]
                if sql_text:
                    self._extract_tables_from_sql(sql_text, getattr(node, "lineno", None))

        if cn.endswith(".write.saveAsTable") or cn.endswith(".write.insertInto") or cn.endswith(".write.save"):
            if node.args:
                argd = describe_node(node.args[0])
                val = eval_simple_node_desc(argd, self.simple_vars, self.config)
                if val is None and argd and argd[0] == "name":
                    nm = argd[1]
                    val = self.simple_vars.get(nm)
                if val:
                    self.writes.append((getattr(node, "lineno", None), str(val)))

        self.generic_visit(node)

    def _extract_tables_from_sql(self, sql_text, lineno):
        if not sql_text:
            return
        sql_lower = sql_text.lower()
        if "insert into" in sql_lower:
            try:
                idx = sql_lower.index("insert into") + len("insert into")
                rest = sql_text[idx:].strip()
                table = rest.split()[0].strip().strip(";").strip("()")
                self.writes.append((lineno, table))
            except Exception:
                pass
        if " from " in sql_lower:
            try:
                parts = sql_lower.split(" from ")
                after = parts[1]
                table = after.split()[0].strip().strip(";").strip("()")
                self.reads.append((lineno, table))
            except Exception:
                pass

# ----------------- Controller -----------------
def analyze_project(project_dir="."):
    config = load_config(os.path.join(project_dir, "config.yml"))
    py_files = []
    for root, _, files in os.walk(project_dir):
        for fname in files:
            if fname.endswith(".py") and fname != os.path.basename(__file__):
                py_files.append(os.path.join(root, fname))
    py_files = sorted(py_files)

    results = {"files": {}, "edges": []}

    for path in py_files:
        rel = os.path.relpath(path, project_dir)
        try:
            src = open(path, "r", encoding="utf-8").read()
            tree = ast.parse(src, filename=rel)
        except Exception as e:
            print(f"Skipping {rel}: parse error {e}")
            continue

        collector = SimpleCollector(config)
        collector.visit(tree)
        simple_vars = collector.simple_vars

        scanner = CallScanner(simple_vars, config)
        scanner.visit(tree)

        results["files"][rel] = {
            "reads": scanner.reads,
            "writes": scanner.writes,
            "simple_vars": simple_vars,
        }

        read_tables = set([t for _, t in scanner.reads if t])
        write_tables = set([t for _, t in scanner.writes if t])
        for r in read_tables:
            for w in write_tables:
                results["edges"].append({"source": r, "target": w, "via": rel, "confidence": 0.8})

    return results

# ----------------- JSON sanitization -----------------
def sanitize_for_json(obj):
    """Recursively convert bytes to str (safe) so JSON dump works."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, bytes):
        # decode with replacement to avoid UnicodeDecodeError
        return obj.decode("utf-8", errors="replace")
    elif isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    else:
        # fallback: convert unknown types to string
        return str(obj)


# ----------------- Output -----------------
def write_output(results, out_json=OUTPUT_JSON, png=GRAPH_PNG):
    with open(out_json, "w", encoding="utf-8") as fh:
        json.dump(sanitize_for_json(results), fh, indent=2)

    G = nx.DiGraph()
    for e in results.get("edges", []):
        G.add_node(e["source"], type="table")
        G.add_node(e["target"], type="table")
        G.add_edge(e["source"], e["target"], via=e.get("via"))

    plt.figure(figsize=(8, 6))
    if len(G) == 0:
        plt.text(0.5, 0.5, "No edges detected", ha="center", va="center")
    else:
        pos = nx.spring_layout(G, seed=42)
        nx.draw(G, pos, with_labels=True, node_size=2000, font_size=8, arrowsize=20)
        edge_labels = {(u, v): d.get("via") for u, v, d in G.edges(data=True)}
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7)
    plt.savefig(png, bbox_inches="tight")
    plt.close()
    print(f"Wrote {out_json} and {png}")

if __name__ == "__main__":
    res = analyze_project(PROJECT_DIR)
    write_output(res)
