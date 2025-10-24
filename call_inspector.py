# call_inspector.py (robust)
import ast, os, json

def call_name(node):
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
    return node.__class__.__name__

def describe_arg(a):
    if a is None:
        return "NONE"
    if isinstance(a, ast.Constant):
        return f"CONST:{repr(a.value)}"
    if isinstance(a, ast.Name):
        return f"NAME:{a.id}"
    if isinstance(a, ast.JoinedStr):
        parts = []
        for v in a.values:
            if isinstance(v, ast.Constant):
                parts.append(repr(v.value))
            elif isinstance(v, ast.FormattedValue):
                fv = v.value
                if isinstance(fv, ast.Name):
                    parts.append("{" + fv.id + "}")
                else:
                    parts.append("{expr}")
            else:
                parts.append("{?}")
        return "FSTR:" + "+".join(parts)
    if isinstance(a, ast.BinOp) and isinstance(a.op, ast.Add):
        left = describe_arg(a.left)
        right = describe_arg(a.right)
        return f"CONCAT({left}+{right})"
    if isinstance(a, ast.Subscript):
        return "SUBSCRIPT"
    return a.__class__.__name__

def inspect_file(path):
    src = open(path, "r", encoding="utf-8").read()
    try:
        tree = ast.parse(src, filename=path)
    except Exception as e:
        return {"error": str(e)}
    calls = []
    class V(ast.NodeVisitor):
        def visit_Call(self, node):
            nm = call_name(node.func)
            first = node.args[0] if node.args else None
            argdesc = describe_arg(first)
            lineno = getattr(node, "lineno", None)
            snippet = None
            try:
                lines = src.splitlines()
                if lineno and 1 <= lineno <= len(lines):
                    snippet = lines[lineno-1].strip()
            except:
                snippet = None
            calls.append({"lineno": lineno, "call": nm, "arg": argdesc, "snippet": snippet})
            self.generic_visit(node)
    V().visit(tree)
    return {"calls": calls}

def main():
    cwd = os.getcwd()
    print("Working directory:", cwd)
    py_files = [f for f in os.listdir(".") if f.endswith(".py") and f not in ("analyzer.py","call_inspector.py","analyzer_debug.py")]
    if not py_files:
        print("No .py files found in directory.")
        return
    overall = {}
    for f in sorted(py_files):
        print("\n=== FILE:", f, "===")
        res = inspect_file(f)
        if "error" in res:
            print("  parse error:", res["error"])
            continue
        if not res["calls"]:
            print("  (no calls found)")
        else:
            for c in res["calls"]:
                ln = str(c['lineno']) if c['lineno'] is not None else "?"
                arg = c['arg'] if c['arg'] is not None else "NONE"
                snippet = c['snippet'] if c['snippet'] is not None else ""
                print(f"  line {ln.rjust(3)} | {c['call']:<40} | arg={arg:<40} | snippet={snippet}")
        overall[f] = res
    with open("call_inspector_output.json", "w", encoding="utf-8") as fh:
        json.dump(overall, fh, indent=2)
    print("\nWrote call_inspector_output.json")

if __name__ == "__main__":
    main()
