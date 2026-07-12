#!/usr/bin/env python3
"""Add SUCCESS-query logging to the warehouse read API (backend-agnostic; foundation #1).
Additive only + try/except-wrapped so it can NEVER break the query path. Logs a 'query' event
(user, referenced tables, row_count, ms, snapshot_id, truncated, sql[:500]) to the existing
mcp_access.jsonl. Applies to prod /opt/duckdb/bin/mcp_server.py with a backup + py_compile."""
import sys, py_compile, subprocess, os
F = "/opt/duckdb/bin/mcp_server.py"
src = open(F).read()
if "_CURRENT_USER" in src:
    print("already patched"); sys.exit(0)

# (1) import + contextvar — insert right after `import common`
anchor1 = "import common\n"
assert src.count(anchor1) >= 1, "import common not found"
add1 = "import common\nimport contextvars as _ctxvars, re as _relog\n_CURRENT_USER = _ctxvars.ContextVar(\"mcp_user\", default=None)\n"
src = src.replace(anchor1, add1, 1)

# (2) middleware: capture the resolved user into the contextvar
anchor2 = "        request.state.user_email = email\n"
assert src.count(anchor2) == 1, "middleware user_email line not unique"
src = src.replace(anchor2, anchor2 + "        _CURRENT_USER.set(email or \"unknown\")\n", 1)

# (3) _run_query: log success just before returning (single point covers MCP + REST)
anchor3 = ('    return {"columns": cols, "rows": safe, "row_count": len(safe),\n'
           '            "execution_ms": ms, "snapshot_id": snap_id, "truncated": truncated, "note": note}')
assert src.count(anchor3) == 1, "run_query return not unique"
logblock = (
    '    try:\n'
    '        _tables = sorted(set(_relog.findall(r"\\b(?:core|main|derived|dash)\\.[A-Za-z_][A-Za-z0-9_]*", sql)))\n'
    '        common.log_event(os.path.join(PATHS["logs_dir"], "mcp_access.jsonl"), "query",\n'
    '                         user=_CURRENT_USER.get(), tables=_tables[:25], row_count=len(safe),\n'
    '                         ms=ms, snapshot_id=snap_id, truncated=truncated, sql=sql[:500])\n'
    '    except Exception:\n'
    '        pass\n'
)
src = src.replace(anchor3, logblock + anchor3, 1)

stamp = subprocess.check_output(["date","-u","+%Y%m%dT%H%M%SZ"]).decode().strip()
subprocess.run(["cp","-p",F,f"{F}.bak-qlog-{stamp}"], check=True)
open(F,"w").write(src)
py_compile.compile(F, doraise=True)
print(f"PATCHED + COMPILED ok; backup {F}.bak-qlog-{stamp}")
