import argparse, os, re, sys
import psycopg2
from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

# ---------- helpers for SQL logging ----------
SQL_LOG_PATH = "checkdb.sql"
def log_sql(header, sql):
    with open(SQL_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n-- {header}\n")
        f.write(sql.strip() + ";\n")

# ---------- parse the input schema file ----------
TABLE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$")
FK_RE    = re.compile(r"\(fk\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", re.I)
PK_TAG   = "(pk)"

def smart_split_cols(inner):
    # split by commas not inside parentheses
    parts, buf, depth = [], [], 0
    for ch in inner:
        if ch == '(':
            depth += 1
            buf.append(ch)
        elif ch == ')':
            depth -= 1
            buf.append(ch)
        elif ch == ',' and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return parts

def parse_schema_line(line):
    m = TABLE_RE.match(line)
    if not m: return None
    tname, inner = m.group(1), m.group(2)
    columns = smart_split_cols(inner)
    pk_col = None
    fks = []  # list of dicts: {col, ref_table, ref_col}
    pure_cols = []
    for coldef in columns:
        # shapes: id(pk) | i(fk:T2.i) | A
        name_part = coldef.split("(")[0].strip()
        pure_cols.append(name_part)
        if PK_TAG in coldef.replace(" ", "").lower().replace("pk","pk"):
            pk_col = name_part
        fk_m = FK_RE.search(coldef)
        if fk_m:
            ref_table, ref_col = fk_m.group(1), fk_m.group(2)
            fks.append({"col": name_part, "ref_table": ref_table, "ref_col": ref_col})
    return {"table": tname, "columns": pure_cols, "pk": pk_col, "fks": fks}

def parse_input_file(path):
    tables = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("--"): continue
            parsed = parse_schema_line(line)
            if parsed: tables.append(parsed)
    return tables

# ---------- DB connection ----------
def connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

# ---------- SQL helpers ----------
def q_count_all(table):
    return f"SELECT COUNT(*) FROM {table}"

def q_count_join_left_on_fk(left, fk_col, right, right_pk):
    return (
        "SELECT COUNT(*) "
        f"FROM {left} INNER JOIN {right} ON {left}.{fk_col} = {right}.{right_pk}"
    )

def q_exists_rows(table, where_sql):
    return f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {where_sql} LIMIT 1)"

def q_exists_fd_violation(table, x, y):
    # FD X→Y holds for some repeated X if MIN(y) == MAX(y) (NULL-safe)
    return (
        "SELECT EXISTS ("
        f"  SELECT 1 FROM {table} "
        f"  GROUP BY {x} "
        f"  HAVING COUNT(*) > 1 AND MIN({y}) IS NOT DISTINCT FROM MAX({y})"
        ")"
    )

def q_table_has_column(table, col):
    return (
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.columns "
        f"  WHERE table_name = '{table.lower()}' AND column_name = '{col.lower()}'"
        ")"
    )

# ---------- checks ----------
def safe_fetch_bool(cur, sql, header):
    log_sql(header, sql)
    cur.execute(sql)
    v = cur.fetchone()[0]
    return bool(v)

def safe_fetch_int(cur, sql, header):
    log_sql(header, sql)
    cur.execute(sql)
    return int(cur.fetchone()[0])

def check_table_exists_and_columns(cur, t):
    # must have table
    cur.execute(f"SELECT to_regclass('{t['table']}')")
    if cur.fetchone()[0] is None:
        return False, "table-missing"

    # must have a PK name in the input AND that column must exist
    pk = t.get("pk")
    if not pk:
        return False, "pk-missing"
    if not safe_fetch_bool(cur,
                           q_table_has_column(t["table"], pk),
                           f"check pkcol {t['table']}.{pk}"):
        return False, "pkcol-missing"

    # only validate FKs strictly (FK col + referenced table/col)
    for fk in t["fks"]:
        if not safe_fetch_bool(cur, q_table_has_column(t["table"], fk["col"]),
                               f"check fkcol {t['table']}.{fk['col']}"):
            return False, f"fkcol-missing:{fk['col']}"
        cur.execute(f"SELECT to_regclass('{fk['ref_table']}')")
        if cur.fetchone()[0] is None:
            return False, f"reftable-missing:{fk['ref_table']}"
        if not safe_fetch_bool(cur, q_table_has_column(fk["ref_table"], fk["ref_col"]),
                               f"check refcol {fk['ref_table']}.{fk['ref_col']}"):
            return False, f"refcol-missing:{fk['ref_table']}.{fk['ref_col']}"
    return True, "ok"

def check_referential_integrity(cur, t):
    # Tables without FK are trivially Y:contentReference[oaicite:7]{index=7}
    if not t["fks"]:
        return True
    # For each FK, compare |T| with |T ⋈ FK T_ref|
    n_all = safe_fetch_int(cur, q_count_all(t["table"]), f"count {t['table']}")
    for fk in t["fks"]:
        join_n = safe_fetch_int(
            cur,
            q_count_join_left_on_fk(t["table"], fk["col"], fk["ref_table"], fk["ref_col"]),
            f"count join {t['table']}..{fk['col']} -> {fk['ref_table']}..{fk['ref_col']}"
        )
        if join_n != n_all:
            return False
    return True

def check_normalization_3nf_bcnf(cur, t):
    # Per spec: simple PKs, skip 1NF/2NF; 3NF/BCNF OK iff no FD with non-PK determinant:contentReference[oaicite:8]{index=8}
    pk = t["pk"]
    non_pk_cols = [c for c in t["columns"] if c != pk]
    # If any non-PK X determines any other attribute Y (including other non-PKs or even FKs), it violates 3NF/BCNF.
    # We only count a violation when the X pattern repeats (COUNT(*)>1) and is single-valued for Y (COUNT(DISTINCT Y)=1).
    for x in non_pk_cols:
        for y in t["columns"]:
            if y == x: 
                continue
            sql = q_exists_fd_violation(t["table"], x, y)
            if safe_fetch_bool(cur, sql, f"FD check {t['table']}: {x} -> {y}"):
                # We found a non-PK determinant X → Y repeating => not normalized
                return False
    return True

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("database", nargs="?", help="(compat) use database=<file>.txt style")
    ap.add_argument("--database", dest="database_kw", help="path to input schema file")
    args, unknown = ap.parse_known_args()

    # Support "database=foo.txt" style like the spec:contentReference[oaicite:9]{index=9}
    schema_path = None
    if args.database_kw:
        schema_path = args.database_kw
    elif args.database and args.database.startswith("database="):
        schema_path = args.database.split("=", 1)[1]
    else:
        # also accept a plain positional path
        schema_path = args.database

    if not schema_path or not os.path.exists(schema_path):
        print("Error: provide input schema file as `python3 checkdb.py database=<file>.txt`")
        sys.exit(1)

    # Reset SQL log; include comment with input file name:contentReference[oaicite:10]{index=10}
    with open(SQL_LOG_PATH, "w", encoding="utf-8") as f:
        f.write(f"-- checkdb.sql generated for input: {os.path.basename(schema_path)}\n")

    tables = parse_input_file(schema_path)
    if not tables:
        print("No valid tables parsed from input file.")
        sys.exit(1)

    out_path = f"refintnorm-{os.path.basename(schema_path)}"
    rows_for_output = []

    try:
        conn = connect()
        cur = conn.cursor()
    except Exception as e:
        print(f"DB connection failed: {e}")
        sys.exit(1)

    # Process each table; continue on errors (skip bad tables):contentReference[oaicite:11]{index=11}
    for t in tables:
        ok, reason = check_table_exists_and_columns(cur, t)
        if not ok:
            # If invalid, mark as 'N' for both (permissive: we skip checks but reflect failure)
            rows_for_output.append((t["table"], "N", "N"))
            continue

        ri_ok = check_referential_integrity(cur, t)
        norm_ok = check_normalization_3nf_bcnf(cur, t)
        rows_for_output.append((t["table"], "Y" if ri_ok else "N", "Y" if norm_ok else "N"))

    cur.close()
    conn.close()

    # Sort by table name and compute DB summaries:contentReference[oaicite:12]{index=12}
    rows_for_output.sort(key=lambda x: x[0].lower())
    db_ri = "Y" if all(r[1] == "Y" for r in rows_for_output) else "N"
    db_norm = "Y" if all(r[2] == "Y" for r in rows_for_output) else "N"

    # Write output in your exact format
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("referential integrity normalized\n")
        for tname, ri, norm in rows_for_output:
            f.write(f"{tname}\t\t{ri}\t\t{norm}\n")
        f.write(f"\nDB referential integrity: {db_ri}\n")
        f.write(f"DB normalized: {db_norm}\n")

    # Mirror to stdout for convenience
    print(open(out_path, "r", encoding="utf-8").read())

if __name__ == "__main__":
    main()
