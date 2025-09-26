import argparse, os, re, sys 
import psycopg2 
from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD 

# ---------- helpers for SQL logging ---------- 
SQL_LOG_PATH = "queries.sql" 
def log_sql(header, sql): 
    with open(SQL_LOG_PATH, "a", encoding="utf-8") as f: 
        f.write(f"\n-- {header}\n") 
        f.write(sql.strip() + ";\n")

# ---------- parse the input schema file ---------- 
TABLE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$") 
FK_RE = re.compile(r"\(fk\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", re.I) 
PK_TAG = "(pk)"

def smart_split_cols(inner): # split by commas not inside parentheses 
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
    tname = m.group(1).strip().lower() # <-- force lower 
    inner = m.group(2) 
    
    columns = smart_split_cols(inner) 
    pk_col = None 
    fks = [] 
    pure_cols = [] 
    
    for coldef in columns: 
        coldef_clean = coldef.strip() 
        name_part = coldef_clean.split("(")[0].strip().lower() # <-- lower 
        if name_part: 
            pure_cols.append(name_part) 
        # PK tag tolerant 
        if PK_TAG in coldef_clean.replace(" ", "").lower(): 
            pk_col = name_part 
            
        # FK tag tolerant 
        fk_m = FK_RE.search(coldef_clean) 
        if fk_m: 
            ref_table = fk_m.group(1).strip().lower() # <-- lower 
            ref_col = fk_m.group(2).strip().lower() # <-- lower 
            fks.append({"col": name_part, "ref_table": ref_table, "ref_col": ref_col}) 
            
    return {"table": tname, "columns": pure_cols, "pk": pk_col, "fks": fks}

def parse_input_file(path): 
    tables = [] 
    with open(path, "r", encoding="utf-8") as f: 
        for raw in f: 
            raw = raw.strip("\ufeff").strip() 
            if not raw: 
                continue 
            # ignore filenames, separators, and comments 
            if raw.lower().endswith((".txt", ".sql", ".out")): 
                continue 
            if all(ch == "-" for ch in raw) and len(raw) >= 5: 
                continue 
            if raw.startswith(("--", "#", "//")): 
                continue 
            if "(" not in raw or ")" not in raw: 
                continue 
            parsed = parse_schema_line(raw) 
            if parsed: 
                tables.append(parsed) 
    return tables

# ---------- DB connection ---------- 
def connect(): 
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

# ---------- SQL helpers ---------- 
def q_count_all(table): 
    return f"SELECT COUNT(*) FROM {table}" 

def q_count_join_left_on_fk(left, fk_col, right, right_pk): 
    return ("SELECT COUNT(*) " f"FROM {left} INNER JOIN {right} ON {left}.{fk_col} = {right}.{right_pk}")

def q_exists_rows(table, where_sql): 
    return f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {where_sql} LIMIT 1)" 

def q_exists_fd_violation(table, x, y):
    # FD X→Y exists for some repeated X if:
    # - COUNT(*) > 1  (the determinant value repeats)
    # - At least one non-NULL Y in that group
    # - MIN(Y) IS NOT DISTINCT FROM MAX(Y) (NULL-safe "all equal")
    return (
        "SELECT EXISTS ("
        f"  SELECT 1 FROM {table} "
        f"  GROUP BY {x} "
        f"  HAVING COUNT(*) > 1 "
        f"     AND COUNT({y}) FILTER (WHERE {y} IS NOT NULL) > 0 "
        f"     AND MIN({y}) IS NOT DISTINCT FROM MAX({y})"
        ")"
    )


def q_table_has_column(table, col): 
    return (
        "SELECT EXISTS (" 
        " SELECT 1 FROM information_schema.columns " 
        f" WHERE table_name = '{table.lower()}' AND column_name = '{col.lower()}'" 
        ")" 
        )

def get_actual_columns(cur, table): 
    """ Return the actual column names from the DB for table, in order. """ 
    cur.execute(
        "SELECT column_name FROM information_schema.columns " 
        "WHERE table_name = %s ORDER BY ordinal_position", 
        (table.lower(),) ) 
    return [r[0] for r in cur.fetchall()]

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
    # table must exist 
    cur.execute("SELECT to_regclass(%s)", (t["table"],)) 
    if cur.fetchone()[0] is None: 
        return False, "table-missing" 
    
    # PK must be present in input and exist as a column 
    pk = t.get("pk") 
    if not pk: 
        return False, "pk-missing"
    if not safe_fetch_bool(cur, q_table_has_column(t["table"], pk), f"check pkcol {t['table']}.{pk}"): 
        return False, "pkcol-missing" 
    
    # If FKs exist: the FK column on THIS table must exist. 
    # (Do NOT hard-fail on referenced table/col; the RI check will reflect it.) 
    for fk in t["fks"]: 
        if not safe_fetch_bool(cur, q_table_has_column(t["table"], fk["col"]), f"check fkcol {t['table']}.{fk['col']}"): 
            return False, f"fkcol-missing:{fk['col']}" 
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
            f"count join {t['table']}..{fk['col']} -> {fk['ref_table']}..{fk['ref_col']}" ) 
        if join_n != n_all: 
            return False 
    return True

def check_normalization_3nf_bcnf(cur, t):
    cols = [c.lower() for c in get_actual_columns(cur, t["table"])]
    if not cols:
        return True

    pk = (t.get("pk") or "").lower()
    if pk not in cols:
        pk = cols[0]

    non_pk_cols = [c for c in cols if c != pk]

    # NEW: build a set of FK columns for this table
    fk_cols = { fk["col"].lower() for fk in t.get("fks", []) }

    # Only check FDs where:
    # - X is a non-PK column AND not an FK column
    # - Y is a non-PK column (as before; ignore X→PK)
    for x in non_pk_cols:
        if x in fk_cols:
            continue  # skip FK determinants
        for y in non_pk_cols:
            if y == x:
                continue
            sql = q_exists_fd_violation(t["table"], x, y)
            if safe_fetch_bool(cur, sql, f"FD check {t['table']}: {x} -> {y}"):
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
        print("Error: provide input schema file as python3 checkdb.py database=<file>.txt") 
        sys.exit(1) 
        
    # Reset SQL log; include comment with input file name:contentReference[oaicite:10]{index=10} 
    with open(SQL_LOG_PATH, "w", encoding="utf-8") as f: 
        f.write(f"-- checkdb.sql generated for input: {os.path.basename(schema_path)}\n") 
            
        tables = parse_input_file(schema_path) 
        if not tables: 
            print("No valid tables parsed from input file.") 
            sys.exit(1) 
            
    out_path = "output.txt" 
    rows_for_output = []

    try: 
        conn = connect() 
        cur = conn.cursor() 
    except Exception as e: 
        print(f"DB connection failed: {e}") 
        sys.exit(1) 
        
    for t in tables: 
        ok, reason = check_table_exists_and_columns(cur, t) 
        if not ok: 
            # RI unknown -> N; still attempt normalization best-effort 
            ri_ok = False 
            norm_ok = check_normalization_3nf_bcnf(cur, t) 
            rows_for_output.append((t["table"], "Y" if ri_ok else "N", "Y" if norm_ok else "N")) 
            continue 
        else: 
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
    with open(out_path, "a", encoding="utf-8") as f: 
        f.write("\n" + "-"*50 + "\n")
        f.write("referential integrity normalized\n") 
        for tname, ri, norm in rows_for_output: 
            f.write(f"{tname}\t\t{ri}\t\t{norm}\n") 
        f.write(f"\nDB referential integrity: {db_ri}\n") 
        f.write(f"DB normalized: {db_norm}\n") 
        
    # Mirror to stdout for convenience 
    print(open(out_path, "r", encoding="utf-8").read()) 
        
if __name__ == "__main__": main()
