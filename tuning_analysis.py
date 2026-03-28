#!/usr/bin/env python3
# tuning_analysis_multi.py
# Cria um banco Postgres a partir dos .tbl do TPC-H e mede power / throughput / qphh
# para múltiplos cenários de tuning lidos dinamicamente de arquivos JSON.
# Mantém compatibilidade com os 4 argumentos antigos.

import argparse
import io
import json
import math
import os
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Iterable

import psycopg2


# =========================
# Config fixo do benchmark
# =========================

SF_DEFAULT = 0.1
NUM_STREAMS_DEFAULT = 4


# =========================
# Helpers de conexão
# =========================


def make_conn_params(args) -> Dict[str, str]:
    return {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "dbname": args.dbname,
    }



def get_admin_conn(args):
    params = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "dbname": "postgres",
    }
    return psycopg2.connect(**params)



def create_or_replace_database(args):
    admin_conn = get_admin_conn(args)
    admin_conn.autocommit = True
    cur = admin_conn.cursor()

    cur.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = %s AND pid <> pg_backend_pid();",
        (args.dbname,),
    )

    # Observação: idealmente validar/whitelist dbname antes de interpolar.
    cur.execute(f"DROP DATABASE IF EXISTS {args.dbname};")
    cur.execute(f"CREATE DATABASE {args.dbname};")

    cur.close()
    admin_conn.close()



def get_conn(conn_params: Dict[str, str]):
    return psycopg2.connect(**conn_params)


# =========================
# Schema TPC-H
# =========================

TPC_H_TABLES = {
    "region": {
        "file": "region.tbl",
        "columns": [
            "r_regionkey",
            "r_name",
            "r_comment",
        ],
        "ddl": """
        CREATE TABLE region (
            r_regionkey INTEGER NOT NULL,
            r_name      CHAR(25) NOT NULL,
            r_comment   VARCHAR(152)
        );
        """,
    },
    "nation": {
        "file": "nation.tbl",
        "columns": [
            "n_nationkey",
            "n_name",
            "n_regionkey",
            "n_comment",
        ],
        "ddl": """
        CREATE TABLE nation (
            n_nationkey INTEGER NOT NULL,
            n_name      CHAR(25) NOT NULL,
            n_regionkey INTEGER NOT NULL,
            n_comment   VARCHAR(152)
        );
        """,
    },
    "part": {
        "file": "part.tbl",
        "columns": [
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
        ],
        "ddl": """
        CREATE TABLE part (
            p_partkey     INTEGER NOT NULL,
            p_name        VARCHAR(55) NOT NULL,
            p_mfgr        CHAR(25) NOT NULL,
            p_brand       CHAR(10) NOT NULL,
            p_type        VARCHAR(25) NOT NULL,
            p_size        INTEGER NOT NULL,
            p_container   CHAR(10) NOT NULL,
            p_retailprice DECIMAL(15,2) NOT NULL,
            p_comment     VARCHAR(23) NOT NULL
        );
        """,
    },
    "supplier": {
        "file": "supplier.tbl",
        "columns": [
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment",
        ],
        "ddl": """
        CREATE TABLE supplier (
            s_suppkey   INTEGER NOT NULL,
            s_name      CHAR(25) NOT NULL,
            s_address   VARCHAR(40) NOT NULL,
            s_nationkey INTEGER NOT NULL,
            s_phone     CHAR(15) NOT NULL,
            s_acctbal   DECIMAL(15,2) NOT NULL,
            s_comment   VARCHAR(101) NOT NULL
        );
        """,
    },
    "partsupp": {
        "file": "partsupp.tbl",
        "columns": [
            "ps_partkey",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        ],
        "ddl": """
        CREATE TABLE partsupp (
            ps_partkey    INTEGER NOT NULL,
            ps_suppkey    INTEGER NOT NULL,
            ps_availqty   INTEGER NOT NULL,
            ps_supplycost DECIMAL(15,2) NOT NULL,
            ps_comment    VARCHAR(199) NOT NULL
        );
        """,
    },
    "customer": {
        "file": "customer.tbl",
        "columns": [
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        ],
        "ddl": """
        CREATE TABLE customer (
            c_custkey    INTEGER NOT NULL,
            c_name       VARCHAR(25) NOT NULL,
            c_address    VARCHAR(40) NOT NULL,
            c_nationkey  INTEGER NOT NULL,
            c_phone      CHAR(15) NOT NULL,
            c_acctbal    DECIMAL(15,2) NOT NULL,
            c_mktsegment CHAR(10) NOT NULL,
            c_comment    VARCHAR(117) NOT NULL
        );
        """,
    },
    "orders": {
        "file": "orders.tbl",
        "columns": [
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        ],
        "ddl": """
        CREATE TABLE orders (
            o_orderkey      INTEGER NOT NULL,
            o_custkey       INTEGER NOT NULL,
            o_orderstatus   CHAR(1) NOT NULL,
            o_totalprice    DECIMAL(15,2) NOT NULL,
            o_orderdate     DATE NOT NULL,
            o_orderpriority CHAR(15) NOT NULL,
            o_clerk         CHAR(15) NOT NULL,
            o_shippriority  INTEGER NOT NULL,
            o_comment       VARCHAR(79) NOT NULL
        );
        """,
    },
    "lineitem": {
        "file": "lineitem.tbl",
        "columns": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        ],
        "ddl": """
        CREATE TABLE lineitem (
            l_orderkey      INTEGER NOT NULL,
            l_partkey       INTEGER NOT NULL,
            l_suppkey       INTEGER NOT NULL,
            l_linenumber    INTEGER NOT NULL,
            l_quantity      DECIMAL(15,2) NOT NULL,
            l_extendedprice DECIMAL(15,2) NOT NULL,
            l_discount      DECIMAL(15,2) NOT NULL,
            l_tax           DECIMAL(15,2) NOT NULL,
            l_returnflag    CHAR(1) NOT NULL,
            l_linestatus    CHAR(1) NOT NULL,
            l_shipdate      DATE NOT NULL,
            l_commitdate    DATE NOT NULL,
            l_receiptdate   DATE NOT NULL,
            l_shipinstruct  CHAR(25) NOT NULL,
            l_shipmode      CHAR(10) NOT NULL,
            l_comment       VARCHAR(44) NOT NULL
        );
        """,
    },
}



def create_schema(conn):
    cur = conn.cursor()
    cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    for _, info in TPC_H_TABLES.items():
        cur.execute(info["ddl"])
    conn.commit()
    cur.close()



def load_tbl_into_table(conn, table_name: str, filepath: str, columns: List[str]):
    print(f"Carregando {filepath} em {table_name}...")
    with open(filepath, "r", encoding="latin1") as f:
        lines = []
        for line in f:
            line = line.rstrip("\n")
            if line.endswith("|"):
                line = line[:-1]
            lines.append(line + "\n")
        data = "".join(lines)

    buf = io.StringIO(data)
    copy_sql = (
        f"COPY {table_name} ({', '.join(columns)}) "
        "FROM STDIN WITH (FORMAT csv, DELIMITER '|');"
    )

    cur = conn.cursor()
    cur.copy_expert(copy_sql, buf)
    conn.commit()
    cur.close()



def load_all_data(conn, tabelas_dir: str):
    for table_name, info in TPC_H_TABLES.items():
        filepath = os.path.join(tabelas_dir, info["file"])
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo {filepath} não encontrado")
        load_tbl_into_table(conn, table_name, filepath, info["columns"])


# =========================
# Refresh functions (RF1 / RF2)
# =========================


def build_refresh_sql(conn, fraction: float = 0.001) -> Tuple[str, str]:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM orders;")
    initial_order_count = cur.fetchone()[0]
    cur.close()

    new_orders_count = max(1, int(initial_order_count * fraction))

    rf1_sql = f"""
    INSERT INTO orders (
        o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
        o_orderpriority, o_clerk, o_shippriority, o_comment
    )
    SELECT
        (SELECT MAX(o_orderkey) FROM orders) + ROW_NUMBER() OVER (ORDER BY o_orderkey) AS o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        CURRENT_DATE AS o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment
    FROM orders
    LIMIT {new_orders_count};

    INSERT INTO lineitem (
        l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
        l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
        l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
    )
    SELECT
        (SELECT MAX(o_orderkey) FROM orders) - {new_orders_count} +
            ROW_NUMBER() OVER (ORDER BY l_orderkey) AS l_orderkey,
        l_partkey,
        l_suppkey,
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        CURRENT_DATE AS l_shipdate,
        CURRENT_DATE AS l_commitdate,
        CURRENT_DATE AS l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment
    FROM lineitem
    WHERE l_orderkey <= {new_orders_count};
    """

    rf2_sql = f"""
    DELETE FROM lineitem
    WHERE l_orderkey IN (
        SELECT o_orderkey FROM orders
        ORDER BY o_orderdate, o_orderkey
        LIMIT {new_orders_count}
    );

    DELETE FROM orders
    WHERE o_orderkey IN (
        SELECT o_orderkey FROM orders
        ORDER BY o_orderdate, o_orderkey
        LIMIT {new_orders_count}
    );
    """

    return rf1_sql, rf2_sql


# =========================
# Queries do benchmark
# =========================


def load_queries(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return [p.strip() for p in sql.split(";") if p.strip()]


# =========================
# Índices / tuning
# =========================


def read_json_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)



def is_supported_suggestions_json(path: str) -> bool:
    try:
        data = read_json_file(path)
    except Exception:
        return False

    if isinstance(data, dict):
        return any(k in data for k in ("indexes", "materialized_views", "suggestions", "sql"))

    if isinstance(data, list):
        if not data:
            return True
        for item in data:
            if isinstance(item, str):
                continue
            if isinstance(item, dict) and "sql" in item and isinstance(item["sql"], str):
                continue
            return False
        return True

    return False



def load_index_sqls_from_json(path: Optional[str]) -> List[str]:
    if not path:
        return []

    if not os.path.exists(path):
        print(f"Arquivo de sugestões {path} não encontrado, ignorando.")
        return []

    data = read_json_file(path)
    sqls: List[str] = []

    # Formato novo:
    # {
    #   "indexes": [...],
    #   "materialized_views": [...]
    # }
    if isinstance(data, dict) and ("indexes" in data or "materialized_views" in data):
        for idx in data.get("indexes", []):
            if not isinstance(idx, dict):
                continue

            name = idx.get("name")
            table = idx.get("table")
            columns = idx.get("columns") or []
            where = idx.get("where")

            if not name or not table or not columns:
                continue

            cols_str = ", ".join(columns)
            stmt = f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols_str})"
            if where:
                stmt += f" WHERE {where}"
            stmt += ";"
            sqls.append(stmt)

        for mv in data.get("materialized_views", []):
            if not isinstance(mv, dict):
                continue
            def_sql = mv.get("definition_sql")
            if not def_sql:
                continue
            stmt = def_sql.strip()
            if not stmt.endswith(";"):
                stmt += ";"
            sqls.append(stmt)

        return sqls

    # Fallbacks antigos
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                sqls.append(item)
            elif isinstance(item, dict) and "sql" in item and isinstance(item["sql"], str):
                sqls.append(item["sql"])
        return sqls

    if isinstance(data, dict):
        if "suggestions" in data and isinstance(data["suggestions"], list):
            for item in data["suggestions"]:
                if isinstance(item, str):
                    sqls.append(item)
                elif isinstance(item, dict) and "sql" in item and isinstance(item["sql"], str):
                    sqls.append(item["sql"])
            return sqls

        if "sql" in data and isinstance(data["sql"], str):
            sqls.append(data["sql"])
            return sqls

    raise ValueError(
        f"Formato de JSON de sugestões não suportado em {path}. "
        "Adapte load_index_sqls_from_json se necessário."
    )



def apply_tuning_statements(conn, statements: List[str]):
    cur = conn.cursor()

    for stmt in statements:
        stmt_clean = stmt.strip().rstrip(";")
        if not stmt_clean:
            continue

        print("Aplicando tuning:", stmt_clean)
        try:
            cur.execute(stmt_clean)
        except Exception as e:
            print(f"Erro ao aplicar statement '{stmt_clean}': {e}")

    conn.commit()
    cur.close()


# =========================
# Descoberta dinâmica de cenários
# =========================


def scenario_name_from_path(path: str) -> str:
    return Path(path).stem



def iter_json_files_from_dir(directory: str) -> Iterable[str]:
    if not directory:
        return []
    p = Path(directory)
    if not p.exists():
        raise FileNotFoundError(f"Diretório de resultados não encontrado: {directory}")
    if not p.is_dir():
        raise NotADirectoryError(f"--resultados-dir deve apontar para uma pasta: {directory}")
    return sorted(str(x) for x in p.glob("*.json"))



def discover_tuning_sets(args) -> "OrderedDict[str, List[str]]":
    output_abs = os.path.abspath(args.output_json)

    discovered: List[Tuple[str, str]] = []

    # Compatibilidade com os 4 argumentos antigos
    legacy_args = [
        ("sem_prompt_sem_ontologia", args.resultado_sem_prompt_sem_ontologia),
        ("sem_prompt_com_ontologia", args.resultado_sem_prompt_com_ontologia),
        ("com_prompt_sem_ontologia", args.resultado_com_prompt_sem_ontologia),
        ("com_prompt_com_ontologia", args.resultado_com_prompt_com_ontologia),
    ]
    for scenario_name, path in legacy_args:
        if path:
            discovered.append((scenario_name, path))

    # Arquivos explícitos: --resultado-json a.json b.json c.json
    for path in args.resultado_json or []:
        discovered.append((scenario_name_from_path(path), path))

    # Diretório: --resultados-dir resultados_v2_sem_vm
    if args.resultados_dir:
        for path in iter_json_files_from_dir(args.resultados_dir):
            if os.path.abspath(path) == output_abs:
                continue
            if not is_supported_suggestions_json(path):
                print(f"Ignorando JSON que não parece arquivo de sugestões: {path}")
                continue
            discovered.append((scenario_name_from_path(path), path))

    # Deduplicação por nome de cenário; o último prevalece.
    ordered_paths: "OrderedDict[str, str]" = OrderedDict()
    for scenario_name, path in discovered:
        if not path:
            continue
        if os.path.abspath(path) == output_abs:
            continue
        ordered_paths[scenario_name] = path

    tuning_sets: "OrderedDict[str, List[str]]" = OrderedDict()

    if not args.no_baseline:
        tuning_sets[args.baseline_name] = []

    for scenario_name, path in ordered_paths.items():
        tuning_sets[scenario_name] = load_index_sqls_from_json(path)

    return tuning_sets


# =========================
# Métricas
# =========================


def measure_power(
    query_times: List[float],
    refresh_times: List[float],
    sf: float,
) -> Optional[float]:
    total_time = sum(query_times) + sum(refresh_times)
    if total_time <= 0:
        return None
    n_ops = len(query_times) + len(refresh_times)
    return 3600.0 * sf * n_ops / total_time



def execute_stream_once(conn_params: Dict[str, str], queries: List[str]):
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    for q in queries:
        q_clean = q.strip()
        if not q_clean:
            continue
        cur.execute(q_clean)
        try:
            cur.fetchall()
        except psycopg2.ProgrammingError:
            pass

    conn.commit()
    cur.close()
    conn.close()



def execute_streams_parallel(
    base_conn_params: Dict[str, str],
    queries: List[str],
    number_of_streams: int,
) -> float:
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=number_of_streams) as executor:
        futures = [
            executor.submit(execute_stream_once, base_conn_params, queries)
            for _ in range(number_of_streams)
        ]
        for f in futures:
            f.result()
    return time.time() - start_time



def get_index_size_mb(conn) -> float:
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COALESCE(SUM(pg_indexes_size(format('%I.%I', schemaname, tablename)::regclass)), 0)
        FROM pg_tables
        WHERE schemaname = 'public';
        """
    )
    index_bytes = cur.fetchone()[0] or 0

    cur.execute(
        """
        SELECT COALESCE(
            SUM(pg_total_relation_size(format('%I.%I', schemaname, matviewname)::regclass)),
            0
        )
        FROM pg_matviews
        WHERE schemaname = 'public';
        """
    )
    mv_bytes = cur.fetchone()[0] or 0

    cur.close()
    total_bytes = float(index_bytes) + float(mv_bytes)
    return total_bytes / (1024.0 * 1024.0)


# =========================
# Execução de cenário
# =========================


def run_benchmark_scenario(
    args,
    base_conn_params: Dict[str, str],
    tabelas_dir: str,
    queries: List[str],
    tuning_statements: List[str],
    scenario_name: str,
    sf: float,
    number_of_streams: int,
) -> Dict[str, Optional[float]]:
    print("=" * 80)
    print(f"Iniciando cenário: {scenario_name}")
    print("=" * 80)

    # recria tudo do zero => equivale ao "restore" entre cenários
    create_or_replace_database(args)

    conn = get_conn(base_conn_params)
    create_schema(conn)
    load_all_data(conn, tabelas_dir)

    rf1_sql, rf2_sql = build_refresh_sql(conn)

    if tuning_statements:
        apply_tuning_statements(conn, tuning_statements)

    # POWER
    cur = conn.cursor()

    print("Executando RF1 (power)...")
    start = time.time()
    cur.execute(rf1_sql)
    conn.commit()
    rf1_time = time.time() - start

    print("Executando queries (power)...")
    query_times: List[float] = []
    for i, q in enumerate(queries, start=1):
        q_clean = q.strip()
        if not q_clean:
            continue
        print(f"  Query {i}...")
        q_start = time.time()
        cur.execute(q_clean)
        try:
            cur.fetchall()
        except psycopg2.ProgrammingError:
            pass
        query_times.append(time.time() - q_start)

    print("Executando RF2 (power)...")
    start = time.time()
    cur.execute(rf2_sql)
    conn.commit()
    rf2_time = time.time() - start

    cur.close()
    conn.close()

    refresh_times = [rf1_time, rf2_time]
    power = measure_power(query_times, refresh_times, sf)
    print(f"Power ({scenario_name}): {power:.2f}" if power is not None else f"Power ({scenario_name}): None")

    # THROUGHPUT
    total_time = execute_streams_parallel(base_conn_params, queries, number_of_streams)
    if total_time > 0:
        throughput = (number_of_streams * len(queries)) / total_time * 3600.0 * sf
    else:
        throughput = None

    print(
        f"Throughput ({scenario_name}): {throughput:.2f}"
        if throughput is not None
        else f"Throughput ({scenario_name}): None"
    )

    # QphH
    if power is not None and throughput is not None:
        qphh = math.sqrt(power * throughput)
        print(f"QphH ({scenario_name}): {qphh:.2f}")
    else:
        qphh = None
        print(f"QphH ({scenario_name}): None")

    # Size
    conn_size = get_conn(base_conn_params)
    tuning_size_mb = get_index_size_mb(conn_size)
    conn_size.close()
    print(f"Tamanho total do tuning ({scenario_name}): {tuning_size_mb:.2f} MB")

    return {
        "scenario": scenario_name,
        "power": power,
        "throughput": throughput,
        "qphh": qphh,
        "tuning_size_mb": tuning_size_mb,
    }


# =========================
# main
# =========================


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark de tuning (TPC-H) em Postgres com múltiplos cenários."
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--dbname", default="tpch_tuning")

    parser.add_argument(
        "--tabelas-dir",
        default="tabelas",
        help="Diretório com os .tbl do TPC-H",
    )
    parser.add_argument(
        "--no-baseline",
        action="store_true",
        help="Não inclui o cenário baseline sem tuning",
    )
    parser.add_argument(
        "--queries-sql",
        required=True,
        help="Arquivo .sql com as queries do benchmark",
    )
    parser.add_argument(
        "--sf",
        type=float,
        default=SF_DEFAULT,
        help="Scale Factor (SF) do TPC-H",
    )
    parser.add_argument(
        "--streams",
        type=int,
        default=NUM_STREAMS_DEFAULT,
        help="Nº de streams para throughput",
    )

    # Compatibilidade com a versão antiga
    parser.add_argument(
        "--resultado-sem-prompt-sem-ontologia",
        default=None,
        help="JSON de sugestões da LLM sem prompt engineering e sem ontologia",
    )
    parser.add_argument(
        "--resultado-sem-prompt-com-ontologia",
        default=None,
        help="JSON de sugestões da LLM sem prompt engineering e com ontologia",
    )
    parser.add_argument(
        "--resultado-com-prompt-sem-ontologia",
        default=None,
        help="JSON de sugestões da LLM com prompt engineering e sem ontologia",
    )
    parser.add_argument(
        "--resultado-com-prompt-com-ontologia",
        default=None,
        help="JSON de sugestões da LLM com prompt engineering e com ontologia",
    )

    # Novo modo dinâmico
    parser.add_argument(
        "--resultados-dir",
        default=None,
        help="Pasta com vários JSONs de sugestões para comparar automaticamente",
    )
    parser.add_argument(
        "--resultado-json",
        nargs="*",
        default=None,
        help="Lista explícita de JSONs de sugestões para comparar",
    )
    parser.add_argument(
        "--baseline-name",
        default="baseline_sem_indices_extra",
        help="Nome do cenário baseline sem tuning",
    )

    parser.add_argument(
        "--output-json",
        default="resultados/resultados_benchmark_multiplos_cenarios.json",
        help="Arquivo final com as métricas dos cenários",
    )

    return parser.parse_args()



def main():
    args = parse_args()
    base_conn_params = make_conn_params(args)

    # garante pasta do output
    output_dir = os.path.dirname(args.output_json)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    queries = load_queries(args.queries_sql)
    print(f"{len(queries)} queries carregadas do arquivo {args.queries_sql}")

    tuning_sets = discover_tuning_sets(args)
    print("\nCenários encontrados:")
    for scenario_name, statements in tuning_sets.items():
        print(f"  - {scenario_name}: {len(statements)} statement(s)")

    resultados: List[Dict[str, Optional[float]]] = []

    for scenario_name, tuning_statements in tuning_sets.items():
        res = run_benchmark_scenario(
            args=args,
            base_conn_params=base_conn_params,
            tabelas_dir=args.tabelas_dir,
            queries=queries,
            tuning_statements=tuning_statements,
            scenario_name=scenario_name,
            sf=args.sf,
            number_of_streams=args.streams,
        )
        resultados.append(res)

    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)

    print("\nResultados salvos em", args.output_json)
    for r in resultados:
        if (
            r["power"] is not None
            and r["throughput"] is not None
            and r["qphh"] is not None
        ):
            print(
                f"[{r['scenario']}] "
                f"power={r['power']:.2f} "
                f"throughput={r['throughput']:.2f} "
                f"qphh={r['qphh']:.2f} "
                f"tuning_size_mb={r['tuning_size_mb']:.2f}"
            )
        else:
            print(f"[{r['scenario']}] (alguma métrica None) {r}")


if __name__ == "__main__":
    main()
