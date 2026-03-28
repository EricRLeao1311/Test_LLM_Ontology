#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO


REPO_ROOT = Path(__file__).resolve().parent


class SilentArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def parse_args() -> argparse.Namespace:
    parser = SilentArgumentParser(
        description="Roda a comparação LLM x baseline do TPC-H de forma simples, uma vez por scale factor."
    )
    scale_group = parser.add_mutually_exclusive_group(required=True)
    scale_group.add_argument("--sf", type=float, help="Um único scale factor.")
    scale_group.add_argument(
        "--sfs",
        nargs="+",
        type=float,
        help="Vários scale factors, por exemplo: --sfs 0.01 0.02 0.05 0.1",
    )

    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--dbname", default="tpch_tuning")
    parser.add_argument(
        "--tabelas-base-dir",
        default="tabelas",
        help="Pasta raiz que contém subpastas por SF, por exemplo tabelas/0.01.",
    )
    parser.add_argument(
        "--queries-sql",
        default="tpch_queries.sql",
        help="Arquivo .sql com as queries do benchmark.",
    )
    parser.add_argument(
        "--streams",
        type=int,
        default=4,
        help="Número de streams para throughput.",
    )
    parser.add_argument(
        "--resultados-dir",
        default="resultados_v3_sem_vm",
        help="Pasta com os JSONs de sugestões da LLM.",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Arquivo de saída para um único SF. Exemplo: comparacao_llm_001.json",
    )
    parser.add_argument(
        "--output-dir",
        default="resultados_benchmark",
        help="Pasta de saída quando usar vários SFs.",
    )
    parser.add_argument(
        "--prefix",
        default="comparacao_llm",
        help="Prefixo dos arquivos quando usar vários SFs.",
    )
    parser.add_argument(
        "--before-each",
        default=None,
        help="Comando shell opcional executado antes de cada benchmark.",
    )
    return parser.parse_args()


def format_sf(sf: float) -> str:
    text = f"{sf}".rstrip("0").rstrip(".")
    return text or "0"


def compact_sf_token(sf: float) -> str:
    text = format_sf(sf)
    if text.startswith("0."):
        return text[2:]
    return text.replace(".", "_")


def build_output_path(args: argparse.Namespace, sf: float, multi_mode: bool) -> Path:
    if not multi_mode and args.output_json:
        return Path(args.output_json).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"{args.prefix}_{compact_sf_token(sf)}.json"


def build_log_path(output_path: Path) -> Path:
    return output_path.with_suffix(".log")


def write_log_line(log_handle: TextIO, message: str = "") -> None:
    log_handle.write(message + "\n")
    log_handle.flush()


def format_command(command: list[str]) -> str:
    return subprocess.list2cmdline(command)


def log_header(
    log_handle: TextIO,
    *,
    sf_text: str,
    output_path: Path,
    log_path: Path,
    before_each: str | None,
    tuning_command: list[str],
) -> None:
    write_log_line(log_handle, "=" * 80)
    write_log_line(log_handle, "RUN SIMPLE COMPARISON")
    write_log_line(log_handle, f"Timestamp: {datetime.now().isoformat()}")
    write_log_line(log_handle, f"SF atual: {sf_text}")
    write_log_line(log_handle, f"JSON de saída: {output_path}")
    write_log_line(log_handle, f"Log: {log_path}")
    write_log_line(
        log_handle,
        f"Comando de preparação: {before_each if before_each else '(não informado)'}",
    )
    write_log_line(log_handle, f"Comando tuning_analysis.py: {format_command(tuning_command)}")
    write_log_line(log_handle, "=" * 80)
    write_log_line(log_handle)


def run_logged_command(
    *,
    command: list[str] | str,
    shell: bool,
    log_handle: TextIO,
    cwd: Path,
    description: str,
) -> None:
    write_log_line(log_handle, f"[INÍCIO] {description}")
    completed = subprocess.run(
        command,
        shell=shell,
        check=False,
        cwd=cwd,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    write_log_line(log_handle, f"[FIM] {description} (exit_code={completed.returncode})")
    write_log_line(log_handle)
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, command)


def run_one(args: argparse.Namespace, sf: float, multi_mode: bool) -> None:
    sf_text = format_sf(sf)
    tabelas_dir = Path(args.tabelas_base_dir) / sf_text
    output_path = build_output_path(args, sf, multi_mode)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log_path = build_log_path(output_path)

    command = [
        sys.executable,
        str(REPO_ROOT / "tuning_analysis.py"),
        "--host",
        args.host,
        "--port",
        args.port,
        "--user",
        args.user,
        "--password",
        args.password,
        "--dbname",
        args.dbname,
        "--tabelas-dir",
        str(tabelas_dir),
        "--queries-sql",
        args.queries_sql,
        "--sf",
        sf_text,
        "--streams",
        str(args.streams),
        "--resultados-dir",
        args.resultados_dir,
        "--output-json",
        str(output_path),
    ]

    with open(log_path, "w", encoding="utf-8") as log_handle:
        log_header(
            log_handle,
            sf_text=sf_text,
            output_path=output_path,
            log_path=log_path,
            before_each=args.before_each,
            tuning_command=command,
        )

        if not tabelas_dir.is_dir():
            write_log_line(log_handle, f"[ERRO] Dataset não encontrado: {tabelas_dir}")
            raise NotADirectoryError(f"Dataset não encontrado: {tabelas_dir}")

        if args.before_each:
            write_log_line(log_handle, "[INFO] Executando preparação antes do benchmark.")
            run_logged_command(
                command=args.before_each,
                shell=True,
                log_handle=log_handle,
                cwd=REPO_ROOT,
                description=f"before-each para SF {sf_text}",
            )

        write_log_line(log_handle, "[INFO] Executando tuning_analysis.py.")
        run_logged_command(
            command=command,
            shell=False,
            log_handle=log_handle,
            cwd=REPO_ROOT,
            description=f"benchmark para SF {sf_text}",
        )
        write_log_line(log_handle, "[SUCESSO] Execução concluída.")


def main() -> int:
    try:
        args = parse_args()
        scales = [args.sf] if args.sf is not None else list(args.sfs or [])
        multi_mode = len(scales) > 1

        if any(sf <= 0 for sf in scales):
            raise ValueError("Todos os scale factors devem ser maiores que zero.")
        if multi_mode and args.output_json:
            raise ValueError("--output-json só pode ser usado com um único --sf.")

        for sf in scales:
            run_one(args, sf, multi_mode)
    except (subprocess.CalledProcessError, OSError, ValueError):
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
