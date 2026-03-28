#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME:-tpch_tuning}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgresql}"

echo "=============================="
echo "RESETANDO AMBIENTE TPC-H"
echo "=============================="

echo "[1] Reiniciando PostgreSQL..."
sudo systemctl restart "$POSTGRES_SERVICE"
sleep 2

echo "[2] Recriando banco..."
sudo -u postgres dropdb --if-exists "$DB_NAME"
sudo -u postgres createdb "$DB_NAME"

echo "[3] OK - Ambiente pronto"
