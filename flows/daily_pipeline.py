"""
ward-eye — Prefect flow principal
Corre una vez por día, orquesta:
  1. dlt  → extrae partidas nuevas de Riot API y las carga en DuckDB (archivo local)
  2. dbt  → transforma raw → staging → marts
  3. Partidas del día → placeholder de coaching (el coaching real vive en web/)

Schedule: 23:00 ART todos los días.
  python -m flows.daily_pipeline         # una corrida inmediata
  python -m flows.daily_pipeline serve   # proceso long-running con el schedule
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.schedules import Cron

load_dotenv(override=True)

# Rutas relativas al directorio raíz del proyecto (ward-eye/)
ROOT_DIR  = Path(__file__).parent.parent
DBT_DIR   = ROOT_DIR / "dbt"
DBT_PROFILES_DIR = DBT_DIR   # profiles.yml está dentro de dbt/

# Archivo DuckDB local compartido por dlt, dbt y la web (override con DUCKDB_PATH).
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(ROOT_DIR / "warddata.duckdb"))

# Huso del schedule y de "hoy". Explícito para no depender del reloj del host:
# una Raspberry suele venir en UTC, y a las 23:00 ART en UTC ya es mañana.
# Tiene que coincidir con la var `game_timezone` de dbt (dbt_project.yml).
TIMEZONE = "America/Argentina/Buenos_Aires"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="dlt-ingest", retries=2, retry_delay_seconds=60)
def run_dlt_pipeline() -> dict:
    """
    Ejecuta el pipeline dlt.
    Importa y corre run_pipeline() directamente (sin subprocess)
    para compartir el mismo proceso y aprovechar el logging de Prefect.
    """
    logger = get_run_logger()
    logger.info("Iniciando ingesta dlt desde Riot API...")

    import sys
    sys.path.insert(0, str(ROOT_DIR))
    from dlt_pipeline.pipeline import run_pipeline

    load_info = run_pipeline()

    # Contamos paquetes cargados para el resumen
    packages = getattr(load_info, "load_packages", [])
    logger.info(f"dlt completado — {len(packages)} paquete(s) cargado(s)")
    return {"packages_loaded": len(packages), "load_info": str(load_info)}


@task(name="dbt-run", retries=1, retry_delay_seconds=30)
def run_dbt() -> None:
    """
    Ejecuta dbt run + dbt test.
    Usa subprocess para respetar el entorno virtual de dbt y
    capturar su stdout/stderr en los logs de Prefect.
    """
    logger = get_run_logger()

    # Ruta absoluta del archivo DuckDB: dbt corre con cwd=dbt/, así apunta al
    # mismo archivo que dlt y la web sin depender del default relativo.
    env = {
        **os.environ,
        "DUCKDB_PATH": DUCKDB_PATH,
    }

    def _run(cmd: list[str], step: str) -> None:
        logger.info(f"dbt {step}...")
        result = subprocess.run(
            cmd,
            cwd=str(DBT_DIR),
            env=env,
            capture_output=True,
            text=True,
        )
        # Volcamos stdout/stderr a los logs de Prefect
        if result.stdout:
            for line in result.stdout.splitlines():
                logger.info(f"[dbt] {line}")
        if result.stderr:
            for line in result.stderr.splitlines():
                logger.warning(f"[dbt stderr] {line}")

        if result.returncode != 0:
            raise RuntimeError(f"dbt {step} falló con código {result.returncode}")

        logger.info(f"dbt {step} exitoso")

    # Invocamos dbt como módulo de Python (no el dbt.exe del venv): en esta
    # máquina una Application Control policy bloquea los .exe wrapper del venv
    # (WinError 4551). Usamos `python -c` (en vez de `-m`) para evitar el
    # RuntimeWarning de doble import de runpy en cada corrida.
    dbt_entry = (
        "import sys; from dbt.cli.main import cli; "
        "sys.argv = ['dbt', *sys.argv[1:]]; cli()"
    )

    def _dbt(*args: str) -> list[str]:
        return [sys.executable, "-c", dbt_entry, *args]

    _run(_dbt("run", "--profiles-dir", str(DBT_PROFILES_DIR)), "run")
    _run(_dbt("test", "--profiles-dir", str(DBT_PROFILES_DIR)), "test")


@task(name="fetch-todays-matches")
def get_todays_match_ids() -> list[str]:
    """
    Consulta los match_ids jugados hoy en el archivo DuckDB local (solo lectura).
    Se usa para decidir si hay partidas nuevas y para pasarlas al coaching.
    """
    import duckdb

    logger = get_run_logger()
    today = datetime.now(ZoneInfo(TIMEZONE)).date()

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT match_id
            FROM lol_marts.mart_match_performance
            WHERE game_date = ?
            ORDER BY game_start_at
            """,
            [today],
        ).fetchall()
    finally:
        conn.close()

    match_ids = [r[0] for r in rows]
    logger.info(f"Partidas jugadas hoy ({today}): {len(match_ids)}")
    return match_ids


@task(name="generate-coaching-report", retries=1, retry_delay_seconds=30)
def generate_coaching_report(match_ids: list[str]) -> str | None:
    """
    Placeholder para el coaching report via Claude API.
    Se implementa en el punto 4 del proyecto.
    Por ahora loguea las partidas del día y retorna None.
    """
    logger = get_run_logger()

    if not match_ids:
        logger.info("Sin partidas hoy — no se genera coaching report")
        return None

    logger.info(
        f"[PENDIENTE] Generar coaching report para {len(match_ids)} partida(s): "
        f"{', '.join(match_ids)}"
    )
    # TODO: punto 4 — integración Claude API
    return None


# ---------------------------------------------------------------------------
# Flow principal
# ---------------------------------------------------------------------------

@flow(
    name="ward-eye-daily",
    description="Pipeline diario: Riot API → DuckDB → dbt → coaching",
    log_prints=True,
)
def daily_pipeline():
    logger = get_run_logger()
    logger.info(f"=== ward-eye daily pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    # 1. Ingesta
    dlt_result = run_dlt_pipeline()

    # 2. Transformaciones (depende de ingesta exitosa)
    dbt_result = run_dbt(wait_for=[dlt_result])

    # 3. Partidas del día — espera a dbt: DuckDB es de un solo escritor, no se
    #    puede leer el archivo mientras dbt lo está escribiendo.
    match_ids = get_todays_match_ids(wait_for=[dbt_result])

    # 4. Coaching report (punto 4)
    generate_coaching_report(match_ids)

    logger.info("=== Pipeline completado ===")


# ---------------------------------------------------------------------------
# Deployment con schedule
# ---------------------------------------------------------------------------

def serve():
    """
    Registra el deployment con el schedule diario y queda corriendo: el propio
    proceso dispara las corridas, sin work pool ni worker aparte. Necesita una
    API de Prefect (PREFECT_API_URL → `prefect server start` o Prefect Cloud).
    En la Raspberry lo levanta systemd (ver deploy/ y docs/RASPBERRY.md):
        python -m flows.daily_pipeline serve
    """
    daily_pipeline.serve(
        name="ward-eye-daily-23h",
        schedule=Cron("0 23 * * *", timezone=TIMEZONE),
    )


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        serve()
    else:
        # Ejecución manual inmediata
        daily_pipeline()
