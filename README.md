# ward-eye

Pipeline de datos personal para **análisis de partidas de League of Legends** con
**coaching automático vía LLM**. Extrae tus partidas de la Riot API, las modela
con dbt y las expone en un dashboard web que, además de graficar tus métricas,
genera un informe de coaching on-demand.

Todo corre **100% local**: el storage es un único archivo DuckDB
(`warddata.duckdb`), sin servidor ni free tier que se duerma.

## Arquitectura

```
Riot API (Match-V5)
   │  extraction/        cliente HTTP + parsers
   ▼
 dlt  ───────────────►  DuckDB  ◄─────────── dbt
   dlt_pipeline/       warddata.duckdb        dbt/
   carga incremental   (archivo único)        staging → marts
   (write=merge)             │
                             ├──────────────► Prefect flow (flows/)
                             │                schedule diario 23:00 ART
                             │
                             └──────────────► Web dashboard (web/)
                                              Next.js + ECharts (READ_ONLY)
                                                     │
                                                     └─► coaching on-demand
                                                         LLM OpenAI-compatible
```

| Capa | Herramienta | Carpeta |
|---|---|---|
| Extracción | Python + Riot Games API (Match-V5) | `extraction/` |
| Ingesta | dlt (`write_disposition=merge`, incremental) | `dlt_pipeline/` |
| Storage | DuckDB — archivo local único | `warddata.duckdb` |
| Transformación | dbt Core (staging → marts) | `dbt/` |
| Orquestación | Prefect 3 (schedule diario) | `flows/` |
| Dashboard + coaching | Next.js 16 + React 19 + ECharts + SDK `openai` | `web/` |

> **Nota:** el coaching se implementó como un **dashboard web** (`web/`), no como
> un módulo Python. El placeholder `generate_coaching_report()` en
> `flows/daily_pipeline.py` sigue siendo eso, un placeholder; el coaching real
> vive en el frontend y es independiente del flow diario.

## Estructura del proyecto

```
ward-eye/
├── extraction/            # Cliente Riot API + parsers (API → registros planos)
│   ├── riot_client.py
│   └── transformers.py
├── dlt_pipeline/
│   └── pipeline.py        # Pipeline dlt, carga incremental a DuckDB
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml       # (gitignored — ver Setup)
│   └── models/
│       ├── staging/       # vistas: limpian raw y calculan métricas derivadas
│       └── marts/         # tablas materializadas (mart_*)
├── flows/
│   └── daily_pipeline.py  # Prefect flow: dlt → dbt → coaching
├── web/                   # Dashboard Next.js (ver web/README.md)
├── pyproject.toml         # deps Python (uv)
├── requirements.txt       # deps Python (pip)
└── warddata.duckdb        # archivo de datos (generado, no versionado)
```

## Setup

### Prerrequisitos

- Python 3.13
- Node.js 20+ (solo para el dashboard `web/`)
- Una [Riot API Key](https://developer.riotgames.com/) (la personal sirve)

### 1. Dependencias Python

Con [uv](https://docs.astral.sh/uv/) (recomendado):

```bash
uv sync
```

O con pip:

```bash
pip install -r requirements.txt
```

### 2. Variables de entorno

```bash
cp .env.example .env
# Editá .env con tus credenciales
```

| Variable | Descripción | Ejemplo |
|---|---|---|
| `RIOT_API_KEY` | Personal API Key de Riot | `RGAPI-xxxx...` |
| `RIOT_REGION` | Platform de tu servidor | `la1` (LAN), `la2` (LAS), `na1`, `euw1` |
| `RIOT_ROUTING` | Regional routing para Match-V5 | `americas`, `europe`, `asia`, `sea` |
| `SUMMONER_NAME` | Tu nombre de invocador (sin el `#tag`) | `MiNombre` |
| `SUMMONER_TAG` | Tu tag de Riot ID | `LAN` |
| `DUCKDB_PATH` | (Opcional) Ruta del archivo DuckDB | default `<raíz>/warddata.duckdb` |

**Combinaciones región / routing:**

| Servidor | `RIOT_REGION` | `RIOT_ROUTING` |
|---|---|---|
| LAN | `la1` | `americas` |
| LAS | `la2` | `americas` |
| NA  | `na1` | `americas` |
| EUW | `euw1` | `europe` |

### 3. Configurar dbt

`dbt/profiles.yml` está gitignored (contiene rutas locales). Crealo con:

```yaml
# dbt/profiles.yml
ward_eye:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH', '../warddata.duckdb') }}"
      schema: lol_staging
      threads: 4
```

## Uso

### Bootstrap (primera ejecución)

Carga las últimas 50 partidas en el schema `lol_raw`:

```bash
python -m dlt_pipeline.pipeline
```

dlt crea las tablas automáticamente. En ejecuciones siguientes solo trae partidas
nuevas (cursor incremental sobre `game_start_ts`, guardado en `.dlt/`).

### Transformar con dbt

```bash
cd dbt
python -m dbt.cli.main run  --profiles-dir .
python -m dbt.cli.main test --profiles-dir .
```

> Se invoca dbt como módulo de Python en vez del `dbt.exe` del venv porque en la
> máquina de desarrollo una política de Application Control bloquea los `.exe`
> wrapper del venv. Si en tu entorno `dbt` funciona normalmente, podés usar
> `dbt run` / `dbt test` directamente.

### Pipeline completo (orquestado)

Corre dlt → dbt → coaching en orden, con reintentos y logging de Prefect:

```bash
python -m flows.daily_pipeline
```

Para dejarlo corriendo con el schedule diario (23:00 ART), en un proceso
long-running que necesita un `prefect server start` aparte:

```bash
python -m flows.daily_pipeline serve
```

Setup completo como servicio (Raspberry Pi + systemd): [`docs/RASPBERRY.md`](docs/RASPBERRY.md).

### Dashboard web

```bash
cd web
cp .env.example .env.local   # completá LLM_API_KEY (Groq gratis u otro proveedor)
npm install
npm run dev                  # http://localhost:3000
```

El dashboard muestra KPIs, tendencias semanales, KDA por partida, impacto
(daño % vs kill participation), winrate por campeón y early game; y genera un
informe de coaching on-demand. Detalles en [`web/README.md`](web/README.md).

## Modelo de datos (DuckDB, archivo `warddata.duckdb`)

Tres schemas conviven en el mismo archivo:

### `lol_raw.*` — raw de dlt
```
raw_matches            -- 1 fila por partida
raw_participants       -- 10 filas por partida (1 jugador por fila)
raw_timeline_frames    -- ~200 filas por partida (estado por minuto)
raw_timeline_events    -- variable (kills, items, objetivos, etc.)
raw_summoners          -- 1 fila por jugador (estado ranked actual)
```

### `lol_staging.*` — vistas de dbt
Limpian las tablas raw y calculan métricas derivadas (cs/min, vision/min,
damage/min, etc.).

### `lol_marts.*` — tablas materializadas de dbt
```
mart_match_performance  -- rendimiento por partida
mart_champion_stats     -- agregados históricos por campeón
mart_early_game         -- métricas de los primeros ~15 minutos
mart_player_trends      -- tendencia semanal (ranked)
```

### DuckDB es single-writer

Un archivo `.duckdb` admite **un solo proceso escritor** a la vez; los lectores
solo conviven entre sí, no con un escritor. Por eso:

- El pipeline (`dlt` → `dbt`) corre en serie y es el **único** que escribe.
- El flow espera a que dbt termine antes de leer (`get_todays_match_ids` con
  `wait_for=[dbt_result]`).
- La web abre el archivo en `READ_ONLY` **por request** y lo cierra enseguida.
  Si el pipeline está escribiendo justo en ese instante, la lectura falla y la UI
  degrada con elegancia.

## Coaching (LLM)

El coaching corre desde el dashboard contra un endpoint **OpenAI-compatible**,
configurable 100% por env (`web/.env.local`) para no acoplar el proveedor:

| Variable | Descripción |
|---|---|
| `LLM_BASE_URL` | Endpoint OpenAI-compatible (default Groq) |
| `LLM_API_KEY` | API key del proveedor |
| `LLM_MODEL` | Modelo a usar |
| `LLM_REASONING_EFFORT` | (Opcional) `low` / `medium` / `high` para modelos de razonamiento |

Por defecto usa el **free tier de Groq** para no gastar tokens en pruebas.
Swappear a Claude/OpenAI = cambiar esas variables (ver
[`web/README.md`](web/README.md#swappear-a-claude)).

## Estado

- [x] Extracción + pipeline dlt
- [x] Modelos dbt (staging + marts)
- [x] Prefect flow con schedule diario
- [x] Coaching vía LLM (dashboard web)
