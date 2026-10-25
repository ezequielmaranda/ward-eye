# ward-eye — manual para volver al proyecto

Leé esto cuando vuelvas después de un tiempo. Diagramas en
[`diagrama.md`](diagrama.md); correrlo desatendido en la Pi en
[`RASPBERRY.md`](RASPBERRY.md). El detalle fino (por qué se decidió cada cosa)
está en [`../CLAUDE.md`](../CLAUDE.md) y [`../web/CLAUDE.md`](../web/CLAUDE.md).

---

## 1. Qué es, en 30 segundos

Pipeline personal que baja tus partidas de LoL de la Riot API, las guarda en un
archivo DuckDB local, las modela con dbt y las muestra en un dashboard Next.js
que además pide un informe de coaching a un LLM.

```
Riot API → extraction/ → dlt → warddata.duckdb ← dbt
                                     │
                                     └→ web/ (dashboard + coaching LLM)
Prefect corre dlt → dbt todos los días a las 23:00 ART.
```

Todo es local: no hay servidor de base de datos. El "warehouse" es el archivo
`warddata.duckdb` en la raíz.

---

## 2. Checklist de reenganche

1. **Riot API key**: las keys de desarrollo **vencen cada 24 h**. Si dlt falla
   con 401/403, renovala en developer.riotgames.com y actualizá `RIOT_API_KEY`
   en `.env`.
2. **Ponerte al día con datos**: `python -m flows.daily_pipeline` (dlt + dbt
   + tests). Si estuviste mucho tiempo afuera y jugaste más de 100 partidas,
   corrélo varias veces: cada corrida procesa como máximo 100, empezando por las
   más viejas, y no se pierde nada.
3. **Ver el dashboard**: `cd web && npm run dev` → http://localhost:3000.
4. **Estado del repo**: `git status`, `git log --oneline -10`, `gh pr list`.

> En esta máquina `dbt.exe` está bloqueado por Application Control. Invocá dbt
> siempre como módulo: `python -m dbt.cli.main ...` (el flow ya lo hace así).

---

## 3. Mapa del repo

| Carpeta / archivo | Qué hay |
|---|---|
| `extraction/riot_client.py` | Cliente HTTP de Riot. Rate limit, reintentos y memoización de `get_match` / `get_match_timeline`. |
| `extraction/transformers.py` | Parsers: JSON de la API → filas planas (`parse_match`, `parse_participants`, `parse_timeline_*`, `parse_summoner`). |
| `dlt_pipeline/pipeline.py` | El pipeline dlt: recursos, estado incremental y `run_pipeline()`. |
| `dlt_pipeline/backfill_participant_id.py` | Backfill histórico que se corrió una sola vez. No hace falta volver a correrlo. |
| `dbt/` | Proyecto dbt (`ward_eye`): `models/staging`, `models/intermediate`, `models/marts`, `tests/`, `macros/`. |
| `flows/daily_pipeline.py` | Flow de Prefect: dlt → dbt → partidas del día → placeholder de coaching. `serve` lo deja corriendo con el schedule. |
| `deploy/*.service` | Units de systemd para la Raspberry (Prefect server + `serve`). Ver [`RASPBERRY.md`](RASPBERRY.md). |
| `scripts/migrate_*.py` | Migraciones de datos que ya se aplicaron. Son idempotentes. |
| `web/` | Dashboard Next.js 16 + coaching. Tiene su propio `README.md` y `CLAUDE.md`. |
| `analysis/` | EDA de factores de victoria: `eda_factores_victoria.ipynb` y `data.py`, que arma los datasets. Las dependencias van en un grupo aparte: `uv sync --group analysis`. |
| `main.py` | Hello world del template de `uv`. No se usa. |

---

## 4. dlt: qué carga y cómo

dlt escribe **todo en el schema `lol_raw`** (`dataset_name="lol_raw"`,
`pipeline_name="lol_coach"`). Todos los recursos usan
`write_disposition="merge"`, así que re-correr es idempotente.

### Tablas que crea y carga

| Tabla | Grano | Clave de merge | Qué es |
|---|---|---|---|
| `raw_matches` | 1 fila por partida | `primary_key=match_id` | Metadata de la partida: cola, duración, `game_start_ts`, resultado. **Es también el cursor incremental.** |
| `raw_participants` | 10 filas por partida | `primary_key=(match_id, puuid)` | Stats finales de cada jugador. `is_me` marca tu fila. |
| `raw_timeline_frames` | participante × frame (~1/min) | `primary_key=(match_id, participant_id, timestamp_ms)` | Oro, XP, CS y nivel minuto a minuto. |
| `raw_timeline_events` | 1 fila por evento | **`merge_key=match_id`** | Kills, wards, objetivos, compras, etc. Usa `merge_key` porque un evento no tiene clave natural (`participant_id` es NULL en varios tipos de evento). Re-ingestar una partida borra y reinserta todos sus eventos. |
| `raw_summoners` | 1 fila (tu cuenta) | `primary_key=puuid` | Nivel y rango actual. Se refresca en **cada** corrida, aunque no haya partidas nuevas. Ningún modelo dbt lo lee. |
| `raw_player_rank_snapshots` | 10 filas por partida | `primary_key=(match_id, puuid)` | Rango SoloQ de los 10 jugadores en el momento de ingestar. Solo para partidas que terminaron hace ≤36 h (`RANK_SNAPSHOT_MAX_LAG_HOURS`). |

### Tablas internas (no tocar)

- `lol_raw._dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`: metadata de
  dlt (cargas, estado, schema guardado).
- Schema **`lol_raw_staging`**: tablas temporales que dlt usa para el
  delete-insert del merge. Es normal que exista y no lo lee nadie. Si lo
  borrás, dlt lo recrea en la próxima carga.

### Cómo decide qué bajar

1. Lee de DuckDB, en READ_ONLY, `max(game_start_ts)` y los `match_id` ya
   cargados. El estado incremental sale **de la tabla misma**, no del state
   de dlt.
2. Pagina `get_match_ids(startTime=cursor)` hasta agotar y hace el diff contra
   los IDs cargados.
3. Ordena de más vieja a más nueva y se queda con 100 como máximo
   (`MAX_MATCHES_PER_RUN`).
4. La primera vez (base vacía) baja las 50 más recientes
   (`BOOTSTRAP_MATCH_COUNT`).
5. Todos los recursos comparten esa misma lista de IDs. Como `get_match` y
   `get_match_timeline` están memoizados, cada partida se descarga una sola
   vez.

### Trampas conocidas

- **Los hints de dlt se fusionan, no se reemplazan.** Si sacás un
  `primary_key` del decorador, sigue vivo en el schema guardado (en
  `~/.dlt/pipelines/lol_coach/` y en `lol_raw._dlt_version`). Hay que limpiarlo
  a mano: ver `scripts/migrate_timeline_events_merge_key.py`.
- **dlt ignora el destino que le pasás** si el `pipeline_name` ya existe:
  restaura el destino guardado. No sirve para testear contra una copia de la
  base cambiando solo el path.
- Las partidas cargadas antes del fix de `merge_key` tienen ~4% menos de
  eventos. Se aceptó así, sin re-ingesta.

---

## 5. dbt: qué schemas y tablas maneja

Proyecto `ward_eye`, perfil en `dbt/profiles.yml` (gitignored) apuntando al
mismo `warddata.duckdb`. La macro `generate_schema_name` hace que el
`+schema` de cada carpeta se use **tal cual**, sin prefijo del target. Por
eso los schemas se llaman `lol_staging` y no `main_lol_staging`.

Es la arquitectura *medallion* con otros nombres: `lol_raw` = bronce,
`lol_staging` + `lol_intermediate` = plata, `lol_marts` = oro.

| Carpeta | Schema | Materialización |
|---|---|---|
| `models/staging` | `lol_staging` | **view** |
| `models/intermediate` | `lol_intermediate` | table |
| `models/marts` | `lol_marts` | table |

Fuente: `sources.yml` declara las 6 tablas de `lol_raw` (todas menos las
internas `_dlt_*`).

Var `game_timezone` (en `dbt_project.yml`): huso con el que se calculan
`game_date` y `week_start`. Fijado a ART para no depender del host; tiene
que coincidir con `TIMEZONE` en `flows/daily_pipeline.py`.

### Staging (`lol_staging`, vistas)

| Modelo | Qué hace |
|---|---|
| `stg_matches` | Timestamps, `game_date` en ART, duración en minutos, nombre de cola. **Descarta remakes (<5 min).** |
| `stg_participants` | Métricas por minuto (cs/min, vision/min, damage/min). Hereda el filtro de remakes vía `stg_matches`. |
| `stg_timeline_frames` | CS total y fase de juego por frame. |
| `stg_timeline_events` | Limpieza liviana, más **`is_real_ward`**, el filtro canónico de wards reales (`WARD_PLACED` incluye cajas de Shaco, hongos de Teemo, etc.). |
| `stg_player_rank_snapshots` | Corrige wins/losses a "antes de la partida". Tier, división y LP quedan post-partida. Filtrá por `snapshot_is_reliable`. |

### Intermediate (`lol_intermediate`, tablas)

| Modelo | Qué hace |
|---|---|
| `int_frame_snapshots` | Recursos de cada participante @5/@10/@15/@20/@25, tomando el frame **más cercano** al hito. Descarta los hitos que quedan a más de 1.5 min (la partida terminó antes). Existe porque los frames de Riot tienen drift (15.01, 20.01…). |

### Marts (`lol_marts`, tablas — esto es lo que lee la web)

| Modelo | Grano | Para qué |
|---|---|---|
| `mart_match_performance` | 1 fila por partida tuya | Tabla central: todas tus métricas más el contexto del equipo (shares). |
| `mart_champion_stats` | 1 fila por campeón | Promedios y winrate por campeón (solo ranked). |
| `mart_player_trends` | 1 fila por semana | Evolución semana a semana. |
| `mart_early_game` | 1 fila por partida | Laning: snapshots @10/@15 y eventos tempranos. |
| `mart_jungle_matchup` | 1 fila por partida de jungla | Diferenciales vs el jungla rival (campamentos y oro @10/@15, wards, muertes en duelo vs gank). |
| `mart_objective_control` | 1 fila por partida | Quién se llevó cada objetivo neutral (primer dragón, heraldo, barón, conteos y diferenciales). |
| `mart_match_rank_context` | 1 fila por partida con snapshot | Rango tuyo, del rival directo y promedio por equipo, `*_gap`, `lobby_difficulty` y heurística de smurf. |

`mart_jungle_matchup`, `mart_objective_control` y `mart_match_rank_context`
son los únicos marts **relativos** (miran a los otros 9 jugadores). El resto
solo tiene tu `puuid`.

### Tests (`dbt/tests/`)

| Test | Qué protege |
|---|---|
| `assert_frame_snapshots_grain` | Que `int_frame_snapshots` no se duplique (un duplicado contaría doble cualquier diferencial). |
| `assert_frame_snapshots_within_tolerance` | Que ningún snapshot quede a más de 1.5 min de su hito. |
| `assert_marts_reconcile_with_raw` | Que raw → marts solo pierda remakes y no partidas por un join roto. |
| `assert_ward_counts_are_plausible` | Que `is_real_ward` siga coincidiendo con `wards_placed` de Match-V5 (tolerancia 3). |

Además están los tests genéricos de los `schema.yml` (unique, not_null, etc.).

**Regla de oro:** en un mart nuevo no reimplementes el filtro de wards ni el
lookup de frames. Usá `stg_timeline_events.is_real_ward` e
`int_frame_snapshots`.

---

## 6. Web (`web/`)

- **Datos**: `lib/db.ts` abre DuckDB en READ_ONLY **por request** y lo cierra.
  `lib/queries.ts` lee los 7 marts. Todo server-side (`server-only`).
- **Gráficos**: ECharts con un wrapper propio (`components/EChart.tsx`). Regla
  de estilo: nunca doble eje y.
- **Coaching**: el selector de proveedor manda `POST /api/coaching
  {provider}` → `lib/coach.ts`, que arma el prompt (victorias vs derrotas,
  early game, duelo de jungla, rango, objetivos, tendencia, partidas recientes,
  campeones) y llama al endpoint OpenAI-compatible de Groq o Gemini. Devuelve
  Markdown y se puede bajar en PDF (`lib/reportPdf.ts`).
- **Config**: `web/.env.local` con `LLM_GROQ_*` / `LLM_GEMINI_*` (ver
  `web/.env.example`).
- **Next.js 16** tiene breaking changes: antes de tocar el frontend, leé la
  guía relevante en `web/node_modules/next/dist/docs/`.

---

## 7. Reglas de DuckDB que te van a morder

- **Un solo escritor a la vez.** dlt y dbt corren en serie. Si la web lee
  justo mientras el pipeline escribe, la lectura falla y la UI degrada sin
  romperse.
- **No dejes un cliente abierto sobre el archivo** (DBeaver, la CLI de
  `duckdb`, un notebook) mientras corre el pipeline, porque toma el lock. Para
  explorar: `duckdb -readonly warddata.duckdb`.
- Todos (dlt, dbt, web) apuntan al mismo archivo vía `DUCKDB_PATH` (opcional;
  el default es la raíz del repo).

---

## 8. Comandos

```bash
# Pipeline completo
python -m flows.daily_pipeline

# Solo ingesta
python -m dlt_pipeline.pipeline

# Solo dbt
cd dbt
python -m dbt.cli.main run  --profiles-dir .
python -m dbt.cli.main test --profiles-dir .
python -m dbt.cli.main run  --profiles-dir . --select mart_jungle_matchup+   # un modelo y sus hijos

# Dejarlo corriendo con el schedule 23:00 ART (necesita `prefect server start`)
python -m flows.daily_pipeline serve

# Web
cd web && npm install && npm run dev
```

### Variables de entorno (`.env` en la raíz)

```
RIOT_API_KEY=RGAPI-...    # vence cada 24 h (dev key)
RIOT_REGION=la2           # platform: summoner / league
RIOT_ROUTING=americas     # routing: account / match-v5
SUMMONER_NAME=...
SUMMONER_TAG=...
DUCKDB_PATH=...           # opcional
```

`RIOT_REGION` y `RIOT_ROUTING` son dos niveles distintos de la API, no son
redundantes.

---

## 9. Limitaciones y deuda conocida

- `generate_coaching_report()` en el flow **es un placeholder**. El coaching
  real solo corre desde la web.
- El rango de los 10 jugadores **no tiene histórico** previo a la feature de
  snapshots, y no se puede recuperar. La cobertura crece a medida que jugás.
  Los agregados por dificultad del lobby recién aparecen en el prompt con ≥10
  partidas con snapshot.
- Partidas viejas: ~4% de eventos faltantes (muertes y objetivos levemente
  subestimados).
- `lane_minions_first_10min` es un **conteo** (0-5 para jungla), no un ratio.
  Antes se llamaba `cs_per_minute`.
- **La dev key de Riot vence cada 24 h**: para correr desatendido (Pi) hace
  falta una Personal API Key. Ver [`RASPBERRY.md`](RASPBERRY.md).
- `mart_player_trends.top_champion` es no determinístico cuando hay empate de
  partidas entre campeones en la semana: puede cambiar entre corridas de dbt.
