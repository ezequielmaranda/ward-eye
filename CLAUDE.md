# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ward-eye — LoL coaching pipeline

Pipeline de datos personal para análisis de partidas de League of Legends con coaching automático vía LLM, expuesto en un dashboard web.

## Stack
- **Extracción**: Python + Riot Games API (Match-V5) — `extraction/`
- **Ingesta**: dlt con `write_disposition=merge`, carga incremental a DuckDB — `dlt_pipeline/`
- **Storage**: DuckDB — archivo local único `warddata.duckdb` (sin servidor; evita el sleep del free tier de Supabase)
- **Transformación**: dbt Core (staging → marts) — `dbt/`
- **Orquestación**: Prefect 3, schedule diario 23:00 ART — `flows/`
- **Dashboard + coaching**: Next.js 16 + React 19 + ECharts; coaching on-demand vía endpoint LLM OpenAI-compatible (SDK `openai`) — `web/`

## Estado actual
- [x] Punto 1: extracción + dlt pipeline
- [x] Punto 2: modelos dbt
- [x] Punto 3: Prefect flow
- [x] Punto 4: coaching vía LLM — **implementado como dashboard web en `web/`**, NO como `coaching/claude_coach.py`. Ver "Coaching (web)" más abajo.

> **OJO**: el placeholder `generate_coaching_report()` en `flows/daily_pipeline.py` sigue existiendo y NO genera coaching real. El coaching corre desde el frontend (`web/`), independiente del flow diario de Prefect.

## Comandos

```bash
# Pipeline completo (dlt → dbt; el coaching del flow es solo un placeholder)
python -m flows.daily_pipeline

# Solo ingesta dlt
python -m dlt_pipeline.pipeline

# dbt — OJO: en esta máquina dbt.exe está bloqueado por Application Control,
# invocar vía python (el flow de Prefect ya lo hace internamente)
cd dbt && python -m dbt.cli.main run --profiles-dir . && python -m dbt.cli.main test --profiles-dir .

# Schedule 23:00 ART: proceso long-running (flow.serve), necesita `prefect server start`
python -m flows.daily_pipeline serve      # en la Raspberry lo levanta systemd: docs/RASPBERRY.md

# Dashboard web (coaching real)
cd web && npm install && npm run dev   # http://localhost:3000
```

## Variables de entorno

### Pipeline — `.env` (raíz)

```
RIOT_API_KEY=RGAPI-...
RIOT_REGION=la1          # la1=LAN, la2=LAS, na1=NA, euw1=EUW
RIOT_ROUTING=americas    # americas | europe | asia | sea
SUMMONER_NAME=...
SUMMONER_TAG=...
DUCKDB_PATH=...          # opcional; default <raíz>/warddata.duckdb (usá ruta absoluta para overridear)
```

### Coaching — `web/.env.local`

El LLM se configura 100% por env, con un set de vars por proveedor (base URL
y API key difieren entre proveedores, no solo el modelo). El dashboard elige
entre ellos con un selector — ver "Coaching (web)" más abajo.

```
LLM_GROQ_BASE_URL=...           # default https://api.groq.com/openai/v1
LLM_GROQ_API_KEY=...
LLM_GROQ_MODEL=...              # default openai/gpt-oss-120b
LLM_GROQ_REASONING_EFFORT=...   # opcional: low | medium | high

LLM_GEMINI_BASE_URL=...         # default https://generativelanguage.googleapis.com/v1beta/openai/
LLM_GEMINI_API_KEY=...          # key de Google AI Studio
LLM_GEMINI_MODEL=...            # default gemini-3.5-flash
LLM_GEMINI_REASONING_EFFORT=... # opcional: low | medium | high

DUCKDB_PATH=...          # opcional; default ../warddata.duckdb (relativo a web/)
```

## Arquitectura clave

### Riot API — dos niveles de routing
`RiotClient` usa dos bases de URL separadas:
- **platform** (`RIOT_REGION`, ej: `la1`): endpoints de summoner y league
- **routing** (`RIOT_ROUTING`, ej: `americas`): Match-V5 y Account endpoints

### dlt — estrategia incremental
Cursor manual con **única fuente de verdad = la propia tabla DuckDB** (`read_incremental_state()` en `dlt_pipeline/pipeline.py`): antes de correr se lee `max(game_start_ts)` y los `match_id` ya cargados de `lol_raw.raw_matches` (READ_ONLY, se cierra antes de que dlt escriba). Con eso se calcula **una sola lista de partidas nuevas** que comparten **todos** los recursos (`matches`, `participants`, `timeline_frames`, `timeline_events`) → ninguno re-procesa partidas viejas.

- `fetch_new_match_ids()` **pagina** `get_match_ids` con `startTime` hasta agotar (no se pierden partidas aunque se jueguen >100 entre corridas) y las ordena de más vieja a más nueva.
- El diff contra los `match_id` ya cargados descarta la partida del borde (que `startTime`, inclusivo en segundos, re-trae) y cualquier solape.
- `MAX_MATCHES_PER_RUN=100` acota memoria/API por corrida; un backlog mayor se drena (más viejas primero) en corridas siguientes, sin pérdida.
- `RiotClient.get_match` / `get_match_timeline` están **memoizados por instancia**: cada partida y cada timeline se descargan una sola vez aunque los consuman varios recursos (antes había doble fetch).
- Primera corrida (bootstrap): trae las `BOOTSTRAP_MATCH_COUNT=50` más recientes. Día sin partidas: `new_ids` vacío → solo se refresca `raw_summoners`.
- Idempotencia garantizada por `write_disposition=merge` en cada recurso. `raw_matches`, `raw_participants`, `raw_timeline_frames` y `raw_summoners` usan `primary_key` (tienen clave natural y sin NULLs). **`raw_timeline_events` usa `merge_key="match_id"`, no `primary_key`** — ver abajo.

### `raw_timeline_events`: por qué `merge_key` y no `primary_key`

Un evento del timeline **no tiene clave natural**. `participant_id` es NULL en
`CHAMPION_KILL`, `BUILDING_KILL` y `OBJECTIVE_BOUNTY_PRESTART`, y tenerlo dentro
de una `primary_key` rompía el merge de dlt por las dos puntas:

- **Dentro de una carga**: el delete-insert deduplica con `partition by`, que
  agrupa los NULL como iguales → se comía eventos simultáneos genuinamente
  distintos (dos ítems diferentes comprados en el mismo milisegundo).
- **Entre cargas**: borra con `=`, y `participant_id = NULL` nunca matchea → las
  filas viejas sobrevivían y los duplicados se acumulaban.

Con `merge_key="match_id"` el grano es la partida: re-ingestar una borra sus
eventos y los reinserta enteros, sin dedup por fila.

> **Si alguna vez tocás hints de dlt**: se **fusionan, no se reemplazan**. Sacar
> `primary_key` del decorador no lo saca del schema guardado (ni del local en
> `~/.dlt/pipelines/lol_coach/schemas/`, ni del blob en `lol_raw._dlt_version`),
> y el delete-insert lo sigue usando. Hay que limpiarlo explícitamente — lo hace
> `scripts/migrate_timeline_events_merge_key.py`.

**Limitación conocida**: las partidas cargadas antes de este arreglo arrastran
eventos que el dedup viejo borró en su carga original (~4% estimado, sobre todo
`CHAMPION_KILL` e `ITEM_PURCHASED`). No se puede reconstruir desde la base: hay
que re-ingestar el timeline desde la API. Se decidió no hacerlo. Consecuencia
práctica: los conteos de muertes y objetivos de partidas viejas están levemente
subestimados, y `LA2_1583646824` (re-ingestada al validar el arreglo) es la
única con datos completos. Si algún día se re-ingesta todo, esos números suben.

### Flujo en `flows/daily_pipeline.py`
```
run_dlt_pipeline()
    └─► run_dbt()                  (wait_for dlt)
            └─► get_todays_match_ids()   (wait_for dbt — DuckDB es single-writer)
                    └─► generate_coaching_report()   (placeholder — no genera coaching)
```
`get_todays_match_ids()` consulta `lol_marts.mart_match_performance` filtrando por `game_date = today` (hoy en ART). **Importante**: espera a `run_dbt` (no solo a `dlt`) porque DuckDB no permite leer el archivo mientras dbt lo está escribiendo. `generate_coaching_report()` solo loguea los match_ids del día; el coaching real vive en `web/` (ver más abajo).

### Huso horario: fijado a ART, no al host
El schedule (`Cron(..., timezone=TIMEZONE)`), el "hoy" del flow y `game_date` /
`week_start` en dbt (var `game_timezone` en `dbt_project.yml`) usan
`America/Argentina/Buenos_Aires` explícito. Antes `game_date` heredaba el
TimeZone de la sesión DuckDB (= el del host) y el flow usaba la fecha UTC, que a
las 23:00 ART ya es mañana → nunca encontraba partidas del día. En una Raspberry
en UTC eso además corría los días. Si se cambia el huso, cambiarlo en los dos
lugares.

### DuckDB — concurrencia (single-writer)
Un archivo `.duckdb` admite **un solo proceso escritor** a la vez; varios lectores solo conviven entre sí (no con un escritor). Implicancias:
- El pipeline (`dlt` → `dbt`) corre en serie y es el único que escribe.
- La web (`web/lib/db.ts`) abre el archivo en `READ_ONLY` **por request** y lo cierra enseguida, para no bloquear al pipeline diario. Si el pipeline está escribiendo justo en ese instante, la lectura falla y la UI degrada con elegancia.
- dbt y dlt comparten el mismo archivo vía `DUCKDB_PATH` (el flow pasa la ruta absoluta al subprocess de dbt).

### Schemas en DuckDB (archivo único `warddata.duckdb`)
- `lol_raw.*` — tablas raw (dlt): `raw_matches`, `raw_participants`, `raw_timeline_frames`, `raw_timeline_events`, `raw_summoners`, `raw_player_rank_snapshots`
- `lol_staging.*` — vistas (dbt staging): limpian y calculan métricas derivadas (cs/min, vision/min, damage/min)
- `lol_intermediate.*` — tablas (dbt intermediate): `int_frame_snapshots`
- `lol_marts.*` — tablas materializadas (dbt marts): `mart_match_performance`, `mart_champion_stats`, `mart_early_game`, `mart_player_trends`, `mart_jungle_matchup`

### Dos trampas del timeline de Riot (ya resueltas en staging/intermediate)

Ambas producían métricas silenciosamente incorrectas. **No reimplementes estos
filtros en un mart nuevo — usá los modelos que ya los encapsulan.**

- **`WARD_PLACED` no significa "ward"**. Incluye trampas y objetos de campeón
  (cajas de Shaco, hongos de Teemo, trampas de Jhin/Nidalee) bajo `ward_type`
  `UNDEFINED` / `TEEMO_MUSHROOM` — ~64% de los eventos, con partidas de 930
  "wards". El filtro canónico es `stg_timeline_events.is_real_ward`, validado
  contra `raw_participants.wards_placed` (fuente independiente, Match-V5):
  1166/1170 participantes coinciden exacto.
- **Los frames no caen en minutos enteros**. Riot los emite cada ~60s con drift
  acumulado (5.00, 13.01, 14.01, 15.01, 20.01…). La igualdad exacta
  (`timestamp_min = 15`) pierde ~2/3 de las partidas; un rango con `max()`
  (`between 14 and 16`) se queda con el frame *más tardío*, no el más cercano.
  Los snapshots @5/@10/@15/@20/@25 salen de `int_frame_snapshots`, que resuelve
  el frame más cercano una sola vez y descarta hitos a más de 1.5 min (partidas
  que terminaron antes).

### Los dos marts relativos

El resto de la capa marts sólo tiene al jugador (1 `puuid`), así que ninguna
métrica *relativa* se puede calcular ahí — y en jungla casi todo lo que importa
es relativo. Estos dos son la excepción:

- `mart_jungle_matchup` — una fila por partida de jungla con el jungla enemigo
  como contraparte: diferenciales de campamentos y oro @10/@15, wards vs wards,
  y muertes desagregadas entre duelo (`deaths_solo`, sin asistencias) y gank.
- `mart_objective_control` — una fila por partida con los objetivos neutrales
  atribuidos a cada bando (primer dragón / heraldo / barón, conteos y
  diferenciales de dragones y grubs). La atribución sale de unir `killer_id` del
  evento `ELITE_MONSTER_KILL` contra el `team_id` del participante.

Ambos alimentan el dashboard y el prompt de coaching (ver "Coaching (web)").

### Rango de los 10 jugadores (`raw_player_rank_snapshots`)

La API solo da el rango **actual**, así que dlt lo snapshotea al ingestar cada
partida nueva (`player_rank_snapshots_resource`) y saltea las que terminaron
hace más de `RANK_SNAPSHOT_MAX_LAG_HOURS=36`: el rango de hoy no describe una
partida vieja. **Consecuencia: el histórico previo no existe ni se puede
completar**; la cobertura crece a medida que se juega.

- `stg_player_rank_snapshots` corrige wins/losses a "antes de la partida",
  restando esa partida y las SoloQ posteriores que comparten snapshot (dlt pide
  un snapshot por jugador por corrida). Tier/división/LP **no** se pueden
  retroceder: quedan post-partida. Filtrá por `snapshot_is_reliable`.
- `mart_match_rank_context` — una fila por partida con snapshot: rango propio,
  del rival directo (misma posición) y promedio de cada equipo como
  `rank_score` lineal (400/tier, 100/división, + LP), sus diferencias
  (`*_gap` positivo = el rival era más alto), `lobby_difficulty` (±150) y una
  heurística gruesa de smurf (`games_before < 60` y winrate ≥ 65%).

### Crawl por tier (`lol_crawl`)

`dlt_pipeline/crawl.py` muestrea jugadores al azar por tier/división
(League-V4) y baja sus últimas 10 ranked de los 30 días previos al muestreo,
para comparar elos. Pipeline dlt **aparte** (`pipeline_name="lol_crawl"`,
dataset `lol_crawl`): nunca reusar `lol_coach` para esto, que restaura su
propio destino y escribiría en `lol_raw`. Estado retomable desde las propias
tablas (`crawl_seeds.listed_at`, `crawl_seed_matches` vs `crawl_matches`).
Usa `RiotClient(memoize=False, min_interval=1.25)`: sin caché (son miles de
partidas) y a ritmo sostenido de 100 req/2 min. Timeline recortado: frames
solo en los hitos 5/10/15/20/25 y eventos de kills/estructuras/objetivos/
ítems. El tier vale para la semilla, al momento del muestreo; los otros 9 de
cada partida son aproximación.

### `challenges`: blob en raw, tipado en staging

`raw_participants.challenges_json` guarda el objeto `challenges` de Match-V5
**entero** (~120-135 claves, sin documentación oficial y cambian entre
versiones). `stg_challenges` extrae y tipa las 72 útiles con la macro
`challenge(key, type)`; el resto (modos SWARM/ARAM, duplicados de
`stg_participants`, contadores de equipo) queda en raw por si hace falta.
NULL = la clave no vino ("no aplica": campos de jungla en un laner), no cero.
Los conteos absolutos dependen de campeón y rivales: normalizar antes de
comparar jugadores. Un campo nuevo en el parser se rellena hacia atrás con
`python -m dlt_pipeline.backfill_participants` (re-baja el detalle, ~1 request
por partida, merge idempotente).

### `lane_minions_first_10min` no es un ratio

Se llamaba `cs_per_minute` en raw, lo que invitaba a leerla como un ratio y a
compararla contra `cs_per_min` (ese sí lo es). Viene de
`challenges.laneMinionsFirst10Minutes`: **conteo** de súbditos de línea en los
primeros 10 min, que para un jungla son 0-5. El BIGINT que infirió dlt era el
tipo correcto, no un truncamiento. El rename está aplicado end-to-end
(`extraction/transformers.py` → `dlt_pipeline/pipeline.py` → staging → mart);
`scripts/migrate_lane_minions_rename.py` migró el histórico y es idempotente.

### Coaching (web)
El coaching se implementó como **dashboard Next.js en `web/`** (no como `coaching/claude_coach.py`, que nunca se creó). Detalles en `web/README.md` y `web/CLAUDE.md`. Puntos clave:

- **Datos**: server-side, lee los marts (`lol_marts.*`) del archivo DuckDB en `READ_ONLY` por request (`web/lib/db.ts`, `web/lib/queries.ts`). El acceso nunca llega al cliente (`server-only`).
- **Gráficos**: Apache ECharts vía wrapper propio `web/components/EChart.tsx` (sin `echarts-for-react` por peer-deps con React 19).
- **Coaching on-demand**: selector de proveedor + botón → `POST /api/coaching` con `{ provider: "groq" | "gemini" }` → `web/lib/coach.ts`. Usa el SDK `openai` contra el endpoint **OpenAI-compatible** del proveedor elegido, configurable por env (`LLM_GROQ_*` / `LLM_GEMINI_*`, ver más arriba). Default Groq free tier. Agregar un tercer proveedor (ej. Claude, que también expone endpoint OpenAI-compatible) = sumar un caso en `getProviderConfig()` + su entrada en `lib/providers.ts`, o reemplazar `lib/coach.ts` por el SDK nativo del proveedor.
- El prompt arma bloques: comparación Victorias vs Derrotas, early game, **duelo vs el jungla rival**, **contexto de rango** (por partida; los agregados por dificultad recién con ≥10 partidas con snapshot), **control de objetivos**, tendencia semanal, partidas recientes y campeones; calibrado al rol principal del jugador. El system prompt le indica priorizar el bloque de duelo por sobre las métricas absolutas, y distinguir muertes en duelo de muertes en gank (se corrigen distinto). Devuelve Markdown con secciones fijas (Resumen / Puntos fuertes / Áreas de mejora / Objetivos próxima sesión).
- Gráficos nuevos: `JungleMatchupChart` (barras diverging del diferencial de campamentos @15, color por signo) y `ObjectivesChart` (barras horizontales, winrate según quién se llevó cada objetivo). Ninguno usa doble eje: dos medidas de escala distinta van a dos gráficos o al tooltip, nunca a un segundo eje y.

> `web/` corre Next.js 16 con su propia copia de docs en `node_modules/next/dist/docs/` (ver `web/AGENTS.md`): tiene breaking changes respecto a versiones previas, leé la guía relevante antes de tocar código del frontend.
