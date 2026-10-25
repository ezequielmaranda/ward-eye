# ward-eye — diagramas

GitHub y VS Code (con la extensión de Mermaid) renderizan estos bloques directo.

## 1. Arquitectura general

```mermaid
flowchart LR
    riot["Riot Games API<br/>Account · Summoner · League · Match-V5"]

    subgraph py["Python (raíz del repo)"]
        ext["extraction/<br/>riot_client.py + transformers.py"]
        dlt["dlt_pipeline/pipeline.py<br/>carga incremental, merge"]
        dbt["dbt/<br/>staging → intermediate → marts"]
        prefect["flows/daily_pipeline.py<br/>Prefect 3 · 23:00 ART"]
    end

    db[("warddata.duckdb<br/>archivo único<br/>single-writer")]

    subgraph web["web/ (Next.js 16)"]
        dash["Dashboard<br/>ECharts"]
        api["POST /api/coaching<br/>lib/coach.ts"]
    end

    llm["LLM OpenAI-compatible<br/>Groq · Gemini"]

    riot --> ext --> dlt
    dlt -- "escribe lol_raw.*" --> db
    dbt -- "lee lol_raw, escribe<br/>lol_staging / intermediate / marts" --> db
    prefect -. "1. corre" .-> dlt
    prefect -. "2. corre (run + test)" .-> dbt
    db -- "READ_ONLY por request<br/>solo lol_marts.*" --> dash
    db -- "READ_ONLY" --> api
    api --> llm
```

## 2. Linaje de datos (dlt → dbt → web)

```mermaid
flowchart LR
    subgraph raw["lol_raw — dlt"]
        r_m[raw_matches]
        r_p[raw_participants]
        r_tf[raw_timeline_frames]
        r_te[raw_timeline_events]
        r_s[raw_summoners]
        r_rs[raw_player_rank_snapshots]
    end

    subgraph stg["lol_staging — vistas"]
        s_m[stg_matches]
        s_p[stg_participants]
        s_tf[stg_timeline_frames]
        s_te[stg_timeline_events]
        s_rs[stg_player_rank_snapshots]
    end

    subgraph int["lol_intermediate — tablas"]
        i_fs[int_frame_snapshots]
    end

    subgraph marts["lol_marts — tablas"]
        m_mp[mart_match_performance]
        m_cs[mart_champion_stats]
        m_pt[mart_player_trends]
        m_eg[mart_early_game]
        m_jm[mart_jungle_matchup]
        m_oc[mart_objective_control]
        m_rc[mart_match_rank_context]
    end

    webapp(["web/ dashboard + coaching"])

    r_m --> s_m
    r_p --> s_p
    s_m --> s_p
    r_tf --> s_tf
    r_te --> s_te
    r_rs --> s_rs
    s_m --> s_rs
    s_p --> s_rs

    s_tf --> i_fs

    s_m --> m_mp
    s_p --> m_mp
    m_mp --> m_cs
    m_mp --> m_pt
    s_p --> m_eg
    i_fs --> m_eg
    s_te --> m_eg
    s_p --> m_jm
    i_fs --> m_jm
    s_te --> m_jm
    s_p --> m_oc
    s_te --> m_oc
    s_rs --> m_rc
    s_p --> m_rc

    marts --> webapp
```

> `raw_summoners` no lo consume ningún modelo de dbt: dlt lo refresca en cada
> corrida y queda como dato de referencia.

## 3. Una corrida diaria, paso a paso

```mermaid
sequenceDiagram
    autonumber
    participant P as Prefect flow
    participant D as dlt
    participant DB as warddata.duckdb
    participant R as Riot API
    participant T as dbt

    P->>D: run_dlt_pipeline()
    D->>DB: READ_ONLY: max(game_start_ts) + match_id cargados
    DB-->>D: cursor + set de IDs
    D->>R: get_match_ids(startTime=cursor) paginado
    R-->>D: IDs → diff contra los cargados (máx 100)
    loop por partida nueva (memoizado)
        D->>R: get_match + get_match_timeline
        D->>R: get_ranked_info de los 10 (si terminó hace ≤36h)
    end
    D->>DB: merge en lol_raw.*
    P->>T: dbt run + dbt test (subprocess vía python)
    T->>DB: reconstruye staging / intermediate / marts
    P->>DB: READ_ONLY: partidas de hoy (mart_match_performance)
    Note over P: generate_coaching_report() es un placeholder:<br/>el coaching real se pide desde la web
```
