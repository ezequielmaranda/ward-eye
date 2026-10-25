# ward-eye — diagramas

Cada diagrama vive en su propio archivo `.mmd` en [`diagramas/`](diagramas/).
Para verlo o editarlo en mermaid.ai / mermaid.live, copiá el archivo entero y
pegalo tal cual.

| Diagrama | Archivo | Qué muestra |
|---|---|---|
| Arquitectura general | [`arquitectura.mmd`](diagramas/arquitectura.mmd) | Riot → dlt → DuckDB ← dbt → web → LLM, con Prefect orquestando. |
| Linaje de datos | [`linaje.mmd`](diagramas/linaje.mmd) | Tabla por tabla: `lol_raw` → `lol_staging` → `lol_intermediate` → `lol_marts` → web. |
| Una corrida diaria | [`corrida-diaria.mmd`](diagramas/corrida-diaria.mmd) | Secuencia del flow de Prefect: estado incremental, descarga, merge, dbt. |

> En el linaje, `raw_summoners` no lo consume ningún modelo de dbt: dlt lo
> refresca en cada corrida y queda como dato de referencia.

Si cambia un modelo, una tabla o un paso del flow, actualizá el `.mmd`
correspondiente.
