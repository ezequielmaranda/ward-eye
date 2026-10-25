# Correr el pipeline en una Raspberry Pi

Objetivo: que la Pi corra sola `dlt → dbt` todos los días a las 23:00 ART,
con Prefect y systemd, sin depender de la PC.

```
systemd
 ├─ prefect-server.service   API + UI + scheduler  (http://<ip-pi>:4200)
 └─ ward-eye.service         python -m flows.daily_pipeline serve
                             └─ a las 23:00 ART: dlt → dbt run → dbt test
```

Se usa `flow.serve()` y no work pool + worker porque hay un solo flow y una
sola máquina: el mismo proceso que registra el deployment es el que ejecuta
las corridas.

---

## 1. Requisitos

- Raspberry Pi de 64 bits (Pi 4 o 5; con 4 GB de RAM va holgada).
  Prefect server, dbt y DuckDB juntos piden unos cientos de MB.
- Raspberry Pi OS **64-bit** (Bookworm o posterior). En 32 bits no hay wheels
  de DuckDB ni de varias dependencias.
- Python 3.13: lo instala `uv` solo, no hace falta el del sistema.

## 2. Instalar

```bash
# uv (gestor de Python/venv del proyecto)
curl -LsSf https://astral.sh/uv/install.sh | sh
exec $SHELL

git clone https://github.com/ezequielmaranda/ward-eye.git ~/ward-eye
cd ~/ward-eye
uv sync                        # crea .venv con Python 3.13 y todo uv.lock
```

## 3. Configurar

```bash
cp .env.example .env           # completar RIOT_API_KEY, SUMMONER_*, región
```

`dbt/profiles.yml` está gitignored. Crealo con el contenido del README
(sección "Configurar dbt"). Usa `DUCKDB_PATH` o el default `../warddata.duckdb`,
así que no hace falta tocar rutas.

**Traer la base desde la PC** (recomendado). Si arrancás vacía, el bootstrap
solo baja las últimas 50 partidas y perdés el histórico, incluido el de los
snapshots de rango, que no se puede recuperar:

```bash
# desde la PC, con el pipeline y la web apagados (DuckDB es single-writer)
scp D:/ward-eye/warddata.duckdb <usuario>@<ip-pi>:~/ward-eye/
```

No hace falta copiar `~/.dlt/`: el estado incremental sale de la propia tabla
`lol_raw.raw_matches`.

**Probar una corrida a mano** antes de automatizar:

```bash
.venv/bin/python -m flows.daily_pipeline
```

## 4. Servicios systemd

Las units en `deploy/` traen dos placeholders: `__USER__` (tu usuario en la
Pi) y `__REPO__` (ruta absoluta al repo).

```bash
cd ~/ward-eye
for f in deploy/*.service; do
  sed -e "s|__USER__|$USER|g" -e "s|__REPO__|$PWD|g" "$f" \
    | sudo tee "/etc/systemd/system/$(basename "$f")" > /dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable --now prefect-server ward-eye
```

Verificar:

```bash
systemctl status prefect-server ward-eye
journalctl -u ward-eye -f                     # logs en vivo
```

La UI queda en `http://<ip-pi>:4200`. Ahí aparece el deployment
`ward-eye-daily/ward-eye-daily-23h` con la próxima corrida a las 23:00 ART.
Para disparar una corrida ya:

```bash
PREFECT_API_URL=http://127.0.0.1:4200/api \
  .venv/bin/prefect deployment run 'ward-eye-daily/ward-eye-daily-23h'
```

## 5. Actualizar el código

```bash
cd ~/ward-eye && git pull && uv sync
sudo systemctl restart ward-eye
```

Reiniciá siempre después de un `pull`. Si cambia el schedule o los
parámetros de `serve()`, el reinicio es obligatorio: el deployment se registra
al arrancar el proceso. Los modelos dbt se leen del disco en cada corrida,
porque dbt corre como subproceso.

## 6. Cosas a tener en cuenta

- **Huso horario.** El schedule (`Cron(..., timezone=TIMEZONE)`), el "hoy" del
  flow y el `game_date` de dbt (var `game_timezone`) están fijados en
  `America/Argentina/Buenos_Aires`. No importa que la Pi esté en UTC. Si
  alguna vez cambiás el huso, cambialo en los dos lugares:
  `flows/daily_pipeline.py` y `dbt/dbt_project.yml`.
- **DuckDB single-writer.** No dejes un cliente abierto sobre
  `warddata.duckdb` en la Pi a las 23:00. Para explorar, usá
  `duckdb -readonly`.
- **¿Y el dashboard?** La base ahora vive en la Pi. Hay dos opciones: correr
  también `web/` ahí (`npm run build && npm start`, con otra unit de systemd)
  o traerte el `.duckdb` a la PC cuando quieras mirarlo. Todavía no se decidió
  cuál.
- **Backups.** La tarjeta SD es el punto débil. Un `cp warddata.duckdb`
  periódico a otro disco (fuera de las 23:00) alcanza.
- **Si la UI abre desde otra PC pero no muestra datos**, revisá
  `PREFECT_UI_API_URL` en la config de Prefect (`prefect config view`): la UI
  tiene que poder llegar a la API en `http://<ip-pi>:4200/api`.
