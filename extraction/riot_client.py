"""
Cliente para la Riot Games API (Match-V5).
Maneja rate limiting, reintentos y los dos niveles de routing:
  - platform: para endpoints de summoner/league  (ej: la1.api.riotgames.com)
  - regional:  para Match-V5 y account           (ej: americas.api.riotgames.com)
"""

import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)


class RiotAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Riot API {status_code}: {message}")


class RiotClient:
    BASE_PLATFORM = "https://{region}.api.riotgames.com"
    BASE_REGIONAL = "https://{region}.api.riotgames.com"

    # Rate limits de personal key: 20 req/s, 100 req/2min
    # Usamos delays conservadores para no acercarnos al límite
    REQ_DELAY = 0.1        # 100ms entre requests (10 req/s, margen amplio)
    RETRY_AFTER = 120      # segundos de espera si recibimos 429

    def __init__(
        self,
        api_key: str,
        platform: str,
        routing: str,
        memoize: bool = True,
        min_interval: float | None = None,
    ):
        """
        api_key:      RGAPI-xxxx...
        platform:     la1 | la2 | na1 | euw1 | ...
        routing:      americas | europe | asia | sea
        memoize:      cachear partidas/timelines en memoria. Sí para el pipeline
                      diario (varios recursos leen la misma partida); NO para
                      el crawl, que baja miles y llenaría la RAM.
        min_interval: segundos mínimos entre requests. Por defecto REQ_DELAY
                      (ráfagas que chocan con el límite de 2 min y esperan el
                      429). Para corridas largas conviene 1.25: 100 req/2 min
                      sostenidas, sin 429.
        """
        self.api_key = api_key
        self.platform = platform
        self.routing = routing
        self.session = requests.Session()
        self.session.headers.update({"X-Riot-Token": self.api_key})
        self._last_request = 0.0
        self.min_interval = self.REQ_DELAY if min_interval is None else min_interval
        self.memoize = memoize
        self.requests_made = 0
        # Memo por instancia (vida = una corrida del pipeline). Evita re-descargar
        # la misma partida/timeline cuando varios recursos dlt la consumen.
        self._match_cache: dict[str, dict] = {}
        self._timeline_cache: dict[str, dict] = {}

    def _wait(self):
        """Respeta el intervalo mínimo entre requests."""
        elapsed = time.time() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def _get(self, url: str, params: dict = None, retries: int = 3) -> dict:
        """GET con reintentos automáticos ante 429 y 5xx."""
        for attempt in range(retries):
            self._wait()
            self._last_request = time.time()

            resp = self.session.get(url, params=params)
            self.requests_made += 1

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", self.RETRY_AFTER))
                logger.warning(f"Rate limited. Esperando {retry_after}s...")
                time.sleep(retry_after)
                continue

            if resp.status_code in (500, 502, 503, 504) and attempt < retries - 1:
                wait = 2 ** attempt
                logger.warning(f"Error {resp.status_code}, reintento en {wait}s...")
                time.sleep(wait)
                continue

            raise RiotAPIError(resp.status_code, resp.text)

        raise RiotAPIError(429, "Rate limit persistente después de reintentos")

    # -------------------------------------------------------------------------
    # Account / Summoner
    # -------------------------------------------------------------------------

    def get_puuid(self, summoner_name: str, tag: str) -> str:
        """Obtiene el PUUID global a partir de Riot ID (nombre#tag)."""
        url = (
            f"{self.BASE_REGIONAL.format(region=self.routing)}"
            f"/riot/account/v1/accounts/by-riot-id/{summoner_name}/{tag}"
        )
        data = self._get(url)
        return data["puuid"]

    def get_summoner_by_puuid(self, puuid: str) -> dict:
        """Datos de summoner (nivel, iconId, etc.)."""
        url = (
            f"{self.BASE_PLATFORM.format(region=self.platform)}"
            f"/lol/summoner/v4/summoners/by-puuid/{puuid}"
        )
        return self._get(url)

    def get_ranked_info(self, puuid: str) -> list[dict]:
        """Información de ranked (tier, rank, LP, winrate) por PUUID."""
        url = (
            f"{self.BASE_PLATFORM.format(region=self.platform)}"
            f"/lol/league/v4/entries/by-puuid/{puuid}"
        )
        return self._get(url)

    # -------------------------------------------------------------------------
    # Matches
    # -------------------------------------------------------------------------

    def get_match_ids(
        self,
        puuid: str,
        start_time: Optional[int] = None,
        count: int = 20,
        start: int = 0,
        queue: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list[str]:
        """
        Lista de match IDs recientes (de más nuevo a más viejo).
        start_time / end_time: epoch Unix en segundos (ventana de partidas)
        start: índice de inicio (para paginar; Riot devuelve máx 100 por página)
        queue: 420 = ranked solo, 400 = normal draft, None = todos
        """
        url = (
            f"{self.BASE_REGIONAL.format(region=self.routing)}"
            f"/lol/match/v5/matches/by-puuid/{puuid}/ids"
        )
        params = {"start": start, "count": count}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        if queue is not None:
            params["queue"] = queue

        return self._get(url, params=params)

    def get_match(self, match_id: str) -> dict:
        """Datos completos de una partida (info + metadata). Memoizado si memoize=True."""
        if match_id in self._match_cache:
            return self._match_cache[match_id]
        url = (
            f"{self.BASE_REGIONAL.format(region=self.routing)}"
            f"/lol/match/v5/matches/{match_id}"
        )
        data = self._get(url)
        if self.memoize:
            self._match_cache[match_id] = data
        return data

    def get_match_timeline(self, match_id: str) -> dict:
        """Timeline frame-a-frame de una partida. Memoizado si memoize=True."""
        if match_id in self._timeline_cache:
            return self._timeline_cache[match_id]
        url = (
            f"{self.BASE_REGIONAL.format(region=self.routing)}"
            f"/lol/match/v5/matches/{match_id}/timeline"
        )
        data = self._get(url)
        if self.memoize:
            self._timeline_cache[match_id] = data
        return data

    # -------------------------------------------------------------------------
    # League (jugadores por tier)
    # -------------------------------------------------------------------------

    APEX_ENDPOINTS = {
        "MASTER": "masterleagues",
        "GRANDMASTER": "grandmasterleagues",
        "CHALLENGER": "challengerleagues",
    }

    def get_league_entries(
        self, tier: str, division: str, page: int = 1, queue: str = "RANKED_SOLO_5x5"
    ) -> list[dict]:
        """Una página (hasta 205) de jugadores de un tier+división (IRON..DIAMOND)."""
        url = (
            f"{self.BASE_PLATFORM.format(region=self.platform)}"
            f"/lol/league/v4/entries/{queue}/{tier}/{division}"
        )
        return self._get(url, params={"page": page})

    def get_apex_league(self, tier: str, queue: str = "RANKED_SOLO_5x5") -> list[dict]:
        """Todos los jugadores de MASTER / GRANDMASTER / CHALLENGER (sin divisiones)."""
        url = (
            f"{self.BASE_PLATFORM.format(region=self.platform)}"
            f"/lol/league/v4/{self.APEX_ENDPOINTS[tier]}/by-queue/{queue}"
        )
        return self._get(url).get("entries", [])
