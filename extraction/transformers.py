"""
Transforma las respuestas crudas de la Riot API en registros planos
listos para cargar con dlt.

La API de Riot devuelve estructuras muy anidadas; acá las aplanamos
en tablas relacionales con match_id y puuid como claves.
"""

import json
from typing import Generator


def parse_match(raw: dict, my_puuid: str) -> dict:
    """
    Extrae los campos del nivel superior de una partida.
    Retorna un único registro para la tabla raw_matches.
    """
    info = raw["info"]
    return {
        "match_id":        raw["metadata"]["matchId"],
        "game_version":    info["gameVersion"],
        "queue_id":        info["queueId"],
        "game_mode":       info["gameMode"],
        "game_type":       info["gameType"],
        "game_duration_s": info["gameDuration"],
        "game_start_ts":   info["gameStartTimestamp"],
        "game_end_ts":     info.get("gameEndTimestamp"),
        "platform_id":     info["platformId"],
        "my_puuid":        my_puuid,
    }


def parse_participants(raw: dict, my_puuid: str) -> Generator[dict, None, None]:
    """
    Aplana la lista de participantes de una partida.
    Cada registro va a la tabla raw_participants.
    """
    match_id = raw["metadata"]["matchId"]
    for p in raw["info"]["participants"]:
        # Challenges es un dict opcional con métricas avanzadas
        challenges = p.get("challenges", {})

        yield {
            "match_id":              match_id,
            "puuid":                 p["puuid"],
            "participant_id":        p["participantId"],
            "is_me":                 p["puuid"] == my_puuid,
            "summoner_name":         p.get("summonerName", ""),
            "riot_id_name":          p.get("riotIdGameName", ""),
            "riot_id_tag":           p.get("riotIdTagline", ""),
            "champion_id":           p["championId"],
            "champion_name":         p["championName"],
            "team_id":               p["teamId"],
            "team_position":         p.get("teamPosition", ""),
            "individual_position":   p.get("individualPosition", ""),
            "win":                   p["win"],
            # KDA
            "kills":                 p["kills"],
            "deaths":                p["deaths"],
            "assists":               p["assists"],
            # Farm
            "total_minions_killed":  p["totalMinionsKilled"],
            "neutral_minions_killed":p["neutralMinionsKilled"],
            # Vision
            "vision_score":          p["visionScore"],
            "wards_placed":          p["wardsPlaced"],
            "wards_killed":          p["wardsKilled"],
            "control_wards_bought":  p["challenges"].get("controlWardsPlaced", 0) if challenges else 0,
            "vision_wards_bought":   p.get("visionWardsBoughtInGame", 0),
            # Daño
            "total_damage_dealt_to_champions": p["totalDamageDealtToChampions"],
            "total_damage_taken":    p["totalDamageTaken"],
            "total_heal":            p["totalHeal"],
            "total_heals_on_teammates": p.get("totalHealsOnTeammates", 0),
            # Oro y nivel
            "gold_earned":           p["goldEarned"],
            "gold_spent":            p["goldSpent"],
            "champ_level":           p["champLevel"],
            # Objetivos
            "turret_kills":          p.get("turretKills", 0),
            "inhibitor_kills":       p.get("inhibitorKills", 0),
            "dragon_kills":          p.get("dragonKills", 0),
            "baron_kills":           p.get("baronKills", 0),
            "first_blood_kill":      p.get("firstBloodKill", False),
            "first_tower_kill":      p.get("firstTowerKill", False),
            # Items (guardados como JSON para flexibilidad)
            "items_json":            json.dumps([
                                         p.get(f"item{i}") for i in range(7)
                                         if p.get(f"item{i}", 0) != 0
                                     ]),
            # Summoner spells
            "summoner1_id":          p.get("summoner1Id"),
            "summoner2_id":          p.get("summoner2Id"),
            # Runes (primera runa principal como referencia)
            "primary_rune_id":       p.get("perks", {}).get("styles", [{}])[0]
                                         .get("selections", [{}])[0].get("perk"),
            # Challenges destacados
            "kill_participation":    challenges.get("killParticipation", 0),
            "kda":                   challenges.get("kda", 0),
            "damage_per_minute":     challenges.get("damagePerMinute", 0),
            "gold_per_minute":       challenges.get("goldPerMinute", 0),
            # OJO: NO es un ratio de CS por minuto. Es el conteo de súbditos de
            # LÍNEA en los primeros 10 min (para un jungla son 0-5, no ~6). Se
            # llamaba "cs_per_minute", lo que invitaba a leerlo como ratio y a
            # compararlo contra cs_per_min (que sí es un ratio, recalculado en
            # staging sobre la duración real de la partida).
            "lane_minions_first_10min": challenges.get("laneMinionsFirst10Minutes", 0),
            "solo_kills":            challenges.get("soloKills", 0),
            "multikills":            p.get("largestMultiKill", 0),
            "killing_sprees":        p.get("largestKillingSpree", 0),
            "time_ccing_others_s":   p.get("timeCCingOthers", 0),
            # El objeto challenges completo (~130 métricas, sin documentación
            # oficial y con claves que Riot agrega o saca entre versiones). Se
            # guarda entero y se tipa en dbt (stg_challenges): si mañana
            # importa un campo que hoy no extraemos, ya está en raw.
            "challenges_json":       json.dumps(challenges),
        }


def parse_timeline_frames(
    raw_timeline: dict, match_id: str
) -> Generator[dict, None, None]:
    """
    Aplana los frames del timeline (estado por minuto de cada jugador).
    Tabla: raw_timeline_frames
    """
    frames = raw_timeline.get("info", {}).get("frames", [])
    for frame in frames:
        ts_ms = frame["timestamp"]
        for pid_str, pf in frame.get("participantFrames", {}).items():
            yield {
                "match_id":        match_id,
                "participant_id":  int(pid_str),
                "timestamp_ms":    ts_ms,
                "timestamp_min":   round(ts_ms / 60000, 2),
                "total_gold":      pf.get("totalGold", 0),
                "current_gold":    pf.get("currentGold", 0),
                "xp":              pf.get("xp", 0),
                "level":           pf.get("level", 0),
                "minions_killed":  pf.get("minionsKilled", 0),
                "jungle_minions":  pf.get("jungleMinionsKilled", 0),
                "position_x":      pf.get("position", {}).get("x"),
                "position_y":      pf.get("position", {}).get("y"),
            }


def parse_timeline_events(
    raw_timeline: dict, match_id: str
) -> Generator[dict, None, None]:
    """
    Aplana los eventos del timeline (kills, objetivos, items, etc.).
    Tabla: raw_timeline_events
    """
    frames = raw_timeline.get("info", {}).get("frames", [])
    for frame in frames:
        for event in frame.get("events", []):
            etype = event.get("type", "")
            # Solo guardamos eventos relevantes para coaching
            if etype not in {
                "CHAMPION_KILL",
                "BUILDING_KILL",
                "ELITE_MONSTER_KILL",
                "ITEM_PURCHASED",
                "ITEM_SOLD",
                "ITEM_UNDO",
                "SKILL_LEVEL_UP",
                "LEVEL_UP",
                "WARD_PLACED",
                "WARD_KILL",
                "OBJECTIVE_BOUNTY_PRESTART",
            }:
                continue

            yield {
                "match_id":         match_id,
                "timestamp_ms":     event.get("timestamp"),
                "timestamp_min":    round(event.get("timestamp", 0) / 60000, 2),
                "event_type":       etype,
                "participant_id":   event.get("participantId") or event.get("killerId") or event.get("creatorId"),
                "killer_id":        event.get("killerId"),
                "victim_id":        event.get("victimId"),
                "assisting_ids":    json.dumps(event.get("assistingParticipantIds", [])),
                "item_id":          event.get("itemId"),
                "skill_slot":       event.get("skillSlot"),
                "level_up_type":    event.get("levelUpType"),
                "building_type":    event.get("buildingType"),
                "lane_type":        event.get("laneType"),
                "tower_type":       event.get("towerType"),
                "monster_type":     event.get("monsterType"),
                "monster_subtype":  event.get("monsterSubType"),
                "ward_type":        event.get("wardType"),
                "position_x":       event.get("position", {}).get("x") if event.get("position") else None,
                "position_y":       event.get("position", {}).get("y") if event.get("position") else None,
                "bounty":           event.get("bounty", 0),
                "shutdown_bounty":  event.get("shutdownBounty", 0),
            }


def parse_summoner(raw_summoner: dict, puuid: str, ranked_info: list) -> dict:
    """
    Combina datos de summoner + ranked en un único registro.
    Tabla: raw_summoners
    """
    # Buscamos la entrada de ranked solo/duo (queue 420)
    solo = next((r for r in ranked_info if r["queueType"] == "RANKED_SOLO_5x5"), {})
    flex = next((r for r in ranked_info if r["queueType"] == "RANKED_FLEX_SR"), {})

    return {
        "puuid":               puuid,
        "summoner_level":      raw_summoner.get("summonerLevel"),
        "profile_icon_id":     raw_summoner.get("profileIconId"),
        # Ranked solo
        "solo_tier":           solo.get("tier"),
        "solo_rank":           solo.get("rank"),
        "solo_lp":             solo.get("leaguePoints"),
        "solo_wins":           solo.get("wins"),
        "solo_losses":         solo.get("losses"),
        # Ranked flex
        "flex_tier":           flex.get("tier"),
        "flex_rank":           flex.get("rank"),
        "flex_lp":             flex.get("leaguePoints"),
        "flex_wins":           flex.get("wins"),
        "flex_losses":         flex.get("losses"),
    }
