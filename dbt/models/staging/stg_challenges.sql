-- stg_challenges.sql
-- Las métricas de `challenges` de Match-V5, tipadas. Una fila por jugador por
-- partida (los 10), mismo grano que stg_participants.
--
-- raw guarda el objeto entero (challenges_json, ~120-135 claves según la
-- partida). Acá se extraen las 72 útiles para análisis. Quedan afuera:
--   - las de otros modos (SWARM_*, ARAM: poroExplosions, snowballsHit…),
--   - las que ya están en stg_participants (kda, killParticipation,
--     damagePerMinute, soloKills, laneMinionsFirst10Minutes…),
--   - las de equipo repetidas en los 5 jugadores (teamBaronKills,
--     firstTurretKilled…), que se calculan mejor desde el timeline,
--   - logros raros que casi siempre valen 0 (perfectGame, mejais…).
-- Si hace falta otra, está en raw: agregarla acá con la macro challenge().
--
-- Riot no documenta estas métricas: el significado sale del nombre. NULL
-- significa que la clave no vino, que para varias es "no aplica" (los campos
-- de jungla en un laner, el rival de línea en un remake), no cero.
-- Los conteos absolutos dependen del contexto (campeón, rivales, duración):
-- antes de comparar jugadores, normalizar por minuto y por campeón.

with participants as (
    select p.match_id, p.puuid, p.participant_id, p.challenges_json::json as c
    from {{ source('lol_raw', 'raw_participants') }} p
    -- hereda el filtro de remakes, igual que stg_participants
    inner join {{ ref('stg_matches') }} m using (match_id)
)

select
    match_id,
    puuid,
    participant_id,

    -- Mecánica individual: ejecución, reacción, supervivencia
    {{ challenge('abilityUses', 'integer') }}                            as ability_uses,
    {{ challenge('skillshotsHit', 'integer') }}                          as skillshots_hit,
    {{ challenge('skillshotsDodged', 'integer') }}                       as skillshots_dodged,
    {{ challenge('dodgeSkillShotsSmallWindow', 'integer') }}             as dodge_skill_shots_small_window,
    {{ challenge('landSkillShotsEarlyGame', 'integer') }}                as land_skill_shots_early_game,
    {{ challenge('enemyChampionImmobilizations', 'integer') }}           as enemy_champion_immobilizations,
    {{ challenge('immobilizeAndKillWithAlly', 'integer') }}              as immobilize_and_kill_with_ally,
    {{ challenge('survivedSingleDigitHpCount', 'integer') }}             as survived_single_digit_hp_count,
    {{ challenge('survivedThreeImmobilizesInFight', 'integer') }}        as survived_three_immobilizes_in_fight,
    {{ challenge('tookLargeDamageSurvived', 'integer') }}                as took_large_damage_survived,
    {{ challenge('outnumberedKills', 'integer') }}                       as outnumbered_kills,
    {{ challenge('quickSoloKills', 'integer') }}                         as quick_solo_kills,
    {{ challenge('multiKillOneSpell', 'integer') }}                      as multi_kill_one_spell,
    {{ challenge('multikillsAfterAggressiveFlash', 'integer') }}         as multikills_after_aggressive_flash,
    {{ challenge('killedChampTookFullTeamDamageSurvived', 'integer') }}  as killed_champ_took_full_team_damage_survived,
    {{ challenge('saveAllyFromDeath', 'integer') }}                      as save_ally_from_death,
    {{ challenge('effectiveHealAndShielding', 'double') }}               as effective_heal_and_shielding,
    {{ challenge('knockEnemyIntoTeamAndKill', 'integer') }}              as knock_enemy_into_team_and_kill,
    {{ challenge('quickCleanse', 'integer') }}                           as quick_cleanse,
    {{ challenge('deathsByEnemyChamps', 'integer') }}                    as deaths_by_enemy_champs,
    {{ challenge('damageTakenOnTeamPercentage', 'double') }}             as damage_taken_on_team_percentage,

    -- Duelo contra el rival directo (NULL si no hay rival de posición)
    {{ challenge('maxCsAdvantageOnLaneOpponent', 'double') }}            as max_cs_advantage_on_lane_opponent,
    {{ challenge('maxLevelLeadLaneOpponent', 'integer') }}               as max_level_lead_lane_opponent,
    {{ challenge('laningPhaseGoldExpAdvantage', 'integer') }}            as laning_phase_gold_exp_advantage,
    {{ challenge('earlyLaningPhaseGoldExpAdvantage', 'integer') }}       as early_laning_phase_gold_exp_advantage,
    {{ challenge('visionScoreAdvantageLaneOpponent', 'double') }}        as vision_score_advantage_lane_opponent,
    {{ challenge('turretPlatesTaken', 'integer') }}                      as turret_plates_taken,
    {{ challenge('takedownsAfterGainingLevelAdvantage', 'integer') }}    as takedowns_after_gaining_level_advantage,
    {{ challenge('kTurretsDestroyedBeforePlatesFall', 'integer') }}      as turrets_destroyed_before_plates_fall,

    -- Jungla: farmeo, rutas, ganks y pelea por objetivos. Los *_as_jungler / jungler_* son NULL para los laners
    {{ challenge('jungleCsBefore10Minutes', 'double') }}                 as jungle_cs_before_10min,
    {{ challenge('alliedJungleMonsterKills', 'integer') }}               as allied_jungle_monster_kills,
    {{ challenge('enemyJungleMonsterKills', 'integer') }}                as enemy_jungle_monster_kills,
    {{ challenge('moreEnemyJungleThanOpponent', 'double') }}             as more_enemy_jungle_than_opponent,
    {{ challenge('initialBuffCount', 'integer') }}                       as initial_buff_count,
    {{ challenge('initialCrabCount', 'integer') }}                       as initial_crab_count,
    {{ challenge('scuttleCrabKills', 'integer') }}                       as scuttle_crab_kills,
    {{ challenge('buffsStolen', 'integer') }}                            as buffs_stolen,
    {{ challenge('killsOnLanersEarlyJungleAsJungler', 'integer') }}      as kills_on_laners_early_jungle_as_jungler,
    {{ challenge('junglerKillsEarlyJungle', 'integer') }}                as jungler_kills_early_jungle,
    {{ challenge('takedownsBeforeJungleMinionSpawn', 'integer') }}       as takedowns_before_jungle_minion_spawn,
    {{ challenge('epicMonsterSteals', 'integer') }}                      as epic_monster_steals,
    {{ challenge('epicMonsterStolenWithoutSmite', 'integer') }}          as epic_monster_stolen_without_smite,
    {{ challenge('epicMonsterKillsWithin30SecondsOfSpawn', 'integer') }} as epic_monster_kills_within_30s_of_spawn,
    {{ challenge('epicMonsterKillsNearEnemyJungler', 'integer') }}       as epic_monster_kills_near_enemy_jungler,
    {{ challenge('junglerTakedownsNearDamagedEpicMonster', 'integer') }} as jungler_takedowns_near_damaged_epic_monster,
    {{ challenge('voidMonsterKill', 'integer') }}                        as void_monster_kill,
    {{ challenge('dragonTakedowns', 'integer') }}                        as dragon_takedowns,
    {{ challenge('riftHeraldTakedowns', 'integer') }}                    as rift_herald_takedowns,
    {{ challenge('baronTakedowns', 'integer') }}                         as baron_takedowns,
    {{ challenge('earliestDragonTakedown', 'double') }}                  as earliest_dragon_takedown_s,         -- segundos
    {{ challenge('soloBaronKills', 'integer') }}                         as solo_baron_kills,

    -- Macro y mapa. Los *_as_laner son NULL para los junglas
    {{ challenge('takedownsFirstXMinutes', 'integer') }}                 as takedowns_first_x_minutes,
    {{ challenge('acesBefore15Minutes', 'integer') }}                    as aces_before_15min,
    {{ challenge('killsNearEnemyTurret', 'integer') }}                   as kills_near_enemy_turret,
    {{ challenge('killsUnderOwnTurret', 'integer') }}                    as kills_under_own_turret,
    {{ challenge('pickKillWithAlly', 'integer') }}                       as pick_kill_with_ally,
    {{ challenge('killAfterHiddenWithAlly', 'integer') }}                as kill_after_hidden_with_ally,
    {{ challenge('teleportTakedowns', 'integer') }}                      as teleport_takedowns,
    {{ challenge('getTakedownsInAllLanesEarlyJungleAsLaner', 'integer') }} as get_takedowns_in_all_lanes_early_jungle_as_laner,
    {{ challenge('killsOnOtherLanesEarlyJungleAsLaner', 'integer') }}    as kills_on_other_lanes_early_jungle_as_laner,
    {{ challenge('unseenRecalls', 'integer') }}                          as unseen_recalls,
    {{ challenge('soloTurretsLategame', 'integer') }}                    as solo_turrets_lategame,
    {{ challenge('turretTakedowns', 'integer') }}                        as turret_takedowns,
    {{ challenge('takedownOnFirstTurret', 'integer') }}                  as takedown_on_first_turret,
    {{ challenge('bountyGold', 'double') }}                              as bounty_gold,
    {{ challenge('HealFromMapSources', 'double') }}                      as heal_from_map_sources,

    -- Visión
    {{ challenge('stealthWardsPlaced', 'integer') }}                     as stealth_wards_placed,
    {{ challenge('controlWardsPlaced', 'integer') }}                     as control_wards_placed,
    {{ challenge('wardTakedowns', 'integer') }}                          as ward_takedowns,
    {{ challenge('wardTakedownsBefore20M', 'integer') }}                 as ward_takedowns_before_20min,
    {{ challenge('wardsGuarded', 'integer') }}                           as wards_guarded,
    {{ challenge('twoWardsOneSweeperCount', 'integer') }}                as two_wards_one_sweeper_count
from participants
