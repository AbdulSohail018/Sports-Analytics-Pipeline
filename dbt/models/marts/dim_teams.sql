{{ config(
    materialized='table'
) }}

WITH teams_base AS (
    SELECT 
        team_id,
        team_name,
        current_team_code,
        first_game_date,
        last_game_date,
        seasons_played,
        total_games,
        is_active
    FROM {{ ref('stg_teams') }}
),

team_performance AS (
    -- Calculate overall team performance metrics
    SELECT 
        t.team_id,
        COUNT(*) AS games_analyzed,
        SUM(
            CASE 
                WHEN g.home_team = t.team_code THEN g.home_win_flag
                WHEN g.away_team = t.team_code THEN g.away_win_flag
                ELSE 0
            END
        ) AS total_wins,
        AVG(
            CASE 
                WHEN g.home_team = t.team_code THEN g.home_elo_post
                WHEN g.away_team = t.team_code THEN g.away_elo_post
            END
        ) AS avg_elo_rating,
        MAX(
            CASE 
                WHEN g.home_team = t.team_code THEN g.home_elo_post
                WHEN g.away_team = t.team_code THEN g.away_elo_post
            END
        ) AS peak_elo_rating
    FROM {{ ref('stg_teams') }} t
    LEFT JOIN {{ ref('stg_games') }} g 
        ON t.team_code = g.home_team OR t.team_code = g.away_team
    GROUP BY t.team_id
),

team_conference AS (
    -- Derive conference from team (simplified - would need external data for accuracy)
    SELECT 
        team_id,
        CASE 
            -- Eastern Conference teams
            WHEN team_id IN ('BOS', 'BKN', 'NYK', 'PHI', 'TOR', 
                           'CHI', 'CLE', 'DET', 'IND', 'MIL',
                           'ATL', 'CHA', 'MIA', 'ORL', 'WAS') THEN 'Eastern'
            -- Western Conference teams  
            WHEN team_id IN ('DEN', 'MIN', 'OKC', 'POR', 'UTA',
                           'GSW', 'LAC', 'LAL', 'PHX', 'SAC', 
                           'DAL', 'HOU', 'MEM', 'NOP', 'SAS') THEN 'Western'
            -- Historical/Unknown
            ELSE 'Historical'
        END AS conference
    FROM teams_base
)

SELECT 
    t.team_id,
    t.team_name,
    t.current_team_code,
    tc.conference,
    t.first_game_date,
    t.last_game_date,
    t.seasons_played,
    t.total_games,
    tp.total_wins,
    ROUND(tp.total_wins * 100.0 / NULLIF(tp.games_analyzed, 0), 2) AS historical_win_rate,
    ROUND(tp.avg_elo_rating, 1) AS avg_elo_rating,
    ROUND(tp.peak_elo_rating, 1) AS peak_elo_rating,
    t.is_active,
    CURRENT_TIMESTAMP AS last_updated
FROM teams_base t
LEFT JOIN team_performance tp ON t.team_id = tp.team_id
LEFT JOIN team_conference tc ON t.team_id = tc.team_id
ORDER BY t.team_name