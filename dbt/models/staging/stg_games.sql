{{ config(
    materialized='view'
) }}

WITH elo_games AS (
    SELECT 
        date AS game_date,
        season,
        neutral,
        playoff,
        team1 AS home_team,
        team2 AS away_team,
        elo1_pre AS home_elo_pre,
        elo2_pre AS away_elo_pre,
        elo1_post AS home_elo_post,
        elo2_post AS away_elo_post,
        score1 AS home_score,
        score2 AS away_score,
        elo_prob1 AS home_win_prob,
        elo_prob2 AS away_win_prob
    FROM {{ source('raw', 'elo') }}
    WHERE date IS NOT NULL
        AND team1 IS NOT NULL
        AND team2 IS NOT NULL
),

game_outcomes AS (
    SELECT 
        *,
        -- Calculate score differences
        home_score - away_score AS home_score_diff,
        away_score - home_score AS away_score_diff,
        
        -- Determine winners
        CASE 
            WHEN home_score > away_score THEN 1 
            ELSE 0 
        END AS home_win_flag,
        
        CASE 
            WHEN away_score > home_score THEN 1 
            ELSE 0 
        END AS away_win_flag,
        
        -- Calculate ELO changes
        home_elo_post - home_elo_pre AS home_elo_change,
        away_elo_post - away_elo_pre AS away_elo_change,
        
        -- Generate game ID
        CONCAT(
            REPLACE(CAST(game_date AS VARCHAR), '-', ''),
            '_',
            home_team,
            '_',
            away_team
        ) AS game_id
    FROM elo_games
)

SELECT 
    game_id,
    game_date,
    season,
    neutral,
    playoff,
    home_team,
    away_team,
    home_score,
    away_score,
    home_score_diff,
    away_score_diff,
    home_win_flag,
    away_win_flag,
    home_elo_pre,
    away_elo_pre,
    home_elo_post,
    away_elo_post,
    home_elo_change,
    away_elo_change,
    home_win_prob,
    away_win_prob
FROM game_outcomes