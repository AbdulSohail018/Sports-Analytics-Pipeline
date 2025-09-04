{{ config(
    materialized='table'
) }}

WITH games_union AS (
    -- Create one row per team per game
    -- Home team perspective
    SELECT 
        g.game_id,
        g.game_date,
        g.season,
        g.neutral,
        g.playoff,
        COALESCE(th.team_id, g.home_team) AS team_id,
        COALESCE(ta.team_id, g.away_team) AS opponent_id,
        'HOME' AS game_location,
        g.home_score AS score_for,
        g.away_score AS score_against,
        g.home_score_diff AS score_diff,
        g.home_win_flag AS win_flag,
        g.home_elo_pre AS elo_pre,
        g.home_elo_post AS elo_post,
        g.home_elo_change AS elo_change,
        g.home_win_prob AS win_probability
    FROM {{ ref('stg_games') }} g
    LEFT JOIN {{ ref('stg_teams') }} th ON g.home_team = th.team_code
    LEFT JOIN {{ ref('stg_teams') }} ta ON g.away_team = ta.team_code
    
    UNION ALL
    
    -- Away team perspective
    SELECT 
        g.game_id,
        g.game_date,
        g.season,
        g.neutral,
        g.playoff,
        COALESCE(ta.team_id, g.away_team) AS team_id,
        COALESCE(th.team_id, g.home_team) AS opponent_id,
        'AWAY' AS game_location,
        g.away_score AS score_for,
        g.home_score AS score_against,
        g.away_score_diff AS score_diff,
        g.away_win_flag AS win_flag,
        g.away_elo_pre AS elo_pre,
        g.away_elo_post AS elo_post,
        g.away_elo_change AS elo_change,
        g.away_win_prob AS win_probability
    FROM {{ ref('stg_games') }} g
    LEFT JOIN {{ ref('stg_teams') }} th ON g.home_team = th.team_code
    LEFT JOIN {{ ref('stg_teams') }} ta ON g.away_team = ta.team_code
),

games_enhanced AS (
    SELECT 
        *,
        -- Additional calculated fields
        CASE 
            WHEN score_diff > 20 THEN 'Blowout Win'
            WHEN score_diff > 10 THEN 'Comfortable Win'
            WHEN score_diff > 0 THEN 'Close Win'
            WHEN score_diff = 0 THEN 'Tie'
            WHEN score_diff > -10 THEN 'Close Loss'
            WHEN score_diff > -20 THEN 'Comfortable Loss'
            ELSE 'Blowout Loss'
        END AS game_outcome_type,
        
        -- Performance vs expectation
        CASE 
            WHEN win_flag = 1 AND win_probability < 0.3 THEN 'Major Upset'
            WHEN win_flag = 1 AND win_probability < 0.45 THEN 'Minor Upset'
            WHEN win_flag = 0 AND win_probability > 0.7 THEN 'Major Upset Loss'
            WHEN win_flag = 0 AND win_probability > 0.55 THEN 'Minor Upset Loss'
            ELSE 'Expected Result'
        END AS performance_vs_expectation,
        
        -- Game importance (based on ELO change magnitude)
        CASE 
            WHEN ABS(elo_change) > 30 THEN 'High Impact'
            WHEN ABS(elo_change) > 20 THEN 'Medium Impact'
            ELSE 'Low Impact'
        END AS game_impact
    FROM games_union
)

SELECT 
    game_id || '_' || team_id AS game_team_id,  -- Composite key
    game_id,
    team_id,
    opponent_id,
    game_date,
    season,
    game_location,
    CASE 
        WHEN neutral = 1 THEN 'NEUTRAL'
        ELSE game_location
    END AS adjusted_game_location,
    playoff,
    score_for,
    score_against,
    score_diff,
    win_flag,
    elo_pre,
    elo_post,
    elo_change,
    win_probability,
    game_outcome_type,
    performance_vs_expectation,
    game_impact,
    CURRENT_TIMESTAMP AS last_updated
FROM games_enhanced