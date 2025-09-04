{{ config(
    materialized='view'
) }}

WITH all_teams AS (
    -- Get all unique teams from games
    SELECT DISTINCT home_team AS team_code
    FROM {{ ref('stg_games') }}
    
    UNION
    
    SELECT DISTINCT away_team AS team_code
    FROM {{ ref('stg_games') }}
),

team_aliases AS (
    SELECT 
        team_code,
        team_name,
        current_team_code
    FROM {{ ref('team_aliases') }}
),

teams_with_names AS (
    SELECT 
        t.team_code,
        COALESCE(ta.team_name, t.team_code) AS team_name,
        COALESCE(ta.current_team_code, t.team_code) AS current_team_code,
        -- Use current team code as canonical ID
        COALESCE(ta.current_team_code, t.team_code) AS team_id
    FROM all_teams t
    LEFT JOIN team_aliases ta ON t.team_code = ta.team_code
),

team_activity AS (
    SELECT 
        team_id,
        MIN(game_date) AS first_game_date,
        MAX(game_date) AS last_game_date,
        COUNT(DISTINCT season) AS seasons_played,
        COUNT(*) AS total_games
    FROM (
        -- Home games
        SELECT 
            COALESCE(ta.current_team_code, g.home_team) AS team_id,
            g.game_date,
            g.season
        FROM {{ ref('stg_games') }} g
        LEFT JOIN team_aliases ta ON g.home_team = ta.team_code
        
        UNION ALL
        
        -- Away games
        SELECT 
            COALESCE(ta.current_team_code, g.away_team) AS team_id,
            g.game_date,
            g.season
        FROM {{ ref('stg_games') }} g
        LEFT JOIN team_aliases ta ON g.away_team = ta.team_code
    ) games
    GROUP BY team_id
)

SELECT DISTINCT
    t.team_id,
    t.team_code,
    t.team_name,
    t.current_team_code,
    ta.first_game_date,
    ta.last_game_date,
    ta.seasons_played,
    ta.total_games,
    CASE 
        WHEN ta.last_game_date >= CURRENT_DATE - INTERVAL '2 years' THEN TRUE
        ELSE FALSE
    END AS is_active
FROM teams_with_names t
JOIN team_activity ta ON t.team_id = ta.team_id