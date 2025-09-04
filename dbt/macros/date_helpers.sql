-- Date helper macros for sports analytics

{% macro extract_season_from_date(date_column) %}
    -- NBA season typically runs Oct-June, so we use year of October
    CASE 
        WHEN EXTRACT(MONTH FROM {{ date_column }}) >= 10 THEN EXTRACT(YEAR FROM {{ date_column }})
        ELSE EXTRACT(YEAR FROM {{ date_column }}) - 1
    END
{% endmacro %}

{% macro season_start_date(season_year) %}
    -- Returns October 1st of the season year
    CAST({{ season_year }} || '-10-01' AS DATE)
{% endmacro %}

{% macro season_end_date(season_year) %}
    -- Returns June 30th of the following year
    CAST(({{ season_year }} + 1) || '-06-30' AS DATE)
{% endmacro %}

{% macro days_between(start_date, end_date) %}
    -- Calculate days between two dates
    DATEDIFF('day', {{ start_date }}, {{ end_date }})
{% endmacro %}