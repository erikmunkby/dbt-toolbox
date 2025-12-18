-- Model to test Python module support in dbt-toolbox
-- Uses modules.datetime, modules.re, and modules.pytz

{% set parsed_date = modules.datetime.datetime.strptime("2025-05-01", "%Y-%m-%d") %}
{% set formatted_date = parsed_date.strftime("%B %d, %Y") %}
{% set cleaned_text = modules.re.sub("[^a-zA-Z0-9]", "_", "test-string@2025!") %}
{% set timezone = modules.pytz.timezone("America/New_York").zone %}

-- Test creating a datetime and localizing it to a timezone
{% set dt = modules.datetime.datetime(2002, 10, 27, 6, 0, 0) %}
{% set dt_local = modules.pytz.timezone('US/Eastern').localize(dt) %}
{% set dt_formatted = dt_local.strftime("%Y-%m-%d %H:%M:%S %Z") %}

select
    '{{ formatted_date }}' as formatted_date,
    '{{ cleaned_text }}' as cleaned_text,
    '{{ timezone }}' as timezone_name,
    {{ modules.datetime.datetime.now().year }} as current_year,
    '{{ dt_formatted }}' as localized_datetime,
    '{{ dt_local }}' as dt_local_raw
