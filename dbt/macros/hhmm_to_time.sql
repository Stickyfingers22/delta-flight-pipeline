{#
  BTS stores clock times as 4-character text, "HHMM" (e.g. "0705").
  "2400" means midnight at the end of the day; TIME has no 24:00, so it becomes 00:00.
  A value that isn't 4 digits makes the build fail instead of turning silently into NULL.
#}
{% macro hhmm_to_time(column) -%}
    case
        when {{ column }} is null then null
        when {{ column }} = '2400' then time '00:00'
        else make_time(
            cast(substr({{ column }}, 1, 2) as integer),
            cast(substr({{ column }}, 3, 2) as integer),
            0
        )
    end
{%- endmacro %}
