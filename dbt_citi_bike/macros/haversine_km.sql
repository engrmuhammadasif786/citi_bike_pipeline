{% macro haversine_km(lat1, lng1, lat2, lng2) %}
    round(
        2.0 * 6371.0 * asin(
            sqrt(
                power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2.0), 2) +
                cos(radians({{ lat1 }})) * cos(radians({{ lat2 }})) *
                power(sin(radians({{ lng2 }} - {{ lng1 }}) / 2.0), 2)
            )
        ),
        3
    )
{% endmacro %}
