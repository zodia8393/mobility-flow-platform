select 'SEOUL_TOPIS' as source_system
where coalesce(
    (
        select max(loaded_at)
        from {{ source('raw', 'traffic_observation') }}
        where source_system = 'SEOUL_TOPIS'
    ),
    '1970-01-01'::timestamptz
) < current_timestamp - interval '30 minutes'
