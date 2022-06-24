{{ config(
    partition_by = {'field': 'session_start_tstamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'session_id',
    cluster_by = 'session_id'
    )}}

with sessions as (

    select * from {{ref('segment_web_sessions__stitched')}}

),

windowed as (

    select

        *,

        row_number() over (
            partition by blended_user_id
            order by sessions.session_start_tstamp
            )
            as session_number

    from sessions

)

select * from windowed
