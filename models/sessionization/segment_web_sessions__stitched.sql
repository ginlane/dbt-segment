{{ config(
    partition_by = {'field': 'session_start_tstamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'session_id',
    cluster_by = 'session_id'
    )}}

with sessions as (
    select * from {{ref('segment_web_sessions__initial')}}
),

id_stitching as (
    select * from {{ref('segment_web_user_stitching')}}
),

joined as (

    select

        sessions.*,

        coalesce(id_stitching.user_id, sessions.anonymous_id)
            as blended_user_id

    from sessions
    left join id_stitching using (anonymous_id)

)

select * from joined
