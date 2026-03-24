with source as (
    select * from {{ source('raw_epl', 'pl_matches_raw') }}
),

renamed as (
    select
        -- match identity
        season,
        Date                                        as match_date,
        HomeTeam                                    as home_team,
        AwayTeam                                    as away_team,

        -- full time result
        CAST(FTHG as INT64)                         as home_goals,
        CAST(FTAG as INT64)                         as away_goals,
        FTR                                         as result,

        -- half time result
        CAST(HTHG as INT64)                         as ht_home_goals,
        CAST(HTAG as INT64)                         as ht_away_goals,
        HTR                                         as ht_result,

        -- match stats
        CAST(HS as INT64)                           as home_shots,
        CAST(SAFE_CAST(`AS` as FLOAT64) as INT64)   as away_shots,
        CAST(HST as INT64)                          as home_shots_on_target,
        CAST(AST as INT64)                          as away_shots_on_target,
        CAST(HC as INT64)                           as home_corners,
        CAST(AC as INT64)                           as away_corners,
        CAST(HF as INT64)                           as home_fouls,
        CAST(AF as INT64)                           as away_fouls,
        CAST(HY as INT64)                           as home_yellow_cards,
        CAST(AY as INT64)                           as away_yellow_cards,
        CAST(HR as INT64)                           as home_red_cards,
        CAST(AR as INT64)                           as away_red_cards,

        -- odds (pinnacle + market average)
        CAST(PSH as FLOAT64)                        as pinnacle_home_odds,
        CAST(PSD as FLOAT64)                        as pinnacle_draw_odds,
        CAST(PSA as FLOAT64)                        as pinnacle_away_odds,
        CAST(AvgH as FLOAT64)                       as avg_home_odds,
        CAST(AvgD as FLOAT64)                       as avg_draw_odds,
        CAST(AvgA as FLOAT64)                       as avg_away_odds,

        -- closing odds
        CAST(PSCH as FLOAT64)                       as pinnacle_close_home_odds,
        CAST(PSCD as FLOAT64)                       as pinnacle_close_draw_odds,
        CAST(PSCA as FLOAT64)                       as pinnacle_close_away_odds,
        CAST(AvgCH as FLOAT64)                      as avg_close_home_odds,
        CAST(AvgCD as FLOAT64)                      as avg_close_draw_odds,
        CAST(AvgCA as FLOAT64)                      as avg_close_away_odds

    from source
)

select * from renamed