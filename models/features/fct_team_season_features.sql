with matches as (
    select * from {{ ref('stg_epl_matches') }}
),

home as (
    select
        season,
        match_date,
        home_team                                       as team,
        1                                               as is_home,
        home_goals                                      as goals_scored,
        away_goals                                      as goals_conceded,
        case when result = 'H' then 3
             when result = 'D' then 1
             else 0 end                                 as points,
        case when result = 'H' then 1 else 0 end        as win,
        case when result = 'D' then 1 else 0 end        as draw,
        case when result = 'A' then 1 else 0 end        as loss,
        home_shots                                      as shots,
        away_shots                                      as shots_against,
        home_shots_on_target                            as shots_on_target,
        away_shots_on_target                            as shots_on_target_against,
        home_corners                                    as corners,
        home_yellow_cards                               as yellow_cards,
        home_red_cards                                  as red_cards,
        pinnacle_home_odds                              as win_odds,
        avg_close_home_odds                             as avg_close_win_odds
    from matches
),

away as (
    select
        season,
        match_date,
        away_team                                       as team,
        0                                               as is_home,
        away_goals                                      as goals_scored,
        home_goals                                      as goals_conceded,
        case when result = 'A' then 3
             when result = 'D' then 1
             else 0 end                                 as points,
        case when result = 'A' then 1 else 0 end        as win,
        case when result = 'D' then 1 else 0 end        as draw,
        case when result = 'H' then 1 else 0 end        as loss,
        away_shots                                      as shots,
        home_shots                                      as shots_against,
        away_shots_on_target                            as shots_on_target,
        home_shots_on_target                            as shots_on_target_against,
        away_corners                                    as corners,
        away_yellow_cards                               as yellow_cards,
        away_red_cards                                  as red_cards,
        pinnacle_away_odds                              as win_odds,
        avg_close_away_odds                             as avg_close_win_odds
    from matches
),

all_matches as (
    select * from home
    union all
    select * from away
),

-- add matchday number per team per season
with_matchday as (
    select *,
        row_number() over (
            partition by season, team
            order by match_date asc
        ) as matchday
    from all_matches
),

-- apply matchday 31 cutoff
cutoff as (
    select * from with_matchday
    where matchday <= 31
),

features as (
    select
        season,
        team,

        -- current standings
        count(*)                                        as matches_played,
        sum(points)                                     as current_points,
        sum(goals_scored)                               as goals_scored,
        sum(goals_conceded)                             as goals_conceded,
        sum(goals_scored) - sum(goals_conceded)         as goal_difference,
        sum(win)                                        as wins,
        sum(draw)                                       as draws,
        sum(loss)                                       as losses,

        -- scoring rates
        round(avg(goals_scored), 3)                     as avg_goals_scored,
        round(avg(goals_conceded), 3)                   as avg_goals_conceded,
        round(avg(points), 3)                           as points_per_game,

        -- shot quality
        round(avg(shots_on_target), 3)                  as avg_shots_on_target,
        round(avg(shots_on_target_against), 3)          as avg_shots_on_target_against,
        round(
            safe_divide(sum(shots_on_target), sum(shots)), 3
        )                                               as shot_accuracy,

        -- home vs away split
        sum(case when is_home = 1 then points else 0 end)   as home_points,
        sum(case when is_home = 0 then points else 0 end)   as away_points,

        -- form (last 5 matches)
        round(avg(case when match_rank <= 5 then points end), 3)    as last_5_ppg,

        -- form (last 10 matches)
        round(avg(case when match_rank <= 10 then points end), 3)   as last_10_ppg,

        -- market implied probability (from closing odds)
        round(avg(safe_divide(1, avg_close_win_odds)), 3)   as avg_implied_win_prob

    from (
        select *,
            row_number() over (
                partition by season, team
                order by match_date desc
            ) as match_rank
        from cutoff
    )
    group by season, team
)

select * from features
order by season, current_points desc