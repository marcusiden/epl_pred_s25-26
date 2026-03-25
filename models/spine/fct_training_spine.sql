with features as (
    select * from {{ ref('fct_team_season_features') }}
),

-- calculate final standings directly from staging (all 38 games)
raw_matches as (
    select * from {{ ref('stg_epl_matches') }}
),

home_results as (
    select
        season,
        home_team as team,
        case when result = 'H' then 3
             when result = 'D' then 1
             else 0 end as points,
        home_goals - away_goals as gd
    from raw_matches
),

away_results as (
    select
        season,
        away_team as team,
        case when result = 'A' then 3
             when result = 'D' then 1
             else 0 end as points,
        away_goals - home_goals as gd
    from raw_matches
),

all_results as (
    select * from home_results
    union all
    select * from away_results
),

final_standings as (
    select
        season,
        team,
        sum(points)     as final_points,
        sum(gd)         as final_goal_difference,
        countif(points = 3) as final_wins
    from all_results
    group by season, team
),

-- join features at matchday 31 cutoff to final standings
spine as (
    select
        f.season,
        f.team,
        case when f.season = '2025/26' then true else false end  as is_prediction_row,
        f.matches_played,
        f.current_points,
        f.goal_difference,
        f.goals_scored,
        f.goals_conceded,
        f.wins,
        f.draws,
        f.losses,
        f.avg_goals_scored,
        f.avg_goals_conceded,
        f.points_per_game,
        f.avg_shots_on_target,
        f.avg_shots_on_target_against,
        f.shot_accuracy,
        f.home_points,
        f.away_points,
        f.last_5_ppg,
        f.last_10_ppg,
        f.avg_implied_win_prob,
        s.final_points,
        s.final_goal_difference,
        s.final_wins
    from features f
    left join final_standings s
        on f.season = s.season
        and f.team = s.team
    where f.matches_played between 29 and 31
)

select * from spine
order by season, current_points desc