with features as (
    select * from {{ ref('fct_team_season_features') }}
),

-- final points per team per season (the label)
-- we get this by taking the max matches played = 38 (full season)
final_standings as (
    select
        season,
        team,
        current_points                              as final_points,
        goal_difference                             as final_goal_difference,
        wins                                        as final_wins
    from features
    where matches_played = 38
),

-- join features at matchday 31 cutoff to final standings
spine as (
    select
        f.season,
        f.team,

        -- is this the current season (prediction row) or historical (training row)?
        case when f.season = '2025/26' then true else false end  as is_prediction_row,

        -- features (inputs to the model)
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

        -- label (what we're trying to predict)
        s.final_points,
        s.final_goal_difference,
        s.final_wins

    from features f
    left join final_standings s
        on f.season = s.season
        and f.team = s.team

    -- only keep matchday 31 snapshot for each team/season
    where f.matches_played between 29 and 31
)

select * from spine
order by season, current_points desc