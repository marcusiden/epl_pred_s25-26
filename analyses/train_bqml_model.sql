-- Train a linear regression model to predict final Premier League points tally
-- Uses matchday 31 features from 10 historical seasons (2015/16 -> 2024/25) as training data

CREATE OR REPLACE MODEL `no-ssg-gcp-miden-isnd.analytics_epl.epl_final_points_model`
OPTIONS (
    model_type = "linear_reg",
    input_label_cols = ["final_points"],
    data_split_method = "AUTO_SPLIT"
) AS
SELECT
    points_per_game,
    avg_goals_scored,
    avg_goals_conceded,
    goal_difference / matches_played           as gd_per_game,
    home_points / matches_played               as home_ppg,
    away_points / matches_played               as away_ppg,
    avg_shots_on_target,
    avg_shots_on_target_against,
    shot_accuracy,
    last_5_ppg,
    last_10_ppg,
    avg_implied_win_prob,
    matches_played,
    final_points
FROM `no-ssg-gcp-miden-isnd.analytics_epl.fct_training_spine`
WHERE is_prediction_row = false