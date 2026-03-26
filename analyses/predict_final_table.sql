-- Generate predicted 2025/26 final Premier League table
-- Uses the trained BigQuery ML model to predict final points for each team

SELECT
    team,
    current_points,
    ROUND(predicted_final_points)                                           as predicted_final_points,
    ROUND(predicted_final_points) - current_points                         as predicted_points_remaining,
    ROW_NUMBER() OVER (ORDER BY predicted_final_points DESC)               as predicted_position
FROM ML.PREDICT(
    MODEL `no-ssg-gcp-miden-isnd.analytics_epl.epl_final_points_model`,
    (
        SELECT
            *,
            goal_difference / matches_played    as gd_per_game,
            home_points / matches_played        as home_ppg,
            away_points / matches_played        as away_ppg
        FROM `no-ssg-gcp-miden-isnd.analytics_epl.fct_training_spine`
        WHERE is_prediction_row = true
    )
)
ORDER BY predicted_final_points DESC