# EPL 2025/26 Final Table Predictor

A end-to-end machine learning pipeline that predicts the final Premier League standings for the 2025/26 season, trained on 10 seasons of historical match data (2015/16 → 2024/25).

Built as a learning project to develop production-level data engineering and ML skills across the modern data stack.

---

## Predicted 2025/26 Final Table

Predictions generated at matchday 31 (March 2026) using two models.

| Pos | Team | Current Pts | BQML Prediction | XGBoost Prediction |
|-----|------|-------------|-----------------|-------------------|
| 1 | Arsenal | 70 | 84 | 85 |
| 2 | Man City | 61 | 77 | 74 |
| 3 | Man United | 55 | 67 | 66 |
| 4 | Aston Villa | 54 | 64 | 66 |
| 5 | Chelsea | 48 | 59 | 62 |
| 6 | Liverpool | 49 | 61 | 61 |
| 7 | Brentford | 46 | 56 | 56 |
| 8 | Everton | 46 | 55 | 56 |
| 9 | Fulham | 44 | 53 | 50 |
| 10 | Brighton | 43 | 53 | 54 |
| 11 | Newcastle | 42 | 52 | 51 |
| 12 | Bournemouth | 42 | 52 | 49 |
| 13 | Sunderland | 43 | 51 | 49 |
| 14 | Crystal Palace | 39 | 48 | 51 |
| 15 | Leeds | 33 | 43 | 43 |
| 16 | Nott'm Forest | 32 | 40 | 43 |
| 17 | Tottenham | 30 | 39 | 38 |
| 18 | West Ham | 29 | 34 | 38 |
| 19 | Burnley | 20 | 25 | 25 |
| 20 | Wolves | 17 | 23 | 23 |

*Relegated (model consensus): Burnley, Wolves. West Ham on the edge.*

---

## Architecture
```
football-data.co.uk CSVs (2015/16 → 2025/26)
            ↓
    GCS Bucket (premier_league_data_marcus)
            ↓
    BigQuery (raw_epl.pl_matches_raw)
            ↓
    dbt Cloud
    ├── staging/stg_epl_matches         → clean & rename raw match data
    ├── features/fct_team_season_features → 1 row per team per season at matchday 31
    └── spine/fct_training_spine         → ML-ready table with features + labels
            ↓
    ┌─────────────────────┬──────────────────────┐
    │   BigQuery ML       │   XGBoost (Python)   │
    │   Linear Regression │   Vertex AI Workbench│
    │   MAE: 2.99         │   MAE: 3.61          │
    │   R²:  0.96         │   R²:  0.90          │
    └─────────────────────┴──────────────────────┘
            ↓
    Predicted 2025/26 Final Table
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Cloud Platform | GCP |
| Raw Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt Cloud |
| ML (SQL) | BigQuery ML |
| ML (Python) | XGBoost, scikit-learn |
| Notebook | Vertex AI Workbench |
| CI/CD | GitHub Actions |
| Version Control | GitHub |

---

## Data Source

Match data sourced from [football-data.co.uk](https://www.football-data.co.uk) — free CSV downloads covering results, match statistics, and bookmaker odds for the Premier League from 2015/16 to 2025/26.

Raw CSVs are stored in GCS and are not committed to this repository.

---

## Feature Engineering

Features are engineered in dbt at the **matchday 31 cutoff** for every team in every season. Using a consistent cutoff is critical to avoid data leakage — the model is only allowed to see information that would be available at prediction time.

| Feature | Description |
|---------|-------------|
| `points_per_game` | Average points per match |
| `avg_goals_scored` | Average goals scored per match |
| `avg_goals_conceded` | Average goals conceded per match |
| `gd_per_game` | Goal difference per match |
| `home_ppg` | Home points per game |
| `away_ppg` | Away points per game |
| `avg_shots_on_target` | Average shots on target per match |
| `avg_shots_on_target_against` | Average shots on target conceded |
| `shot_accuracy` | Shots on target / total shots |
| `last_5_ppg` | Points per game in last 5 matches |
| `last_10_ppg` | Points per game in last 10 matches |
| `avg_implied_win_prob` | Market implied win probability from closing odds |
| `matches_played` | Games played at cutoff point |

---

## ML Approach

### Problem
Supervised regression — predict each team's final points tally at the end of the season based on their stats at matchday 31.

### Training Data
200 rows — 20 teams × 10 completed seasons (2015/16 → 2024/25).

### Prediction Data
20 rows — 20 teams in the current 2025/26 season at matchday 31.

### Models

**BigQuery ML — Linear Regression**
Trained entirely in SQL inside BigQuery using `CREATE MODEL`. Simple, fast, and interpretable. Assumes a linear relationship between features and final points.

**XGBoost — Gradient Boosted Trees**
Trained in Python using the XGBoost library. An ensemble of decision trees that can capture non-linear relationships. Despite being more complex, it performed slightly worse than linear regression — a common outcome with small datasets (200 rows).

### Key Lessons
- **Data leakage**: Always apply a consistent time cutoff to features. Using full season stats as features would make the model appear accurate but fail in production.
- **Train/test split**: Evaluating on training data gives misleadingly optimistic metrics. Always hold out unseen data for honest evaluation.
- **Complexity vs data size**: More powerful models need more data. With 200 rows, linear regression generalises better than XGBoost.

---

## Repository Structure
```
epl_pred_s25-26/
├── README.md
├── dbt_project.yml
├── packages.yml
├── .gitignore
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   └── stg_epl_matches.sql
│   ├── features/
│   │   └── fct_team_season_features.sql
│   └── spine/
│       └── fct_training_spine.sql
├── analyses/
│   ├── train_bqml_model.sql
│   └── predict_final_table.sql
└── notebooks/
    └── xgboost_epl_predictor.ipynb
```

---

## Setup

### Prerequisites
- GCP project with BigQuery and GCS enabled
- dbt Cloud account connected to BigQuery
- Python 3.9+ with the following packages:
```
  google-cloud-bigquery
  pandas
  scikit-learn
  xgboost
```

### Steps

**1. Upload raw data to GCS**
Download season CSVs from football-data.co.uk and upload to your GCS bucket:
```bash
gcloud storage cp *.csv gs://your-bucket/premier_league/
```

**2. Load to BigQuery**
```bash
for file in pl1516 pl1617 pl1718 pl1819 pl1920 pl2021 pl2122 pl2223 pl2324 pl2425 pl2526; do
  bq load \
    --autodetect \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --schema_update_option=ALLOW_FIELD_ADDITION \
    --null_marker="" \
    --column_name_character_map=V2 \
    your-project:raw_epl.pl_matches_raw \
    gs://your-bucket/premier_league/${file}.csv
done
```

**3. Add season column**
```bash
bq query --nouse_legacy_sql \
'ALTER TABLE `your-project.raw_epl.pl_matches_raw` ADD COLUMN IF NOT EXISTS season STRING'
```

**4. Run dbt pipeline**
```bash
dbt run
```

**5. Train BigQuery ML model**
Run the SQL in `analyses/train_bqml_model.sql` in BigQuery.

**6. Generate predictions**
Run the SQL in `analyses/predict_final_table.sql` in BigQuery.

**7. Run XGBoost notebook**
Open `notebooks/xgboost_epl_predictor.ipynb` and run all cells.

---

## Model Performance

| Model | MAE | R² | Notes |
|-------|-----|----|-------|
| BigQuery ML (Linear Regression) | 2.99 | 0.96 | AUTO_SPLIT validation |
| XGBoost | 3.61 | 0.90 | 80/20 train/test split |

MAE = Mean Absolute Error (points). On average the model's predicted final points tally is within 3 points of the actual finish.

---

## Limitations

- **Small training set** — 200 rows limits model complexity. More historical seasons would improve accuracy.
- **No fixture difficulty** — the model doesn't account for remaining fixture strength. A team on 49 points facing the top 6 is treated the same as one facing the bottom 6.
- **No injury/squad data** — managerial changes and key injuries are not captured in the features.
- **Promoted teams** — teams with no top flight history (Sunderland, Burnley, Leeds this season) have no historical baseline to compare against.

---

## Author

Marcus Iden — Data Engineer at Sopra Steria  
[github.com/marcusiden](https://github.com/marcusiden)