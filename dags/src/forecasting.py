# Part1: Settings

import json
import os
import pickle
import re

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error

RAW_DATA = "/opt/airflow/dags/data/bike_demand_hourly.csv"   
WORK_DIR = "/opt/airflow/working_data"                        
TARGET = "count"                                             
TEST_SHARE = 0.2                                            
MIN_ROWS = 1000                                              
REQUIRED_COLUMNS = [
    "season", "year", "month", "hour", "holiday", "weekday", "workingday",
    "weather", "temp", "feel_temp", "humidity", "windspeed", TARGET,
]

MODELS = {
    "ridge_baseline": lambda: Ridge(alpha=1.0),
    "random_forest": lambda: RandomForestRegressor(
        n_estimators=150, max_depth=15, n_jobs=2, random_state=42
    ),
    "gradient_boosting": lambda: HistGradientBoostingRegressor(max_iter=300, random_state=42),
}

# Part2: Data check and build features

def run_folder(run_id):
    safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    path = os.path.join(WORK_DIR, safe_name)
    os.makedirs(path, exist_ok=True)
    return path


def validate_data(raw_path, out_dir):
    df = pd.read_csv(raw_path)

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    if len(df) < MIN_ROWS:
        raise ValueError(f"Only {len(df)} rows; need at least {MIN_ROWS}")
    if df[TARGET].isna().any():
        raise ValueError("Demand column has missing values")
    if (df[TARGET] < 0).any():
        raise ValueError("Demand column has negative values")

    clean_path = os.path.join(out_dir, "validated.csv")
    df.to_csv(clean_path, index=False)
    print(f"Data check passed: {len(df)} rows, {df.shape[1]} columns, "
          f"demand range {df[TARGET].min()}-{df[TARGET].max()}")
    return {"rows": int(len(df)), "path": clean_path}


def build_features(clean_path, out_dir):
    df = pd.read_csv(clean_path)
    text_columns = df.select_dtypes(include=["object", "string", "bool", "category"]).columns.tolist()
    df = pd.get_dummies(df, columns=text_columns, dtype=int)

    cut = int(len(df) * (1 - TEST_SHARE))
    train, test = df.iloc[:cut], df.iloc[cut:]

    train_path = os.path.join(out_dir, "train.csv")
    test_path = os.path.join(out_dir, "test.csv")
    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)
    print(f"Features built: {df.shape[1] - 1} features | train {len(train)} hours | test {len(test)} hours")
    return {"train_path": train_path, "test_path": test_path,
            "n_features": int(df.shape[1] - 1), "test_rows": int(len(test))}


# Part3: train, pick the best, publish

def forecast_scores(actual, forecast):
    actual = np.asarray(actual, dtype=float)
    forecast = np.asarray(forecast, dtype=float)
    return {
        "rmse": round(float(np.sqrt(mean_squared_error(actual, forecast))), 2),
        "mae": round(float(mean_absolute_error(actual, forecast)), 2),
        "wape": round(float(np.abs(actual - forecast).sum() / actual.sum()), 4),
        "bias": round(float((forecast.sum() - actual.sum()) / actual.sum()), 4),
    }


def train_and_evaluate(model_name, train_path, test_path, out_dir):
    train = pd.read_csv(train_path)
    test = pd.read_csv(test_path)
    X_train, y_train = train.drop(columns=TARGET), train[TARGET]
    X_test, y_test = test.drop(columns=TARGET), test[TARGET]

    model = MODELS[model_name]()
    model.fit(X_train, y_train)
    scores = forecast_scores(y_test, model.predict(X_test))

    model_path = os.path.join(out_dir, f"model_{model_name}.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    print(f"{model_name}: {scores}")
    return {"model": model_name, "model_path": model_path, **scores}


def pick_best(results):
    ranked = sorted(results, key=lambda r: r["wape"])
    for r in ranked:
        print(f"{r['model']:18s} WAPE={r['wape']:.1%}  RMSE={r['rmse']}")
    print(f"Best model: {ranked[0]['model']}")
    return {**ranked[0], "ranking": [r["model"] for r in ranked]}


def publish_forecast(best, test_path, out_dir):
    test = pd.read_csv(test_path)
    with open(best["model_path"], "rb") as f:
        model = pickle.load(f)

    forecast = model.predict(test.drop(columns=TARGET))
    output = pd.DataFrame({
        "hour_of_day": test["hour"].astype(int),
        "actual_demand": test[TARGET].astype(int),
        "forecast": np.clip(forecast, 0, None).round(0).astype(int),
    })
    forecast_path = os.path.join(out_dir, "forecast.csv")
    output.to_csv(forecast_path, index=False)

    report_path = os.path.join(out_dir, "report.json")
    with open(report_path, "w") as f:
        json.dump({k: v for k, v in best.items() if k != "model_path"}, f, indent=4)

    print(f"Published {len(output)} hourly forecasts from '{best['model']}' to {forecast_path}")
    return {"forecast_path": forecast_path, "report_path": report_path}