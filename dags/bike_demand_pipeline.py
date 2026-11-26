# Part1: The pipeline settings

"""
# Bike Demand Forecast Pipeline

Weekly demand-forecasting pipeline (modified Airflow Lab 1).

**Steps:** validate data -> build features -> train 3 models in parallel -> pick the best
-> quality gate -> publish the forecast (or raise an alert if accuracy is too poor).

Trigger manually with **Trigger DAG** and change `max_wape` to test the quality gate.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

from src import forecasting   # our plain Python functions (dags/src/forecasting.py)


@dag(
    dag_id="bike_demand_forecast_pipeline",
    description="Weekly bike-demand forecast: validate, train 3 models, gate, publish",
    schedule="0 6 * * 1",                 # every Monday at 06:00 UTC (minute hour day month weekday)
    start_date=datetime(2026, 11, 1),     # the first date the schedule counts from
    catchup=False,                        # don't go back and run all the missed past Mondays
    default_args={                        # settings every task gets unless it says otherwise
        "owner": "Dheeraj Kusuma",
        "retries": 2,                             # if a task fails, try again up to 2 more times
        "retry_delay": timedelta(minutes=1),      # wait 1 minute between tries
        "execution_timeout": timedelta(minutes=15),  # stop a task that hangs for over 15 minutes
    },
    params={                              # settings you can change in the "Trigger DAG" form
        "max_wape": Param(
            0.25, type="number", minimum=0, maximum=1,
            description="Quality bar: the best model's WAPE must be at or below this (0.25 = 25%)",
        ),
    },
    tags=["mlops", "forecasting", "demand"],
    doc_md=__doc__,                       # shows the text at the top of this file in the Airflow UI
)
def bike_demand_forecast_pipeline():

# Part2: The tasks
    @task
    def validate_data():
        """Task 1: check the input data, and stop the pipeline if it looks wrong."""
        run_id = get_current_context()["run_id"]          # a unique name for this pipeline run
        out_dir = forecasting.run_folder(run_id)
        summary = forecasting.validate_data(forecasting.RAW_DATA, out_dir)
        return {**summary, "out_dir": out_dir}            # small summary passed to the next task (XCom)

    @task
    def build_features(validated: dict):
        """Task 2: one-hot encode text columns and split by time (past = train, future = test)."""
        features = forecasting.build_features(validated["path"], validated["out_dir"])
        return {**features, "out_dir": validated["out_dir"]}

    @task
    def train_model(model_name: str, features: dict):
        """Task 3 (x3, in parallel): train one model and score it on the future period."""
        return forecasting.train_and_evaluate(
            model_name, features["train_path"], features["test_path"], features["out_dir"]
        )

    @task
    def select_best(results: list):
        """Task 4: compare the 3 models and keep the one with the lowest WAPE."""
        return forecasting.pick_best(results)

    @task.branch
    def quality_gate(best: dict):
        """Task 5 (branch): choose which task runs next. The other one is skipped."""
        max_wape = get_current_context()["params"]["max_wape"]
        if best["wape"] <= max_wape:
            print(f"PASSED: WAPE {best['wape']:.1%} <= {max_wape:.0%}")
            return "publish_forecast"        # the task_id to run next
        print(f"FAILED: WAPE {best['wape']:.1%} > {max_wape:.0%}")
        return "alert_low_quality"

    @task
    def publish_forecast(best: dict, features: dict):
        """Task 6a: write forecast.csv and report.json for planners."""
        return forecasting.publish_forecast(best, features["test_path"], features["out_dir"])

    @task
    def alert_low_quality(best: dict):
        """Task 6b: the model isn't good enough, so nothing is published. Leave a clear warning in the log."""
        print(f"ALERT: best model '{best['model']}' has WAPE {best['wape']:.1%}. "
              "Forecast NOT published. Check the data or retrain with new settings.")

# Part3: Wire the tasks together

    # Wire the tasks together (this creates the arrows in the Graph view)
    validated = validate_data()
    features = build_features(validated)

    # Three training tasks that all depend on 'features' but not on each other, so they run in parallel.
    # .override(task_id=...) gives each copy its own name in the UI.
    results = [
        train_model.override(task_id=f"train_{name}")(name, features)
        for name in forecasting.MODELS
    ]

    best = select_best(results)                 # waits for all 3 training tasks
    gate = quality_gate(best)
    publish = publish_forecast(best, features)
    alert = alert_low_quality(best)
    gate >> [publish, alert]                    # the gate decides which of these two runs


bike_demand_forecast_pipeline()