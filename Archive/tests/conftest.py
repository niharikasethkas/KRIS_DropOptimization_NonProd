import os

import pandas as pd
import pytest

base_path = os.path.dirname(__file__)
move_to_dir = os.path.join(base_path, "..\\input_data\\prepped_input_data_optimizer\\")


@pytest.fixture
def planner_sales():
    total_sales_planner = pd.read_csv(move_to_dir + "prepped_input_data_options.csv")
    check_sales_planner = pd.DataFrame()
    check_sales_planner["option_sales_fcst"] = total_sales_planner["option_filters"]
    check_sales_planner["total_sales_fcst"] = (
        total_sales_planner["NY AWS"] * total_sales_planner["wks_on_range"]
    )
    return check_sales_planner


@pytest.fixture
def npdf_forecast():
    npdf_sales_fcst = pd.read_csv(move_to_dir + "sales_forecast_npdf.csv")
    sales_forecast_npdf = (
        npdf_sales_fcst.groupby("option_sales_fcst")
        .agg({"sales_forecast": "sum"})
        .reset_index()
    )
    return sales_forecast_npdf


@pytest.fixture
def adjusted_forecast():
    adj_forecast = pd.read_csv(move_to_dir + "adjusted_sales_forecast.csv")
    adj_forecast = (
        adj_forecast.groupby("option_sales_fcst")
        .agg({"sales_forecast": "sum"})
        .reset_index()
    )
    return adj_forecast
