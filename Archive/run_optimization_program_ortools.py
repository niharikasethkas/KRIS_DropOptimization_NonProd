import os
import time
# os.environ['PYTHONPATH'] = 'C:\\Users\\bawasthi\\OneDrive - Kmart Australia Limited\\Documents\\drop_frequency_optimization_tool'
import numpy as np
import pandas as pd
import pytest
from xlwings.main import Book

from app import config
from app.model.drop_frequency_optimization_functions_ortools import (
    create_drop_schedule_report,
    read_input_data,
    run_optimization_workflow,
)

(sales_forecast_data, option_specs, unique_options, not_run_options) = read_input_data()

base_path = os.path.dirname(__file__)


raw_input_data = os.path.join(base_path, "..\\input_data\\raw_input_data\\")

prepped_input_data_optimizer = os.path.join(
    base_path, "..\\input_data\\prepped_input_data_optimizer\\"
)

account_yr_info = pd.read_csv(raw_input_data + config.ACCNT_YR_FILE)
tmp_date = [i.split("/") for i in account_yr_info["week_end_date"]]
day = [j.zfill(2) for j in [i[0] for i in tmp_date]]
date = [y[2] + "-" + y[1] + "-" + day[x] for x, y in enumerate(tmp_date)]
account_yr_info["week_end_date"] = date
account_yr_info.rename(
    columns={"period_id": "Period", "year": "Year", "accounting_week_id": "Week"},
    inplace=True,
)

input_data_optimizer = pd.read_csv(
    prepped_input_data_optimizer + "prepped_input_data_options.csv"
)


def summary(drop_plan):
    drop_plan["Primary Colour"] = drop_plan["Primary Colour"].fillna("MISSING")
    X = (
        drop_plan.groupby(["option", "Primary Colour"])
        .agg({"accounting_pd_wk": ["min", "max", "count"], "inventory": "min"})
        .reset_index()
    )

    X.columns = X.columns.droplevel(0)
    X.columns = [
        "option",
        "Primary Colour",
        "start_date",
        "end_date",
        "total_weeks",
        "min_inventory_level",
    ]

    Y = drop_plan.groupby("option").apply(lambda group: group.iloc[1:])
    Y.reset_index(drop=True, inplace=True)
    Y_summ = Y.groupby("option").agg({"weeks_cover": "mean"}).reset_index()
    Y_summ.columns = ["option", "avg_weeks_cover"]

    woc_lt_4 = drop_plan[(drop_plan["weeks_cover"] < 4)]
    woc_lt_4 = woc_lt_4.groupby("option").agg({"weeks_cover": "count"}).reset_index()

    summary = drop_plan[
        [
            "option",
            "Product Nbr",
            "Primary Colour",
            "Secondary Colour",
            "num_drops",
            "num_weeks",
            "pct_drops_week",
            "avg_drop_size",
            "min_drop_size",
            "num_clearance_weeks",
            "shelf_capacity",
            "num_stores",
            "buy_quantity",
            "carry_over",
            "start_inventory",
            "magnitude_of_first_drop",
            "first_woc",
            "shelf_capacity_level",
            "first_drop_shelf_cap",
            "woc_to_be_maint",
            "smoq",
            "NY AWS",
        ]
    ].drop_duplicates()

    summary["on_markdown"] = np.where(summary["num_clearance_weeks"] > 0, "yes", "no")

    summary = summary.merge(X).merge(Y_summ).merge(woc_lt_4, how="left")
    summary.rename(columns={"weeks_cover": "num_weeks_woc_less_4"}, inplace=True)
    summary["num_weeks_woc_less_4"].fillna(0, inplace=True)
    summary.drop("option", axis=1, inplace=True)

    final_summary = summary[
        [
            "Product Nbr",
            "Primary Colour",
            "Secondary Colour",
            "start_date",
            "end_date",
            "num_clearance_weeks",
            "on_markdown",
            "num_drops",
            "total_weeks",
            "num_weeks",
            "pct_drops_week",
            "avg_drop_size",
            "min_drop_size",
            "avg_weeks_cover",
            "num_weeks_woc_less_4",
            "min_inventory_level",
            "smoq",
            "shelf_capacity",
            "num_stores",
            "buy_quantity",
            "NY AWS",
            "carry_over",
            "start_inventory",
            "magnitude_of_first_drop",
            "first_woc",
            "shelf_capacity_level",
            "first_drop_shelf_cap",
            "woc_to_be_maint",
        ]
    ]

    return final_summary


def run_tests():
    pytest.main(["-x", config.TEST_PATH, ""])


def run_optimization_model():
    # RUNNER_LOGGER.info("%%%%% This is the START of the ITERATION ... %%%%%")

    run_tests()

    os.chdir(config.APP_PATH)
    start_time = time.time()
    drop_schedule_all = pd.DataFrame()
    errors = []
    n = 1
    N = len(unique_options)
    for opt in unique_options:
        print(str(n) + "/" + str(N) + "   " + opt)
        try:
            model, input_data_option, solver = run_optimization_workflow(
                sales_forecast_data, option_specs, opt
            )

            drop_schedule = create_drop_schedule_report(
                model, solver, sales_forecast_data, input_data_option, opt
            )
            drop_schedule_all = drop_schedule_all.append(drop_schedule)
            n += 1
            del model
        except:
            print("Model Errored out for option : " + opt)
            errors.append(opt)

    temp_time = str(time.time() - start_time)
    drop_schedule_details = drop_schedule_all.merge(
        option_specs.drop(["max_first_woc_shelf_cap", "no_straight_zero"], axis=1),
        how="left",
        left_on="option",
        right_on="option_filters",
    ).drop("option_filters", axis=1)

    # drop_schedule_details["week_end_date"] = [
    #     datetime.strptime(i, "%d/%m/%Y") for i in drop_schedule_details["week_end_date"] # noqa: E501
    # ]
    # print("--- %s solver runtime secs ---" % (time.time() - start_time))

    # RUNNER_LOGGER.info("%%%%% ITERATION Ended ...%%%%%")
    # RUNNER_LOGGER.info(
    #     "xxxxx Run time of the iteration = "
    #     + str(time.time() - start_time)
    #     + " secs xxxxx"
    # )
    # RUNNER_LOGGER.info("FINISHED")
    # print("\n")
    # RUNNER_LOGGER.info("END " * 50)

    #####
    drop_schedule_details.rename(
        columns={"primary_colour": "Primary Colour", "drop_qty": "Quantity"},
        inplace=True,
    )
    temp_str = [str(i) for i in drop_schedule_details["accounting_pd_wk"]]
    drop_schedule_details["Year"] = [i[:4] for i in temp_str]
    drop_schedule_details["Period"] = [i[4:6] for i in temp_str]
    drop_schedule_details["Week"] = [i[6:] for i in temp_str]

    input_data_optimizer["option"] = (
        input_data_optimizer["NY Style ID"]
        + "-"
        + input_data_optimizer["NY Secondary Colour"]
    )
    input_data_optimizer.rename(
        columns={"NY Primary Colour": "Primary Colour"}, inplace=True
    )
    drop_schedule_details = drop_schedule_details.merge(
        input_data_optimizer[["option", "Primary Colour", "Fabric Type", "NY AWS"]],
        how="left",
    )

    drop_schedule_details.rename(columns={"Fabric Type": "Fabric"}, inplace=True)
    drop_schedule_details["NZL Split"] = np.NaN
    drop_schedule_details["Allocation Comment for AUS"] = np.NaN
    drop_schedule_details["Allocation Comment for NZL"] = np.NaN

    drop_schedule_details["Product Nbr"] = [
        i[: i.rfind("-")] for i in drop_schedule_details["option"]
    ]
    drop_schedule_details["Secondary Colour"] = [
        i[i.rfind("-") + 1 :] for i in drop_schedule_details["option"]
    ]

    drop_details = drop_schedule_details[
        [
            "Product Nbr",
            "Primary Colour",
            "Secondary Colour",
            "Quantity",
            "Year",
            "Period",
            "Week",
            "Fabric",
            "NZL Split",
            "Allocation Comment for AUS",
            "Allocation Comment for NZL",
            "sales_fcst",
            "inventory",
            "weeks_cover",
        ]
    ]  # "num_drops", "num_weeks", "pct_drops_week"

    #####

    os.chdir(config.RESULT_PATH)
    wb = Book(config.RESULT_FILE)

    wb.sheets[2].range("A2:Y7000").clear_contents()
    wb.sheets[2].range("A2").options(index=False, header=False).value = drop_details

    wb.sheets[2].range("Q2:Q201").clear_contents()
    wb.sheets[2].range("Q2").options(index=False, header=False).value = pd.DataFrame(
        errors
    )

    wb.sheets[2].range("S2:S201").clear_contents()
    wb.sheets[2].range("S2").options(index=False, header=False).value = pd.DataFrame(
        not_run_options
    )

    sht_name = "drop_schedule_" + config.INPUT_FILENAME.split("_")[0]
    wb.sheets[2].name = sht_name

    wb.sheets[3].range("A2:AB1000").clear_contents()
    wb.sheets[3].range("A2").options(index=False, header=False).value = summary(
        drop_schedule_details
    )

    sht_name = "drop_plan_summary_" + config.INPUT_FILENAME.split("_")[0]
    wb.sheets[3].name = sht_name
    # wb.save()
    print("\n")
    if len(errors) == 0:
        print("NO ERRORS :)")
    else:
        print(errors)
    print("\n")
    print("*" * 100)
    print("-" * 50 + "COMPLETED !!" + "-" * 50)
    print("Total Runtime = " + temp_time + " secs")
    print("\n")
    print("******* CLOSING this window now. *******")
    print("*" * 100)
    time.sleep(3)
    os.chdir(config.APP_PATH)


if __name__ == "__main__":
    run_optimization_model()
