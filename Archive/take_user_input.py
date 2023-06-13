import os

import numpy as np
import pandas as pd
from xlwings.main import Book

from app import config

base_path = os.path.dirname(__file__)

prepped_input_data_optimizer = os.path.join(
    base_path, "..\\..\\input_data\\prepped_input_data_optimizer\\"
)

# config_path = os.path.join(base_path, "../")
# os.chdir(config_path)

# log_path = os.path.join(
#     base_path, "..\\..\\logs\\"
# )

# def clear_log_contents():
#     open(log_path + "drop_frequency_optimization.log", "w").close()


def read_user_inputs():

    range_info = pd.read_csv(
        prepped_input_data_optimizer + "prepped_input_data_options.csv"
    )[
        [
            "NY Style ID",
            "NY Primary Colour",
            "NY Secondary Colour",
            "NY Launch Week (All Stores)",
            "NY Planned MD Week",
            "on_markdown",
        ]
    ].drop_duplicates()
    range_info["on_markdown"] = np.where(range_info["on_markdown"] == 1, "Yes", "No")

    results_dir = os.path.join(base_path, "..\\..\\results\\")
    os.chdir(results_dir)
    wb = Book(config.RESULT_FILE)  # "drop_schedule_D010.xlsx"
    sheet = wb.sheets[1]
    sheet.range("B15:G1000").clear_contents()
    sheet.range("B15").options(index=False, header=False).value = range_info

    same_specs = sheet.range("B3").value
    # forecast_option_sales = sheet.range('D5').value
    magnitude_of_first_drop = sheet.range("E5").value
    select_first_drop_WOC = sheet.range("E9").value
    shelf_capacity_pct = sheet.range("F9").value
    woc_to_be_maintained = sheet.range("G5").value
    num_wks_drops_zero = sheet.range("H5").value
    # drop_schedule = sheet.range('I5').value
    select_SMOQ_level = sheet.range("I9").value
    # aws_adjustment_factor = sheet.range('J9').value

    red_fact = (
        sheet.range("L4:M10").expand("table").options(pd.DataFrame, index=False).value
    )
    red_fact.dropna(axis=0, how="any", inplace=True)
    red_fact["week_from_mkdn"] = red_fact["week_from_mkdn"].astype(int)

    custom_specs = (
        sheet.range("B14:Q331").expand("table").options(pd.DataFrame, index=False).value
    )

    sheet.range("R15:T1000").clear_contents()
    sheet.range("U15:U1000").clear_contents()

    sheet.range("R15").options(index=False, header=False).value = range_info[
        ["NY Style ID", "NY Primary Colour", "NY Secondary Colour"]
    ]
    sheet.range("U15").options(index=False, header=False).value = (
        range_info["NY Style ID"] + "-" + range_info["NY Secondary Colour"]
    )

    ranging_data_raw_v1 = pd.read_csv(
        prepped_input_data_optimizer + "prepped_input_data_options.csv"
    )
    ranging_data_raw_v1 = ranging_data_raw_v1.fillna(0)
    select_data = pd.DataFrame(
        columns=[
            "option_filters",
            "shelf_capacity",
            "num_stores",
            "buy_quantity",
            "carry_over",
            "start_inventory",
            "magnitude_of_first_drop",
            "first_woc",
            "shelf_capacity_level",
            "first_drop_shelf_cap",
            "max_first_woc_shelf_cap",
            "woc_to_be_maint",
            "smoq",
            "num_clearance_weeks",
            "no_straight_zero",
        ]
    )
    select_data["option_filters"] = ranging_data_raw_v1["option_filters"]
    # select_data["send_to_optimizer"] = "y"
    select_data["shelf_capacity"] = ranging_data_raw_v1["NY Capacity Code"]
    select_data["num_stores"] = ranging_data_raw_v1["NY No. of Stores"]
    select_data["buy_quantity"] = ranging_data_raw_v1["NYH Rec U"]
    select_data["carry_over"] = ranging_data_raw_v1["NY Flow"]
    select_data["max_first_woc_shelf_cap"] = 12
    select_data["no_straight_zero"] = 3
    select_data["start_inventory"] = ranging_data_raw_v1["NY1Q OSOH U"]

    if same_specs == "Yes":
        select_data["magnitude_of_first_drop"] = magnitude_of_first_drop
        if magnitude_of_first_drop == "WOC":
            select_data["first_woc"] = int(select_first_drop_WOC)
            select_data["shelf_capacity_level"] = 0
            select_data["first_drop_shelf_cap"] = 0
        else:
            select_data["first_woc"] = 0
            select_data["shelf_capacity_level"] = shelf_capacity_pct
            select_data["first_drop_shelf_cap"] = (
                select_data["shelf_capacity_level"]
                * select_data["shelf_capacity"]
                * select_data["num_stores"]
            )

        select_data["woc_to_be_maint"] = int(woc_to_be_maintained)
        select_data["smoq"] = int(select_SMOQ_level)
        select_data["num_clearance_weeks"] = np.where(
            ranging_data_raw_v1["on_markdown"] == 0, 0, num_wks_drops_zero
        )

    else:
        select_data["magnitude_of_first_drop"] = custom_specs["Magnitude_of_first_drop"]
        select_data["first_woc"] = [
            int(i)
            for i in [
                0 if j is None else j for j in custom_specs["Select_first_drop_WOC"]
            ]
        ]
        select_data["shelf_capacity_level"] = custom_specs["Shelf_capacity_%"]
        select_data["first_drop_shelf_cap"] = (
            select_data["shelf_capacity_level"]
            * select_data["shelf_capacity"]
            * select_data["num_stores"]
        )

        select_data["woc_to_be_maint"] = [
            int(i) for i in custom_specs["WOC_to_be_maintained"]
        ]
        select_data["smoq"] = [int(i) for i in custom_specs["SMOQ_level"]]
        select_data["num_clearance_weeks"] = np.where(
            ranging_data_raw_v1["on_markdown"] == 0,
            0,
            [int(i) for i in custom_specs["Num_wks_drops_zero"]],
        )  # [int(i) for i in custom_specs["Num_wks_drops_zero"]]

        select_data["first_woc"] = np.where(
            select_data["magnitude_of_first_drop"] == "shelf_capacity",
            0,
            select_data["first_woc"],
        )
        select_data["shelf_capacity_level"] = np.where(
            select_data["magnitude_of_first_drop"] == "shelf_capacity",
            select_data["shelf_capacity_level"],
            0,
        )
        select_data["first_drop_shelf_cap"] = np.where(
            select_data["magnitude_of_first_drop"] == "shelf_capacity",
            (
                select_data["shelf_capacity_level"]
                * select_data["shelf_capacity"]
                * select_data["num_stores"]
            ),
            0,
        )

    return select_data, red_fact, num_wks_drops_zero, sheet


# =============================================================================
#
# =============================================================================


def adjust_sales_forecast_markdown_factor(
    markdown_reduction_factor, num_wks_drops_zero
):
    ranging_data_raw_v2 = pd.read_csv(
        prepped_input_data_optimizer + "sales_forecast_npdf.csv"
    )
    not_markdown = ranging_data_raw_v2[ranging_data_raw_v2["on_markdown"] == 0]
    markdown = ranging_data_raw_v2[ranging_data_raw_v2["on_markdown"] == 1]

    mkdn_last_n = (
        markdown.sort_values(["option_sales_fcst", "week_end_date"])
        .groupby("option_sales_fcst")
        .tail(num_wks_drops_zero)
    )
    mkdn_first_n = markdown.drop(mkdn_last_n.index)
    mkdn_first_n_sum = (
        mkdn_first_n.groupby("option_sales_fcst")["sales_forecast"]
        .sum()
        .rename("sum")
        .reset_index()
    )
    mkdn_first_n = mkdn_first_n.merge(
        mkdn_first_n_sum, how="left", on="option_sales_fcst"
    )
    mkdn_first_n["ratio"] = mkdn_first_n["sales_forecast"] / mkdn_first_n["sum"]

    mkdn_last_n["index"] = mkdn_last_n.index

    mkdn_last_n["row"] = mkdn_last_n.groupby("option_sales_fcst")["index"].rank(
        method="first", ascending=False
    )
    mkdn_last_n = mkdn_last_n.merge(
        markdown_reduction_factor, how="left", left_on="row", right_on="week_from_mkdn"
    )
    mkdn_last_n["sales_forecast_adj"] = mkdn_last_n["sales_forecast"] * (
        1 - mkdn_last_n["reduction_factor"]
    )
    mkdn_last_n["diff"] = (
        mkdn_last_n["sales_forecast"] - mkdn_last_n["sales_forecast_adj"]
    )

    mkdn_last_n_diff = mkdn_last_n.groupby("option_sales_fcst").agg({"diff": "sum"})
    mkdn_last_n.drop(
        [
            "index",
            "row",
            "week_from_mkdn",
            "reduction_factor",
            "sales_forecast",
            "diff",
        ],
        axis=1,
        inplace=True,
    )

    mkdn_first_n = mkdn_first_n.merge(
        mkdn_last_n_diff, how="left", on="option_sales_fcst"
    )
    mkdn_first_n["sales_forecast_adj"] = mkdn_first_n["sales_forecast"] + (
        mkdn_first_n["ratio"] * mkdn_first_n["diff"]
    )
    mkdn_first_n.drop(["sales_forecast", "sum", "ratio", "diff"], axis=1, inplace=True)

    markdown_all = mkdn_first_n.append(mkdn_last_n)
    markdown_all = markdown_all.sort_values(["option_sales_fcst", "week_end_date"])
    markdown_all.rename(columns={"sales_forecast_adj": "sales_forecast"}, inplace=True)
    all_data = not_markdown.append(markdown_all)
    all_data = all_data.sort_values(["option_sales_fcst", "week_end_date"])
    all_data.reset_index(drop=True, inplace=True)
    all_data.drop("on_markdown", axis=1, inplace=True)

    # prepped_input_data_optimizer = os.path.join(base_path, "..\\..\\input_data\\prepped_input_data_optimizer\\")  # noqa: E501
    all_data.to_csv(
        prepped_input_data_optimizer + "adjusted_sales_forecast.csv", index=False
    )

    return all_data


# adjusted_sales_forecast = adjust_sales_forecast_markdown_factor(markdown_reduction_factor, num_wks_drops_zero)  # noqa: E501

# =============================================================================
#
# =============================================================================


def prepare_final_input_data_for_optimizer(
    user_selection_data, adjusted_sales_forecast, sheet
):
    options_to_run = (
        sheet.range("U14:V1000")
        .expand("table")
        .options(pd.DataFrame, index=False)
        .value
    )
    options_to_run = options_to_run[options_to_run["Send_to_optimizer"] == "y"]
    user_selection_data = user_selection_data.merge(
        options_to_run, how="inner", left_on="option_filters", right_on="NY Option"
    )

    input_data_for_optimizer = adjusted_sales_forecast.join(user_selection_data)
    input_data_for_optimizer.drop("NY Option", axis=1, inplace=True)

    return input_data_for_optimizer


# if __name__ == "__main__":
(
    user_selection_data,
    markdown_reduction_factor,
    num_wks_drops_zero,
    sheet,
) = read_user_inputs()


adjusted_sales_forecast = adjust_sales_forecast_markdown_factor(
    markdown_reduction_factor, num_wks_drops_zero
)


input_data_for_optimizer = prepare_final_input_data_for_optimizer(
    user_selection_data, adjusted_sales_forecast, sheet
)

os.chdir(config.APP_PATH)
