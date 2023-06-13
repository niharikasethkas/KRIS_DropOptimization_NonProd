def user_input():
    import copy
    import json
    import math
    import os
    import time
    from datetime import date, datetime

    import numpy as np
    import pandas as pd

    from dateutil import relativedelta
    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process
    from app import config 
    from app.data_engine.db_manager import DBManager

    # Snowflake setting
    db = DBManager('snowflake')

    # Getting Prepped input data options
    prepped_input_data_options_q=f"""select * from ksfta.ddrpf.prepped_input_data_options 
    where run_date = '{config.today_date}'
    """
    prepped_input_data_options_df=db.pull_into_dataframe(prepped_input_data_options_q)

    # Getting Sales Forecast KAFE

    sales_forecast_kafe_q=f"""select * from ksfta.ddrpf.sales_forecast_kafe 
    where run_date = '{config.today_date}'"""
    sales_forecast_kafe_df=db.pull_into_dataframe(sales_forecast_kafe_q)

    def read_user_inputs():

        range_info = prepped_input_data_options_df[
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

        same_specs = "Yes" #Criteria to be the same across all options
        magnitude_of_first_drop = "WOC"  #or "shelf_capacity" Set default as WOC
        select_first_drop_WOC = 4
        shelf_capacity_pct = (60*.01) #60%
        woc_to_be_maintained = 4
        num_wks_drops_zero = 4
        select_SMOQ_level = 500
    
        d = {'week_from_mkdn': [1,2,3,4], 'reduction_factor': [0,0,0,0]}
        red_fact=pd.DataFrame(data=d)

        custom_specs = prepped_input_data_options_df

        ranging_data_raw_v1 = prepped_input_data_options_df
        ranging_data_raw_v1 = prepped_input_data_options_df.fillna(0)
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
        select_data["start_inventory"] = 0 # ranging_data_raw_v1["NY1Q OSOH U"] set as 0 as considering fresh start of the season

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
            pass

        return select_data, red_fact, num_wks_drops_zero 


    # =============================================================================
    #
    # =============================================================================

    # markdown_reduction_factor=red_fact


    def adjust_sales_forecast_markdown_factor(
        markdown_reduction_factor, num_wks_drops_zero
    ):
        ranging_data_raw_v2 = sales_forecast_kafe_df
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

        # markdown_reduction_factor=red_fact
        mkdn_last_n = mkdn_last_n.merge(
            markdown_reduction_factor, how="left", left_on="row", right_on="week_from_mkdn"
        )
        mkdn_last_n["sales_forecast_adj"] = mkdn_last_n["sales_forecast"] * (
            1 - mkdn_last_n["reduction_factor"]
        )
        mkdn_last_n["diff"] = (
            mkdn_last_n["sales_forecast"] - mkdn_last_n["sales_forecast_adj"]
        )

        mkdn_last_n["diff"].fillna(0,inplace=True)
        mkdn_last_n_diff = mkdn_last_n.groupby("option_sales_fcst").\
            agg({"diff": "sum"})
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

        all_data['run_date']=config.today_date
        # load adjusted data into Snowflake
        db.execute_query(f"""delete from ksfta.ddrpf.adjusted_sales_forecast            
        where run_date = '{config.today_date}' """)
        db.insert_into_table(all_data, "adjusted_sales_forecast")
        return all_data


    # =============================================================================
    #
    # =============================================================================

    def prepare_final_input_data_for_optimizer(
        user_selection_data, adjusted_sales_forecast
    ):

        options_to_run=pd.DataFrame(user_selection_data['option_filters'])
        options_to_run.rename(columns={'option_filters':'NY Option'},inplace=True)
        options_to_run['Send_to_optimizer'] = 'y'
        # Send to optimizer -To give an option to the user. Logic To be incorporated later.
        # options_to_run = options_to_run[options_to_run["Send_to_optimizer"] == "y"]
        user_selection_data = user_selection_data.merge(
            options_to_run, how="inner", left_on="option_filters", right_on="NY Option"
        )

        input_data_for_optimizer = adjusted_sales_forecast.\
            merge(user_selection_data,left_on='option_sales_fcst',right_on='option_filters',how="inner")
        input_data_for_optimizer.drop("NY Option", axis=1, inplace=True)

        return input_data_for_optimizer


    (
        user_selection_data,
        markdown_reduction_factor,
        num_wks_drops_zero,
    ) = read_user_inputs()


    adjusted_sales_forecast = adjust_sales_forecast_markdown_factor(
        markdown_reduction_factor, num_wks_drops_zero
    )


    input_data_for_optimizer = prepare_final_input_data_for_optimizer(
        user_selection_data, adjusted_sales_forecast
    )
    
    return user_selection_data,markdown_reduction_factor,\
        num_wks_drops_zero,adjusted_sales_forecast,input_data_for_optimizer

# if __name__ == "__main__":
#     user_input()