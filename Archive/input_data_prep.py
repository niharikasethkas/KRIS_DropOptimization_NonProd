import copy
import json
import math
import os
# os.environ['PYTHONPATH'] = 'C:\\Users\\bawasthi\\OneDrive - Kmart Australia Limited\\Documents\\drop_frequency_optimization_tool'
import time
from datetime import date, datetime

import numpy as np
import pandas as pd
import requests



# pth = os.path.join(base_path, "..\\..\\")
# os.chdir(r'C:\Users\bawasthi\OneDrive - Kmart Australia Limited\Documents\drop_frequency_optimization_tool')
# print(pth)
from app import config
base_path = os.path.dirname(__file__)

def prepare_input_data():
    print(
        "Format raw input data for %s in appropriate format to enable NPDF tool to extract data."  # noqa: E501
        % config.INPUT_FILENAME.split("_")[0]
    )
    raw_input_data = os.path.join(base_path, "..\\..\\input_data\\raw_input_data\\")

    ranging_data_raw = pd.read_excel(raw_input_data + config.INPUT_FILENAME)
    account_yr_info = pd.read_csv(raw_input_data + config.ACCNT_YR_FILE)

    ranging_data_raw.dropna(axis=0, how="all", inplace=True)

    if any(ranging_data_raw["NY Secondary Colour"].isna()):
        num_nan = sum(ranging_data_raw["NY Secondary Colour"].isna())
        repl = ["Sec_Colour_" + str(i) for i in list(range(1, num_nan + 1))]
        for i, j in enumerate(ranging_data_raw["NY Secondary Colour"]):
            if not isinstance(j, str):
                ranging_data_raw["NY Secondary Colour"].iloc[i] = repl[0]
                repl.remove(repl[0])

    ranging_data_raw["option_filters"] = (
        ranging_data_raw["NY Style ID"] + "-" + ranging_data_raw["NY Secondary Colour"]
    )

    ranging_data_raw = ranging_data_raw[~ranging_data_raw["option_filters"].isna()]

    if len(ranging_data_raw) > len(set(ranging_data_raw["option_filters"])):
        dbl_cnt = list(
            ranging_data_raw["option_filters"]
            .value_counts()
            .index[ranging_data_raw["option_filters"].value_counts() > 1]
        )
        ranging_data_raw["cnt"] = (
            ranging_data_raw.groupby("option_filters").cumcount() + 1
        )
        ranging_data_raw["NY Style ID_v2"] = np.NaN
        for index, row in ranging_data_raw.iterrows():
            if ranging_data_raw.loc[index, "option_filters"] in dbl_cnt:
                ranging_data_raw.loc[index, "NY Style ID_v2"] = (
                    ranging_data_raw.loc[index, "NY Style ID"]
                    + "_"
                    + str(ranging_data_raw.loc[index, "cnt"])
                )
            else:
                ranging_data_raw.loc[index, "NY Style ID_v2"] = ranging_data_raw.loc[
                    index, "NY Style ID"
                ]

        ranging_data_raw["NY Style ID"] = ranging_data_raw["NY Style ID_v2"]
        ranging_data_raw.drop(
            ["NY Style ID_v2", "option_filters", "cnt"], inplace=True, axis=1
        )

    else:
        ranging_data_raw.drop("option_filters", inplace=True, axis=1)

    account_yr_info["week_end_date"] = [
        datetime.strptime(i, "%d/%m/%Y").date()
        for i in account_yr_info["week_end_date"]
    ]

    planning_start_pd = account_yr_info[
        account_yr_info["year_qtr"] == ranging_data_raw["Planning_period"].unique()[0]
    ]["year_qtr_id"].unique()[0]
    planning_end_pd = planning_start_pd + 1
    planning_end_pd_wk = np.max(
        account_yr_info[account_yr_info["year_qtr_id"] == planning_end_pd][
            "week_end_date"
        ]
    )
    planning_start_pd_wk = np.min(
        account_yr_info[account_yr_info["year_qtr_id"] == planning_start_pd][
            "week_end_date"
        ]
    )

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["period", "week_end_date", "year_qtr_id"]],
        how="left",
        left_on="NY Launch Week (All Stores)",
        right_on="period",
    )

    ranging_data_raw["start_date"] = np.where(
        ranging_data_raw["year_qtr_id"] < planning_start_pd,
        planning_start_pd_wk,
        ranging_data_raw["week_end_date"],
    )

    ranging_data_raw.drop(
        ["period", "week_end_date", "year_qtr_id"], axis=1, inplace=True
    )

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["period", "week_end_date"]],
        how="left",
        left_on="NY Planned MD Week",
        right_on="period",
    )

    ranging_data_raw["end_date"] = np.where(
        ranging_data_raw["week_end_date"] > planning_end_pd_wk,
        planning_end_pd_wk,
        ranging_data_raw["week_end_date"],
    )

    ranging_data_raw["on_markdown"] = np.where(
        ranging_data_raw["week_end_date"] > planning_end_pd_wk, 0, 1
    )

    ranging_data_raw["wks_on_range"] = [
        i.days + 1
        for i in ((ranging_data_raw["end_date"] - ranging_data_raw["start_date"]) / 7)
    ]

    ranging_data_raw.drop(["period", "week_end_date"], axis=1, inplace=True)

    ranging_data_raw["forecast_end_date"] = [
        (i + pd.DateOffset(days=7)).date() for i in ranging_data_raw["end_date"]
    ]

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["week_end_date", "period"]],
        how="left",
        left_on="forecast_end_date",
        right_on="week_end_date",
    )

    ranging_data_raw.drop(["forecast_end_date", "week_end_date"], axis=1, inplace=True)
    ranging_data_raw.rename(
        columns={"period": "NY Planned MD Week_plus_1wk"}, inplace=True
    )

    ranging_data_raw_v1 = copy.deepcopy(ranging_data_raw)
    ranging_data_raw_v1["option_filters"] = (
        ranging_data_raw_v1["NY Style ID"]
        + "-"
        + ranging_data_raw_v1["NY Secondary Colour"]
    )

    prepped_input_data_optimizer = os.path.join(
        base_path, "..\\..\\input_data\\prepped_input_data_optimizer\\"
    )
    ranging_data_raw_v1 = ranging_data_raw_v1[
        ~ranging_data_raw_v1["option_filters"].isna()
    ]
    ranging_data_raw_v1.to_csv(
        prepped_input_data_optimizer + "prepped_input_data_options.csv", index=False
    )

    ranging_data_raw.start_date = ranging_data_raw.apply(
        lambda x: pd.date_range(start=x["start_date"], end=x["end_date"], freq="W"),
        axis=1,
    )
    ranging_data_raw = ranging_data_raw.explode("start_date")

    ranging_data_raw["start_date"] = [i.date() for i in ranging_data_raw["start_date"]]

    ranging_data_raw["option_sales_fcst"] = (
        ranging_data_raw["NY Style ID"] + "-" + ranging_data_raw["NY Secondary Colour"]
    )
    ranging_data_raw.rename(
        columns={"NY Primary Colour": "primary_colour", "start_date": "week_end_date"},
        inplace=True,
    )

    account_yr_info["accounting_pd_wk"] = (
        account_yr_info["accounting_period_id"].astype(str)
        + ""
        + [str(i).zfill(2) for i in account_yr_info["accounting_week_id"]]
    )
    account_yr_info["accounting_pd_wk"] = account_yr_info["accounting_pd_wk"].astype(
        int
    )
    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["accounting_pd_wk", "week_end_date"]], how="inner"
    )
    ranging_data_raw.sort_values(
        ["option_sales_fcst", "primary_colour", "accounting_pd_wk"], inplace=True
    )

    ranging_data_raw_v2 = copy.deepcopy(ranging_data_raw)
    print(
        "Data prepped for %s for NPDF tool consumption"
        % config.INPUT_FILENAME.split("_")[0]
    )
    print("\n")
    return account_yr_info, ranging_data_raw_v1, ranging_data_raw_v2


def prepare_input_data_extend():
    print(
        "Format raw input data for %s in appropriate format to enable NPDF tool to extract data"  # noqa: E501
        % config.INPUT_FILENAME.split("_")[0]
    )
    raw_input_data = os.path.join(base_path, "..\\..\\input_data\\raw_input_data\\")

    ranging_data_raw = pd.read_excel(raw_input_data + config.INPUT_FILENAME)
    account_yr_info = pd.read_csv(raw_input_data + config.ACCNT_YR_FILE)

    ranging_data_raw.dropna(axis=0, how="all", inplace=True)
    account_yr_info["week_end_date"] = [
        datetime.strptime(i, "%d/%m/%Y").date()
        for i in account_yr_info["week_end_date"]
    ]

    if any(ranging_data_raw["NY Secondary Colour"].isna()):
        num_nan = sum(ranging_data_raw["NY Secondary Colour"].isna())
        repl = ["Sec_Colour_" + str(i) for i in list(range(1, num_nan + 1))]
        for i, j in enumerate(ranging_data_raw["NY Secondary Colour"]):
            if not isinstance(j, str):
                ranging_data_raw["NY Secondary Colour"].iloc[i] = repl[0]
                repl.remove(repl[0])

    #######
    ranging_data_raw["option_filters"] = (
        ranging_data_raw["NY Style ID"] + "-" + ranging_data_raw["NY Secondary Colour"]
    )

    ranging_data_raw = ranging_data_raw[~ranging_data_raw["option_filters"].isna()]

    if len(ranging_data_raw) > len(set(ranging_data_raw["option_filters"])):
        dbl_cnt = list(
            ranging_data_raw["option_filters"]
            .value_counts()
            .index[ranging_data_raw["option_filters"].value_counts() > 1]
        )
        ranging_data_raw["cnt"] = (
            ranging_data_raw.groupby("option_filters").cumcount() + 1
        )
        ranging_data_raw["NY Style ID_v2"] = np.NaN
        for index, row in ranging_data_raw.iterrows():
            if ranging_data_raw.loc[index, "option_filters"] in dbl_cnt:
                ranging_data_raw.loc[index, "NY Style ID_v2"] = (
                    ranging_data_raw.loc[index, "NY Style ID"]
                    + "_"
                    + str(ranging_data_raw.loc[index, "cnt"])
                )
            else:
                ranging_data_raw.loc[index, "NY Style ID_v2"] = ranging_data_raw.loc[
                    index, "NY Style ID"
                ]

        ranging_data_raw["NY Style ID"] = ranging_data_raw["NY Style ID_v2"]
        ranging_data_raw.drop(
            ["NY Style ID_v2", "option_filters", "cnt"], inplace=True, axis=1
        )

    else:
        ranging_data_raw.drop("option_filters", inplace=True, axis=1)
    ###########

    planning_start_pd = account_yr_info[
        account_yr_info["year_qtr"] == ranging_data_raw["Planning_period"].unique()[0]
    ]["year_qtr_id"].unique()[0]

    planning_start_pd_wk = np.min(
        account_yr_info[account_yr_info["year_qtr_id"] == planning_start_pd][
            "week_end_date"
        ]
    )

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["period", "week_end_date", "year_qtr_id"]],
        how="left",
        left_on="NY Launch Week (All Stores)",
        right_on="period",
    )

    ranging_data_raw["start_date"] = np.where(
        ranging_data_raw["year_qtr_id"] < planning_start_pd,
        planning_start_pd_wk,
        ranging_data_raw["week_end_date"],
    )
    ranging_data_raw.drop(
        ["period", "week_end_date", "year_qtr_id"], axis=1, inplace=True
    )

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["period", "week_end_date"]],
        how="left",
        left_on="NY Planned MD Week",
        right_on="period",
    )

    ranging_data_raw["end_date"] = ranging_data_raw["week_end_date"]

    ranging_data_raw["on_markdown"] = 1
    ranging_data_raw["wks_on_range"] = [
        i.days + 1
        for i in ((ranging_data_raw["end_date"] - ranging_data_raw["start_date"]) / 7)
    ]

    ranging_data_raw.drop(["period", "week_end_date"], axis=1, inplace=True)

    # ranging_data_raw["forecast_end_date"] = [
    #     (i + pd.DateOffset(days=7)).date() for i in ranging_data_raw["end_date"]
    # ]

    ranging_data_raw["forecast_end_date"] = [
        (i + pd.DateOffset(days=7)).date() if i < date(2023, 6, 25) else i
        for i in ranging_data_raw["end_date"]
    ]

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["week_end_date", "period"]],
        how="left",
        left_on="forecast_end_date",
        right_on="week_end_date",
    )
    ranging_data_raw.drop(["forecast_end_date", "week_end_date"], axis=1, inplace=True)
    ranging_data_raw.rename(
        columns={"period": "NY Planned MD Week_plus_1wk"}, inplace=True
    )

    ranging_data_raw_v1 = copy.deepcopy(ranging_data_raw)
    ranging_data_raw_v1["option_filters"] = (
        ranging_data_raw_v1["NY Style ID"]
        + "-"
        + ranging_data_raw_v1["NY Secondary Colour"]
    )

    prepped_input_data_optimizer = os.path.join(
        base_path, "..\\..\\input_data\\prepped_input_data_optimizer\\"
    )
    ranging_data_raw_v1 = ranging_data_raw_v1[
        ~ranging_data_raw_v1["option_filters"].isna()
    ]
    ranging_data_raw_v1.to_csv(
        prepped_input_data_optimizer + "prepped_input_data_options.csv", index=False
    )

    ranging_data_raw.start_date = ranging_data_raw.apply(
        lambda x: pd.date_range(start=x["start_date"], end=x["end_date"], freq="W"),
        axis=1,
    )
    ranging_data_raw = ranging_data_raw.explode("start_date")

    ranging_data_raw["start_date"] = [i.date() for i in ranging_data_raw["start_date"]]

    ranging_data_raw["option_sales_fcst"] = (
        ranging_data_raw["NY Style ID"] + "-" + ranging_data_raw["NY Secondary Colour"]
    )
    ranging_data_raw.rename(
        columns={"NY Primary Colour": "primary_colour", "start_date": "week_end_date"},
        inplace=True,
    )

    account_yr_info["accounting_pd_wk"] = (
        account_yr_info["accounting_period_id"].astype(str)
        + ""
        + [str(i).zfill(2) for i in account_yr_info["accounting_week_id"]]
    )
    account_yr_info["accounting_pd_wk"] = account_yr_info["accounting_pd_wk"].astype(
        int
    )
    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["accounting_pd_wk", "week_end_date"]], how="inner"
    )
    ranging_data_raw.sort_values(
        ["option_sales_fcst", "primary_colour", "accounting_pd_wk"], inplace=True
    )

    ranging_data_raw_v2 = copy.deepcopy(ranging_data_raw)
    # print("prepare_input_data() completed.")
    print(
        "Data prepped for %s for NPDF tool consumption"
        % config.INPUT_FILENAME.split("_")[0]
    )
    print("\n")
    return account_yr_info, ranging_data_raw_v1, ranging_data_raw_v2


# =============================================================================
#
# =============================================================================


def pull_npdf_sales_forecast(account_yr_info, prepped_input_data_options):
    print(
        "Start to pull forecast data for %s from NPDF tool"
        % config.INPUT_FILENAME.split("_")[0]
    )
    npdf_input_data = prepped_input_data_options
    npdf_input_data.drop("NY Planned MD Week", axis=1, inplace=True)
    npdf_input_data.rename(
        columns={"NY Planned MD Week_plus_1wk": "NY Planned MD Week"}, inplace=True
    )

    npdf_input_data.dropna(axis=0, how="all", inplace=True)

    npdf_input_data.rename(
        columns={
            "NY Dept": "Department",
            "NY Class": "Class",
            "NY Sub Class": "Sub Class",
            "NY Sub Sub Class": "Sub Sub Class",
            "NY Style ID": "Style ID",
            "NY Product Description": "Product Description",
            "NY Primary Colour": "Colour",
            'NY Secondary Colour': "colour_sec",
            "NY Seasonality": "Seasonality",
            "NY Launch Week (All Stores)": "Planned Launch Week",
            "NY Planned MD Week": "Planned Markdown Week",
        },
        inplace=True,
    )

    npdf_input_data = npdf_input_data[
        [
            "Existing_New",
            "Department",
            "Class",
            "Sub Class",
            "Sub Sub Class",
            "Style ID",
            "Product Description",
            "Colour",
            "colour_sec",
            "Core_Trend",
            "Product 365",
            "Price Hierarchy",
            "Seasonality",
            "Product Type",
            "Fabric Type",
            "Neck Type",
            "Sleeve Length",
            "Length Type",
            "Material",
            "Shape Silhouette",
            "Pattern Type",
            "Fixture Type",
            "Customer Type",
            "End Use Lifestyle",
            "Selling Pack",
            "Planned Launch Week",
            "Planned Markdown Week",
            "Number of stores ranged in",
            "AU Sale Price",
            "Xmas Product Flag",
            "Planner AWS",
        ]
    ]
    npdf_input_data = npdf_input_data.reset_index(drop=True)

    row_num = pd.DataFrame(range(1, len(npdf_input_data) + 1))
    npdf_input_data.insert(0, "row_num", row_num)
    npdf_input_data.fillna("", inplace=True)

    def str_repl(x):
        if len(x) > 0:
            return (
                x.strip()
                .replace(" - ", "_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
            )  # .lower()
        else:
            return x

    for k in npdf_input_data:
        # print(k)
        if k == "Department":
            npdf_input_data[k] = [
                str_repl(j[1])
                for j in [
                    npdf_input_data["Department"][i].split(" ", 1)
                    for i in range(len(npdf_input_data))
                ]
            ]
            # npdf_input_data[k] = [str_repl(j) for j in npdf_input_data['Department']]
        elif k in ["Class", "Sub Class", "Sub Sub Class"]:
            npdf_input_data[k] = [
                (j[0].strip() + "  " + str_repl(j[1]))
                for j in [
                    npdf_input_data[k][i].split(" ", 1)
                    for i in range(len(npdf_input_data))
                ]
            ]
        elif k not in [
            "row_num",
            "Number of stores ranged in",
            "AU Sale Price",
            "Planner AWS",
            "Department",
            "Class",
            "Sub Class",
            "Sub Sub Class",
        ]:
            npdf_input_data[k] = [str_repl(j) for j in npdf_input_data[k]]

    npdf_input_data["user_email"] = "internal"
    
    npdf_input_data.columns = [i.replace(" ", "_").lower() for i in npdf_input_data.columns]
    # lc = list(npdf_input_data.columns).index('colour')
    # npdf_input_data.insert(loc=lc+1, column="colour_sec", value="")
    
    payload={"df_in":npdf_input_data.to_json()}
    resp = requests.post('https://chanakya.da.int.ap-southeast-2.datasvcsprod.a-sharedinfra.net/ds-kafe/api',json=payload)
    forecast = pd.read_json(resp.json()[1]) #pd.read_json(resp.json())
    forecast["style_id"] = forecast["style_id"].astype("str")
    
    # url = "https://demandforecastnewstyles.da.int.ap-southeast-2.datasvcsnonprod.a-sharedinfra.net/api"  # noqa: E501

    # json_string = npdf_input_data.to_json(orient="records", lines=True)
    # json_string = json_string.replace('""', "null").replace("}", "},").strip()
    # json_string = json_string[:-1]
    # json_string = '{"records":[%s]}' % json_string

    # json_input = json.loads(json_string)

    # response = requests.post(url, json=json_input)
    # results = response.json()

    # forecast = pd.DataFrame(results["records"], columns=results["columns"])
    forecast = forecast[forecast["source"] == "model"]

    forecast = forecast.merge(
        account_yr_info[["year", "period_id", "accounting_week_id", "week_end_date"]],
        how="left",
        left_on=["acc_fy_id", "acc_period_id", "acc_week_id"],
        right_on=["year", "period_id", "accounting_week_id"],
    )
    # forecast["accounting_pd_wk"] = forecast["year"].astype(str) + "" + forecast["period_id"].astype(str) + "" + forecast["accounting_week_id"].astype(str) # noqa: E501
    # forecast["accounting_pd_wk"] = forecast["accounting_pd_wk"].astype(int)
    forecast.drop(["year", "period_id", "accounting_week_id"], axis=1, inplace=True)

    # print("pull_npdf_sales_forecast() completed.")
    # print("NPDF forecast pull completed.")
    return forecast


# =============================================================================
#
# =============================================================================


def adjust_sales_forecast_NY_AWS(npdf_sales_forecast, template_sales_forecast):
    fcst = npdf_sales_forecast[
        ["style_id", "colour", "colour_sec", "week_end_date"]
    ].drop_duplicates()
    fcst = (
        npdf_sales_forecast.groupby(["style_id", "colour", "colour_sec" ,"week_end_date"])
        .agg({"total_preds_median": "mean"})
        .reset_index()
    )

    for i in ["style_id", "colour", "colour_sec"]:
        fcst[i] = [j.upper() for j in fcst[i]]
        
    def str_repl(x):
        if len(x) > 0:
            return (
                x.strip()
                .replace(" - ", "_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
            )  # .lower()
        else:
            return x
        
    template_sales_forecast["colour_sec"] = [str_repl(i) for i in template_sales_forecast["NY Secondary Colour"]]

    template_sales_forecast = template_sales_forecast.merge(
        fcst,
        how="left",
        left_on=["NY Style ID", "primary_colour", "colour_sec", "week_end_date"],
        right_on=["style_id", "colour", "colour_sec" ,"week_end_date"],
    )
    template_sales_forecast.drop(["style_id", "colour", "colour_sec"], axis=1, inplace=True)

    unq_options = list(template_sales_forecast["option_sales_fcst"].unique())
    nan_options = []
    for i in unq_options:
        x = template_sales_forecast[template_sales_forecast["option_sales_fcst"] == i][
            "total_preds_median"
        ]
        if all([math.isnan(j) for j in x]):
            nan_options.append(i)
            template_sales_forecast.loc[x.index, "total_preds_median"] = 1000

    template_sales_forecast = template_sales_forecast.fillna(
        template_sales_forecast.groupby("option_sales_fcst", as_index=False).mean()
    )
    avg = (
        template_sales_forecast.groupby("option_sales_fcst")
        .agg(avg_total_preds_median=("total_preds_median", "mean"))
        .reset_index()
    )
    template_sales_forecast = template_sales_forecast.merge(
        avg, how="left", on="option_sales_fcst"
    )
    template_sales_forecast["SI"] = (
        template_sales_forecast["total_preds_median"]
        / template_sales_forecast["avg_total_preds_median"]
    )
    template_sales_forecast["sales_forecast"] = (
        template_sales_forecast["SI"] * template_sales_forecast["NY AWS"]
    )

    sales_forecast_npdf = template_sales_forecast[
        [
            "option_sales_fcst",
            "primary_colour",
            "NY Secondary Colour",
            "on_markdown",
            "week_end_date",
            "accounting_pd_wk",
            "sales_forecast",
        ]
    ]
    # sales_forecast_npdf.fillna(method="ffill", inplace=True)  # added
    sales_forecast_npdf["sales_forecast"] = sales_forecast_npdf[["option_sales_fcst", "sales_forecast"]].groupby("option_sales_fcst").transform(lambda x: x.fillna(x.mean()))
    
    prepped_input_data_optimizer = os.path.join(
        base_path, "..\\..\\input_data\\prepped_input_data_optimizer\\"
    )
    sales_forecast_npdf.to_csv(
        prepped_input_data_optimizer + "sales_forecast_npdf.csv", index=False
    )

    return sales_forecast_npdf


# if __name__ == "__main__":
start_time = time.time()
if config.EXTEND_BEYOND_PLAN_HALF == "No":
    (
        account_yr_info,
        prepped_input_data_options,
        template_sales_forecast,
    ) = prepare_input_data()
if config.EXTEND_BEYOND_PLAN_HALF == "Yes":
    (
        account_yr_info,
        prepped_input_data_options,
        template_sales_forecast,
    ) = prepare_input_data_extend()

npdf_sales_forecast = pull_npdf_sales_forecast(
    account_yr_info, prepped_input_data_options
)
sales_forecast_npdf = adjust_sales_forecast_NY_AWS(
    npdf_sales_forecast, template_sales_forecast
)

os.chdir(config.APP_PATH)
print("\n")
print(
    "NPDF forecast pulled for %s. Data extract written to ../drop_frequency_optimization_tool/input_data/"  # noqa: E501
    % config.INPUT_FILENAME.split("_")[0]
)
print("\n")
print(
    "Data preparation module ran successfully for %s"
    % config.INPUT_FILENAME.split("_")[0]
)
temp_time = str(time.time() - start_time)
print("\n")
print("---Module runtime secs = %s---" % (time.time() - start_time))
print("\n")
print("******* END!! CLOSING this window now. *******")
time.sleep(3)
