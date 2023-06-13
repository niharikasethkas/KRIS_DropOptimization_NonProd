def data_preparation():
    import copy
    import json
    import math
    import os
    import time
    from datetime import date, datetime

    import numpy as np
    import pandas as pd

    from dateutil import relativedelta
    # from fuzzywuzzy import fuzz
    # from fuzzywuzzy import process
    from app import config as cfg
    from app.data_engine.db_manager import DBManager

    # Snowflake setting
    db = DBManager('snowflake')


    # NY Start/End Date

    NY_Dates_q=f"""select min(DAY_DATE) as ny_start_date, max(DAY_DATE) as ny_end_date 
    from KSFPA.MR2C."DAY" 
    WHERE ACCOUNTING_HALF_ID=(SELECT ACCOUNTING_HALF_ID FROM KSFPA.MR2C."DAY"
    WHERE DAY_DATE= dateadd(year, 1,CURRENT_DATE()))"""

    NY_Dates_df=db.pull_into_dataframe(NY_Dates_q)

    ny_start_date=NY_Dates_df.values[0,0]
    ny_end_date=NY_Dates_df.values[0,1]


    # Getting list of NY options and their parameters

    NY_options_list_q = f"""(with date_fpw as
    (
    select distinct DAY_DATE, concat('F',substr(ACCOUNTING_YEAR_ID,3,2),'P',to_number(substr(ACCOUNTING_PERIOD_DESCRIPTION,8,2)),
    'W',ACCOUNTING_WEEK_ID) as FPW from  KSFPA.MR2C."DAY"
    )
    select distinct 
    a.STYLE_ID as NY_Style_ID,
    a.STYLE_ID as NY_Kmart_Style_Nbr,
    a.PRIMARY_COLOUR_NAME as NY_Primary_Colour,
    a.SECONDARY_COLOUR_NAME as NY_Secondary_Colour,
    a.STYLE_NAME as NY_Product_Description,
    dpt.department_description as NY_Dept,
    a.CLASS_NAME as NY_Class,
    a.SUB_CLASS_NAME as NY_Sub_Class,
    a.SUB_SUB_CLASS_NAME as NY_Sub_Sub_Class,
    b.AU_SELLPRICE_IN_GST as NY_Aus_FP_Sell,
    b.AU_LANDED_COST_EX_GST as NY_Landed_Cost,
    b.SEASONALITY_NAME as NY_Seasonality,
    b.CAPACITY as NY_Capacity_Code, 
    case when b.NO_OF_STORES is NULL then 326 else b.NO_OF_STORES end as NY_No_of_Stores,
    a.STYLE_ID as TY_Style_ID,
    a.STYLE_ID as TY_Kmart_Style_Nbr,
    a.PRIMARY_COLOUR_NAME as TY_Primary_Colour,
    a.SECONDARY_COLOUR_NAME as TY_Secondary_Colour,
    dt1.FPW as NY_Launch_Week_All_Stores,
    dt2.FPW as NY_Planned_MD_Week,
    b.TOTAL_RECEIPT_UNIT as NYH_Rec_U,
    '' as NY1Q_OSOH_U,
    b.AU_SELLPRICE_IN_GST as AU_Sale_Price ,
    a.product_service_identifier,a.class_code, 
    a.sub_class_code, a.sub_sub_class_code,
    b.price_tiering_name as merchandise_type_description,
    att.fixture_type as fixture_type_description,
    att.product_365, att.product_type, att.fabric_type, 
    att.neck_type, att.sleeve_length, att.length_type, att.material, 
    att.shape as shape_silhouette, att.pattern_type, att.selling_pack,
    b.on_range1 as on_range , 
    b.off_range1 as off_range , 
    a.is_new, a.is_active,tpr.department_code,
    b.AWS as planner_aws,a.tpr_id
    from ksfta.ddrpf.tpr_item a left join 
    (select *,
    case when ((on_range is null) or (on_range ='9999-12-31')
    or (on_range ='9999-12-20')) then '{ny_start_date}' else on_range end as on_range1,
    case when ((off_range is null) or (off_range ='9999-12-31')
    or (off_range ='9999-12-20')) then '{ny_end_date}' else off_range end as off_range1
    from ksfta.ddrpf.tpr_item_ny) as b
    on a.tpr_item_id = b.tpr_item_id
    left join ksfta.ddrpf.tpr tpr on a.tpr_id = tpr.tpr_id
    left join ksfpa.mr2c.department dpt
    on tpr.department_code=dpt.department_source_identifier
    left join ksfta.ddrpf.tpr_attribute att 
    on a.tpr_item_id = att.tpr_item_id
    left join date_fpw dt1
    on to_date(b.on_range1)=dt1.DAY_DATE
    left join date_fpw dt2
    on b.MARKDOWN_WEEK=dt2.DAY_DATE
    where a.tpr_id in
    (select distinct tpr_id from ksfta.ddrpf.tpr where is_default=TRUE
    and department_type in ('apparel') 
    and '{cfg.today_date}' between TY_WEEK_END_DATE_FROM and TY_WEEK_END_DATE_TO) 
    and (a.is_active=TRUE or a.is_new=TRUE)
    and a.style_id != 0)"""

    NY_options_list_df=db.pull_into_dataframe(NY_options_list_q)


    # NY_Capacity_Code To be taken from SF once uploaded
    # NY1Q_OSOH_U assuming no carryover stock at beginning of NY. Discuss with Mich for 365 products
    # NY_No_of_Stores Assuming all stores(326) till information is not available
    # planner_aws coming from TPR screen and is assumed to stay the same for 
    #           the remaining ranging weeks
    # Change today date in config

    NY_options_list_df['on_range']=pd.to_datetime(NY_options_list_df['on_range'])
    NY_options_list_df['off_range']=pd.to_datetime(NY_options_list_df['off_range'])

    # Replacing null launch weeks

    NY_LP_q=f"""select concat('F',substr(ACCOUNTING_YEAR_ID,3,2),'P',to_number(substr(ACCOUNTING_PERIOD_DESCRIPTION,8,2)),
    'W',ACCOUNTING_WEEK_ID) as FPW from  KSFPA.MR2C."DAY"
    where DAY_DATE='{ny_start_date}'"""

    NY_LP_df=db.pull_into_dataframe(NY_LP_q)
    ny_lp=NY_LP_df.values[0,0]

    NY_options_list_df['ny_launch_week_all_stores'].fillna(ny_lp,inplace=True)


    # Creating Flow variable
    NY_options_list_df['ny_flow']=np.where(NY_options_list_df['is_new']==True,' ','CARRYOVER')


    # Getting aggregated NY forecasts- AWS

    NY_fcst_q=f"""select distinct TPR_ID,PRODUCT_STYLE_SOURCE_ID,COLOUR,COLOUR_SEC,
    AWS as Recommended_AWS from ksfta.ddrpf.ny_forecast_apparel_v2
    where run_date = (select max(run_date) from ksfta.ddrpf.ny_forecast_apparel_v2)"""

    NY_fcst=db.pull_into_dataframe(NY_fcst_q)

    NY_options_list_df['ny_style_id']=NY_options_list_df['ny_style_id'].astype(str)
    NY_fcst['tpr_id'].fillna(0,inplace=True)
    NY_fcst['tpr_id']=NY_fcst['tpr_id'].astype(int)

    NY_fcst.drop_duplicates(inplace=True)

    NY_options_list_df=NY_options_list_df.merge(NY_fcst[['tpr_id','product_style_source_id','colour',\
                                                        'colour_sec','recommended_aws']],\
                            left_on=['tpr_id','ny_style_id','ny_primary_colour','ny_secondary_colour'],\
                            right_on=['tpr_id','product_style_source_id','colour','colour_sec'],\
                            how='left')

    # NY_options_list_df.drop_duplicates(subset=['tpr_id','product_style_source_id','colour','colour_sec'], keep='first',inplace=True)
    NY_options_list_df.drop(['product_style_source_id','colour','colour_sec'],axis=1,inplace=True)

    # Replacing planner input for NY weekly forecast at option level
    NY_options_list_df['NY_AWS']=np.where(NY_options_list_df['planner_aws'].notna(),NY_options_list_df['planner_aws']\
                                        ,NY_options_list_df['recommended_aws'])


    # Calculating the Planning period
    Planning_period_q=f"""select 
    concat(SUBSTR(ACCOUNTING_QUARTER_ID, 1, 4),'-Q',
    SUBSTR(ACCOUNTING_QUARTER_ID, 5, 1)) as Planning_period,
    concat(SUBSTR(ACCOUNTING_QUARTER_ID, 1, 4),'-Q',
    to_number(to_number(SUBSTR(ACCOUNTING_QUARTER_ID, 5, 1))+1))as Planning_period1 
    from  KSFPA.MR2C."DAY" 
    where Day_DATE=(select min(DAY_DATE) from KSFPA.MR2C."DAY" 
    WHERE ACCOUNTING_HALF_ID=(SELECT ACCOUNTING_HALF_ID FROM KSFPA.MR2C."DAY"
    WHERE DAY_DATE= dateadd(year, 1,CURRENT_DATE())))
    """
    Planning_period_df=db.pull_into_dataframe(Planning_period_q)

    NY_options_list_df['Planning_period']= Planning_period_df.values[0,0]

    NY_options_list_df['ny_style_id'] = \
            NY_options_list_df['ny_style_id'].astype(str).\
                    apply(lambda x: x.replace('.0',''))

    # impute null style ids in new options with product service identifier
    NY_options_list_df['ny_style_id'] = np.where(
            (NY_options_list_df['ny_style_id']=='nan') & 
            (NY_options_list_df['is_new']==True), 
            NY_options_list_df['product_service_identifier'],
            NY_options_list_df['ny_style_id'])

    # delete null style ids in existing options
    NY_options_list_df = NY_options_list_df[NY_options_list_df['ny_style_id']\
                                            .notna()]

    # extract price hierarchy
    NY_options_list_df['price_group'] = \
            NY_options_list_df['merchandise_type_description'].apply(
            lambda x: x.split('-')[1] if x!= None else x
            )

    # substitute null ssc with sc and, null sc with class details
    NY_options_list_df.loc[:,'ny_sub_class'] = \
    np.where(NY_options_list_df['ny_sub_class'].isnull(),
            NY_options_list_df['ny_class'],
            NY_options_list_df['ny_sub_class'])

    NY_options_list_df.loc[:,'ny_sub_sub_class'] = \
    np.where(NY_options_list_df['ny_sub_sub_class'].isnull(),
            NY_options_list_df['ny_class'],
            NY_options_list_df['ny_sub_sub_class'])

    NY_options_list_df.loc[:,'sub_class_code'] = \
    np.where(NY_options_list_df['sub_class_code'].isnull(),
            NY_options_list_df['class_code'],
            NY_options_list_df['sub_class_code'])

    NY_options_list_df.loc[:,'sub_sub_class_code'] = \
    np.where(NY_options_list_df['sub_sub_class_code'].isnull(),
            NY_options_list_df['sub_class_code'],
            NY_options_list_df['sub_sub_class_code'])

    # create class, sub class and sub sub class fields
    NY_options_list_df['class'] = NY_options_list_df['class_code'].astype(str) + ' ' \
    + NY_options_list_df['ny_class']

    NY_options_list_df['sub_class'] = NY_options_list_df['sub_class_code'].astype(str) + ' ' \
    + NY_options_list_df['ny_sub_class']

    NY_options_list_df['sub_sub_class'] = NY_options_list_df['sub_sub_class_code'].astype(str) + ' ' \
    + NY_options_list_df['ny_sub_sub_class']


    NY_options_list_df.drop({'ny_class','ny_sub_class', 'ny_sub_sub_class'},\
                            axis=1,inplace=True)
    NY_options_list_df.rename\
            (columns={'class':'ny_class','sub_class':'ny_sub_class', \
            'sub_sub_class':'ny_sub_sub_class'},inplace=True)

    # add core_trend field
    core_trend_q = """select * from ksfpa.ddrpf.dept_core_trend"""

    core_trend_df = db.pull_into_dataframe(core_trend_q)

    # add core trend info
    NY_options_list_df = NY_options_list_df.merge(
            core_trend_df[['department_source_identifier',
            'product_primary_colour_desc', 'core_trend']],
            left_on=['department_code', 'ny_primary_colour'],
            right_on=['department_source_identifier',
            'product_primary_colour_desc'],
            how='left')


    # create place holders for not required columns
    nreqd_cols = ['customer_type', 'end_use_lifestyle',
    'number_of_stores_ranged_in',
    'xmas_product_flag']

    for col in nreqd_cols:
            NY_options_list_df.loc[:, col] = " "
            
    # add existing_new field
    NY_options_list_df['existing_new'] = np.where(NY_options_list_df['is_new']==True,
                    'new', 'existing')


    NY_options_list_df['on_range']=pd.to_datetime(NY_options_list_df['on_range'])
    NY_options_list_df['off_range']=pd.to_datetime(NY_options_list_df['off_range'])


    NY_options_list_df['on_range'] = np.where(
        (NY_options_list_df['on_range'].dt.date < ny_start_date) | 
        (NY_options_list_df['on_range'].isnull())|
        (NY_options_list_df['on_range']=='2099-12-31'), ny_start_date,
        NY_options_list_df['on_range'].dt.date)
    NY_options_list_df['on_range']=pd.to_datetime(NY_options_list_df['on_range'])

    NY_options_list_df['off_range'] = np.where(
        (NY_options_list_df['off_range'].dt.date>ny_end_date) |
        (NY_options_list_df['off_range'].isnull()) | 
        (NY_options_list_df['off_range'].dt.date < NY_options_list_df['on_range'].dt.date)|
        (NY_options_list_df['off_range']=='2099-12-31'), 
        ny_end_date,
        NY_options_list_df['off_range'].dt.date)
    NY_options_list_df['off_range']=pd.to_datetime(NY_options_list_df['off_range'])


    # remove obs where on-range is beyond off-range date
    NY_options_list_df = NY_options_list_df[~(NY_options_list_df['on_range']>
    NY_options_list_df['off_range'])]


    # Reordering, keeping and renaming only required columns

    ranging_data_raw=copy.deepcopy(NY_options_list_df)
    ranging_data_raw=ranging_data_raw[['tpr_id','ny_style_id','ny_kmart_style_nbr','ny_primary_colour',
    'ny_secondary_colour','ny_product_description','ny_dept','ny_class','ny_sub_class','ny_sub_sub_class',
    'ny_aus_fp_sell','ny_landed_cost','ny_seasonality','ny_flow','ny_capacity_code','ny_no_of_stores',
    'ty_style_id','ty_kmart_style_nbr',
    'ty_primary_colour','ty_secondary_colour','NY_AWS','Planning_period','ny_launch_week_all_stores',
    'ny_planned_md_week','nyh_rec_u','ny1q_osoh_u','existing_new','core_trend','product_365',
    'price_group','product_type','fabric_type','neck_type','sleeve_length','length_type','material',
    'shape_silhouette','pattern_type','fixture_type_description','customer_type','end_use_lifestyle',
    'selling_pack','number_of_stores_ranged_in','au_sale_price','xmas_product_flag','planner_aws',
    ]]

    ranging_data_raw.rename(columns={
    'ny_style_id':'NY Style ID','ny_kmart_style_nbr':'NY Kmart Style Nbr',
    'ny_primary_colour':'NY Primary Colour','ny_secondary_colour':'NY Secondary Colour',
    'ny_product_description':'NY Product Description','ny_dept':'NY Dept',
    'ny_class':'NY Class','ny_sub_class':'NY Sub Class','ny_sub_sub_class':'NY Sub Sub Class',
    'ny_aus_fp_sell':'NY Aus FP Sell','ny_landed_cost':'NY Landed Cost','ny_seasonality':'NY Seasonality',
    'ny_flow':'NY Flow','ny_capacity_code':'NY Capacity Code','ny_no_of_stores':'NY No. of Stores',
    'ty_style_id':'TY Style ID','ty_kmart_style_nbr':'TY Kmart Style Nbr',
    'ty_primary_colour':'TY Primary Colour','ty_secondary_colour':'TY Secondary Colour',
    'NY_AWS':'NY AWS','Planning_period':'Planning_period','ny_launch_week_all_stores':'NY Launch Week (All Stores)',
    'ny_planned_md_week':'NY Planned MD Week','nyh_rec_u':'NYH Rec U','ny1q_osoh_u':'NY1Q OSOH U',
    'existing_new':'Existing_New','core_trend':'Core_Trend','product_365':'Product 365',
    'price_group':'Price Hierarchy','product_type':'Product Type','fabric_type':'Fabric Type',
    'neck_type':'Neck Type','sleeve_length':'Sleeve Length','length_type':'Length Type',
    'material':'Material','shape_silhouette':'Shape Silhouette','pattern_type':'Pattern Type',
    'fixture_type_description':'Fixture Type','customer_type':'Customer Type',
    'end_use_lifestyle':'End Use Lifestyle','selling_pack':'Selling Pack',
    'number_of_stores_ranged_in':'Number of stores ranged in','au_sale_price':'AU Sale Price',
    'xmas_product_flag':'Xmas Product Flag','planner_aws':'Planner AWS'},inplace=True)

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

    # Getting Accounting Year Info

    account_yr_info_q=f"""
    select distinct 
    concat('F',substr(ACCOUNTING_YEAR_ID,3,2),'P',to_number(substr(ACCOUNTING_PERIOD_DESCRIPTION,8,2)),
    'W',ACCOUNTING_WEEK_ID) as period,  
    ACCOUNTING_WEEK_ID,ACCOUNTING_PERIOD_ID, 
    to_number(substr(ACCOUNTING_PERIOD_DESCRIPTION,8,2))as period_id,
    WEEK_END_DATE,
    concat('Q',substr(ACCOUNTING_QUARTER_DESCRIPTION,9,1)) as quarter,
    ACCOUNTING_YEAR_ID as year, 
    concat(ACCOUNTING_YEAR_ID,'-Q',substr(ACCOUNTING_QUARTER_DESCRIPTION,9,1)) as year_qtr,
    substr(ACCOUNTING_QUARTER_DESCRIPTION,9,1) as year_qtr_id,
    concat(ACCOUNTING_WEEK_ID,'/',substr(ACCOUNTING_PERIOD_ID,5,2),'/',substr(ACCOUNTING_PERIOD_ID,1,4)) as year_pd_wk
    from KSFPA.MR2C.WEEK"""

    account_yr_info=db.pull_into_dataframe(account_yr_info_q)

    account_yr_info["week_end_date"] = \
        pd.to_datetime(account_yr_info["week_end_date"])

    planning_start_pd = int(account_yr_info[
        account_yr_info["year_qtr"] == Planning_period_df["planning_period1"].unique()[0]
    ]["year_qtr_id"].unique()[0])
    planning_end_pd = planning_start_pd + 1

    account_yr_info["year_qtr_id"]=account_yr_info["year_qtr_id"].astype(int)

    planning_end_pd_wk =pd.to_datetime(np.max(
        account_yr_info[account_yr_info["year_qtr"] == \
                        Planning_period_df["planning_period1"].unique()[0]][
            "week_end_date"
        ]
    ))
    planning_start_pd_wk =pd.to_datetime(np.min(
        account_yr_info[account_yr_info["year_qtr"] == ranging_data_raw["Planning_period"].unique()[0]][
            "week_end_date"
        ]
    )
    )

    ranging_data_raw = ranging_data_raw.merge(
        account_yr_info[["period", "week_end_date", "year_qtr_id"]],
        how="left",
        left_on="NY Launch Week (All Stores)",
        right_on="period",
    )

    ranging_data_raw['year_qtr_id'].fillna(0,inplace=True)
    ranging_data_raw.replace([np.inf, -np.inf,pd.NaT], 0,inplace=True)
    ranging_data_raw['year_qtr_id']=ranging_data_raw['year_qtr_id'].astype(int)


    ranging_data_raw["week_end_date"]=pd.to_datetime(ranging_data_raw["week_end_date"])
    planning_start_pd_wk=pd.to_datetime(planning_start_pd_wk)

    ranging_data_raw["start_date"] = np.where(
        (ranging_data_raw["year_qtr_id"] < planning_start_pd)|
        (ranging_data_raw["week_end_date"].dt.date<planning_start_pd_wk),
        planning_start_pd_wk, ranging_data_raw["week_end_date"].dt.date)

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
        (ranging_data_raw["week_end_date"] > planning_end_pd_wk),
        planning_end_pd_wk,
        ranging_data_raw["week_end_date"].dt.date
    )

    ranging_data_raw["end_date"].replace(pd.NaT,planning_end_pd_wk,inplace=True)
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

    ranging_data_raw["forecast_end_date"].replace(pd.NaT,planning_end_pd_wk,inplace=True)

    ranging_data_raw["forecast_end_date"]=\
        pd.to_datetime(ranging_data_raw["forecast_end_date"])

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

    ranging_data_raw_v1 = ranging_data_raw_v1[
        ~ranging_data_raw_v1["option_filters"].isna()
    ]

    ranging_data_raw_v1['run_date']=cfg.today_date

    ranging_data_raw_v1['start_date'].replace(pd.NaT,planning_start_pd_wk,inplace=True)
    ranging_data_raw_v1['end_date'].replace(pd.NaT,planning_end_pd_wk,inplace=True)

    ranging_data_raw_v1['NY Style ID']=ranging_data_raw_v1['NY Style ID'].astype(int)

    ranging_data_raw_v1.drop(columns='tpr_id', inplace=True)

    # ranging_data_raw_v1=pd.read_pickle('../app/model/Working_Data/ranging_data_raw_v1.pkl')

    for col in ranging_data_raw_v1.select_dtypes(include=['object']).columns:
        ranging_data_raw_v1[col]=ranging_data_raw_v1[col].astype(str)

    # load prepped_input_data_options into Snowflake

    db.execute_query(f"""delete from ksfta.ddrpf.prepped_input_data_options
                where run_date = '{cfg.today_date}' """)

    db.insert_into_table(ranging_data_raw_v1, "prepped_input_data_options")



    # Preparing Sales Forecast NPDF

    ranging_data_raw_v2 = copy.deepcopy(ranging_data_raw)

    # Impute with end of planning half date
    ranging_data_raw_v2['start_date'].replace(pd.NaT,planning_start_pd_wk,inplace=True)
    ranging_data_raw_v2['end_date'].replace(pd.NaT,planning_end_pd_wk,inplace=True)


    ranging_data_raw_v2.start_date = ranging_data_raw_v2.apply(
            lambda x: pd.date_range(start=x["start_date"], end=x["end_date"], freq="W"),
            axis=1,
        )
    ranging_data_raw_v2 = ranging_data_raw_v2.explode("start_date")

    ranging_data_raw_v2["start_date"] = [i.date() for i in ranging_data_raw_v2["start_date"]]

    ranging_data_raw_v2["option_sales_fcst"] = (
        ranging_data_raw_v2["NY Style ID"] + "-" + ranging_data_raw_v2["NY Secondary Colour"]
    )

    account_yr_info["accounting_pd_wk"] = (
        account_yr_info["accounting_period_id"].astype(str)
        + ""
        + [str(i).zfill(2) for i in account_yr_info["accounting_week_id"]]
    )
    account_yr_info["accounting_pd_wk"] = account_yr_info["accounting_pd_wk"].astype(
        int
    )

    # Get KAFE forecast at week level from NY Model

    NY_fcst_1q=f"""
    select distinct TPR_ID,PRODUCT_STYLE_SOURCE_ID,COLOUR,COLOUR_SEC,
    WEEK_END_DATE,WEEKLY_FORECAST as sales_forecast
    from ksfta.ddrpf.ny_forecast_weekly_apparel_v2
    where run_date = (select max(run_date) from ksfta.ddrpf.ny_forecast_weekly_apparel_v2)
    """
    NY_fcst1=db.pull_into_dataframe(NY_fcst_1q)

    ranging_data_raw_v2['start_date']=pd.to_datetime(ranging_data_raw_v2['start_date'])
    NY_fcst1['week_end_date']=pd.to_datetime(NY_fcst1['week_end_date'])

    ranging_data_raw_v2=ranging_data_raw_v2.merge(NY_fcst1[[\
        'tpr_id','product_style_source_id','colour','colour_sec','week_end_date','sales_forecast']],\
                            left_on=['tpr_id','NY Style ID','NY Primary Colour','NY Secondary Colour','start_date'],\
                            right_on=['tpr_id','product_style_source_id','colour','colour_sec','week_end_date'],\
                            how='left')

    ranging_data_raw_v2.drop(columns=['product_style_source_id', 'colour', 'colour_sec',
        'week_end_date'],inplace=True)
    ranging_data_raw_v2.rename(
        columns={"NY Primary Colour": "primary_colour", "start_date": "week_end_date"},
        inplace=True,
    )
    ranging_data_raw_v2 = ranging_data_raw_v2.merge(
        account_yr_info[["accounting_pd_wk", "week_end_date"]],on='week_end_date',
        how="inner"
    )

    # Adjusting Planner AWS according to shape of KAFE forecasts

    avg = (
            ranging_data_raw_v2.groupby(['NY Style ID', 'primary_colour','NY Secondary Colour'])
            .agg({'sales_forecast':'mean'}) .reset_index()
        )

    avg.rename(columns={'sales_forecast':'mean_fcst'},inplace=True)

    ranging_data_raw_v2 = ranging_data_raw_v2.merge(
        avg, how="left", on=['NY Style ID', 'primary_colour','NY Secondary Colour']
    )
    ranging_data_raw_v2["SI"] = (
        ranging_data_raw_v2["sales_forecast"]
        / ranging_data_raw_v2["mean_fcst"]
    )
    ranging_data_raw_v2["sales_forecast_adj"] = np.where(
        ranging_data_raw_v2['Planner AWS'].notna(),
        (ranging_data_raw_v2["SI"] * ranging_data_raw_v2["Planner AWS"]),
        ranging_data_raw_v2["sales_forecast"])

    sales_forecast_kafe = ranging_data_raw_v2[
        [
            "option_sales_fcst",
            "primary_colour",
            "NY Secondary Colour",
            "on_markdown",
            "week_end_date",
            "accounting_pd_wk",
            "sales_forecast_adj",
        ]
    ]

    sales_forecast_kafe.rename(columns={'sales_forecast_adj':'sales_forecast'},inplace=True)
    sales_forecast_kafe['run_date']=cfg.today_date


    # load sales_forecast_kafe into Snowflake

    db.execute_query(f"""delete from ksfta.ddrpf.sales_forecast_kafe
                where run_date = '{cfg.today_date}' """)

    db.insert_into_table(sales_forecast_kafe, "sales_forecast_kafe")

if __name__ == "__main__":
    data_preparation()