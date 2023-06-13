def run_optimization_func():    
    import os
    import time
    import numpy as np
    import pandas as pd
    import pytest
    from xlwings.main import Book
    from app import config
    from app.data_engine.db_manager import DBManager
    # from app.model.KRIS_User_input import user_input
    from app.model.KRIS_drop_frequency_optimization_functions_ortools import optimization_func
    # (
    #     user_selection_data,markdown_reduction_factor,
    #     num_wks_drops_zero,adjusted_sales_forecast, 
    #     input_data_for_optimizer,
    # )=user_input()
    
    (
        create_drop_schedule_report,
        read_input_data,
        run_optimization_workflow,input_data_for_optimizer,
    )=optimization_func()
    
   

    # Snowflake setting
    db = DBManager('snowflake')

    (sales_forecast_data, option_specs, unique_options, not_run_options) = read_input_data()

    account_yr_info_q=f"""
    select distinct 
    concat('F',substr(ACCOUNTING_YEAR_ID,3,2),'P',to_number(substr(ACCOUNTING_PERIOD_DESCRIPTION,8,2)),
    'W',ACCOUNTING_WEEK_ID) as period,  
    ACCOUNTING_WEEK_ID,ACCOUNTING_PERIOD_ID, 
    to_number(substr(ACCOUNTING_PERIOD_DESCRIPTION,8,2))as period_id,
    WEEK_END_DATE,
    concat('Q',substr(ACCOUNTING_QUARTER_DESCRIPTION,9,1)) as quarter,
    ACCOUNTING_YEAR_ID as year, concat(ACCOUNTING_YEAR_ID,'-Q',substr(ACCOUNTING_QUARTER_DESCRIPTION,9,1)) as year_qtr,substr(ACCOUNTING_QUARTER_DESCRIPTION,9,1) as year_qtr_id,
    concat(ACCOUNTING_WEEK_ID,'/',substr(ACCOUNTING_PERIOD_ID,5,2),'/',substr(ACCOUNTING_PERIOD_ID,1,4)) as year_pd_wk
    from KSFPA.MR2C.WEEK"""

    account_yr_info=db.pull_into_dataframe(account_yr_info_q)

    account_yr_info["week_end_date"] = \
        pd.to_datetime(account_yr_info["week_end_date"])

    account_yr_info["year_qtr_id"]=account_yr_info["year_qtr_id"].astype(int)

    account_yr_info.rename(
        columns={"period_id": "Period", "year": "Year", "accounting_week_id": "Week"},
        inplace=True,
    )


    prepped_input_data_options_q=f"""select * from ksfta.ddrpf.prepped_input_data_options 
    where run_date = '{config.today_date}'
    """
    input_data_optimizer=db.pull_into_dataframe(prepped_input_data_options_q)


    TPR_info_q = f"""(
    select distinct 
    a.STYLE_ID as NY_Style_ID,a.PRIMARY_COLOUR_NAME as NY_Primary_Colour,a.SECONDARY_COLOUR_NAME as NY_Secondary_Colour,
    a.tpr_id, a.tpr_item_id
    from ksfpa.ddrpf.tpr_item a
    where a.tpr_id in
    (select distinct tpr_id from ksfpa.ddrpf.tpr where is_default=TRUE
    and department_type in ('apparel') 
    and '{config.today_date}' between TY_WEEK_END_DATE_FROM and TY_WEEK_END_DATE_TO) 
    and (a.is_active=TRUE or a.is_new=TRUE)
    and a.style_id != 0)"""

    TPR_info_df=db.pull_into_dataframe(TPR_info_q)


    def run_optimization_model():
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
        print(temp_time)
        drop_schedule_details = drop_schedule_all.merge(
            option_specs.drop(['sales_forecast', 'run_date'], axis=1).drop_duplicates(),
            how="left",
            left_on="option",
            right_on="option_filters").drop("option_filters", axis=1)
        drop_schedule_details.rename(
            columns={'sales_fcst':'sales_forecast', "primary_colour": "Primary Colour", "drop_qty": "Quantity"},
            inplace=True,
        )
        temp_str = [str(i) for i in drop_schedule_details["accounting_pd_wk"]]
        drop_schedule_details["Year"] = [i[:4] for i in temp_str]
        drop_schedule_details["Period"] = ['P'+ i[4:6] for i in temp_str]
        drop_schedule_details["Week"] = ['WK'+ i[6:] for i in temp_str]

        input_data_optimizer["option"] = (
            str(input_data_optimizer["NY Style ID"])
            + "-"
            + input_data_optimizer["NY Secondary Colour"]
        )
        input_data_optimizer.rename(
            columns={"NY Primary Colour": "Primary Colour"}, inplace=True
        )
        drop_schedule_details = drop_schedule_details.merge(
            input_data_optimizer[["option", "Primary Colour"]],
            how="left",
        )

        drop_schedule_details.rename(columns={"Quantity": "Drop Quantity"}, inplace=True)
        
        drop_schedule_details["Option ID"] = [
            i[: i.rfind("-")] for i in drop_schedule_details["option"]
        ]
        drop_schedule_details["Secondary Colour"] = [
            i[i.rfind("-") + 1 :] for i in drop_schedule_details["option"]
        ]
    
        drop_details = pd.DataFrame()
        drop_details = drop_schedule_details[
            [
                "Option ID",
                "Primary Colour",
                "Secondary Colour",
                "week_end_date",
                "Year",
                "Period",
                "Week",
                "Drop Quantity",
                "sales_forecast",
                "ub_drop_qty"
            ]
        ]  
        drop_details.rename(columns={'ub_drop_qty':'Total_Receipts'},inplace=True)
        drop_details['Option ID']=drop_details['Option ID'].astype(int)
        drop_details=drop_details.merge(TPR_info_df,\
                                        left_on=["Option ID","Primary Colour","Secondary Colour"],
                                        right_on=['ny_style_id','ny_primary_colour','ny_secondary_colour',] , how='left')
        drop_details.drop(['ny_style_id','ny_primary_colour',\
                        'ny_secondary_colour'],axis=1,inplace=True)
        
        drop_details.replace([np.inf,-np.inf,np.nan],0,inplace=True)
        drop_schedule_details.replace([np.inf,-np.inf,np.nan],0,inplace=True)
        drop_schedule_details['carry_over'].replace(0,' ',inplace=True)

        cols=['tpr_id','tpr_item_id']
        for col in cols:
            drop_details[col]=drop_details[col].astype(int)

    
        drop_details['run_date']=config.now.strftime("%Y-%m-%d %H:%M:%S")
        drop_schedule_details['run_date']=config.now.strftime("%Y-%m-%d %H:%M:%S")
        
        drop_schedule_details=drop_schedule_details[['option', 'Option ID',  'Primary Colour', 'Secondary Colour',
            'week_end_date', 'accounting_pd_wk', 'Year', 'Period',  'Week',
        'sales_forecast', 'Drop Quantity', 'ub_drop_qty', 'inventory',
        'weeks_cover', 'num_drops', 'num_weeks', 'pct_drops_week',
        'avg_drop_size', 'min_drop_size', 'shelf_capacity', 'num_stores',
        'buy_quantity', 'carry_over', 'start_inventory',
        'magnitude_of_first_drop', 'first_woc', 'shelf_capacity_level',
        'first_drop_shelf_cap', 'max_first_woc_shelf_cap', 'woc_to_be_maint',
        'smoq', 'num_clearance_weeks', 'no_straight_zero', 'run_date']]

        drop_details.columns=  [col.lower().replace(" ",'_') for col in drop_details.columns]
        drop_schedule_details.columns=  [col.lower().replace(" ",'_') for col in drop_schedule_details.columns]

        return drop_schedule_details,drop_details
        
    drop_schedule_details,drop_details=run_optimization_model()
    # load final drop schedule into Snowflake
    db.execute_query(f"""delete from ksfta.ddrpf.apparel_drop_details            
    where substr(run_date,1,10) = '{config.today_date}' """)
    db.insert_into_table(drop_details, "apparel_drop_details")

    db.execute_query(f"""delete from ksfta.ddrpf.apparel_drop_schedule_details            
    where substr(run_date,1,10) = '{config.today_date}' """)
    db.insert_into_table(drop_schedule_details, "apparel_drop_schedule_details")


if __name__ == "__main__":
    run_optimization_func()

