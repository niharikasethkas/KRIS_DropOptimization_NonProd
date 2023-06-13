def optimization_func():
    import numpy as np
    import pandas as pd
    from ortools.sat.python import cp_model as cp
    from app.model.KRIS_User_input import user_input

    user_selection_data,markdown_reduction_factor,\
        num_wks_drops_zero,adjusted_sales_forecast,\
            input_data_for_optimizer=user_input()

    # import multiprocessing

    num_cpus = 8  # multiprocessing.cpu_count()


    def read_input_data(data=input_data_for_optimizer):

        data['carry_over'].replace(' ',0,inplace=True)
        data['sales_forecast'].fillna(0,inplace=True)

        options_to_run = np.unique(
            data[((data["Send_to_optimizer"] == "Y") | (data["Send_to_optimizer"] == "y"))][
                "option_filters"
            ]
        ) 
    # Ques - Why dropping NAs. MAinly getting dropped on Forecasts and Carryover
    # Ques - Who puts in buy quantity and on what basis
        option_spec_df = data.iloc[:, 5:].dropna()
        to_remove = list(
            option_spec_df[option_spec_df["buy_quantity"] < option_spec_df["smoq"]][
                "option_filters"
            ].unique()
        )
        option_spec_df = option_spec_df[~option_spec_df["option_filters"].isin(to_remove)]

        options_to_run = set(options_to_run) - set(to_remove)
        options_to_run = [i for i in options_to_run if len(i) > 0]

        # RUNNER_LOGGER.info("Input data read successfully !")

        sls_fcst_df = data.iloc[:, :6]  # .dropna()
        sls_fcst_df.sort_values(
            ["option_sales_fcst", "primary_colour", "accounting_pd_wk"], inplace=True
        )
        
        sls_fcst_df["sales_forecast"] = [0 if np.isnan(i) else round(i) for i in sls_fcst_df["sales_forecast"]]
        # sls_fcst_df["sales_forecast"] = [round(i) for i in sls_fcst_df["sales_forecast"]]
        sls_fcst_df["sales_forecast"] = np.where(
            sls_fcst_df["sales_forecast"] <= 0, 1, sls_fcst_df["sales_forecast"]
        )

        sls_fcst_df = sls_fcst_df[sls_fcst_df["option_sales_fcst"].isin(options_to_run)]

        # option_spec_df = data.iloc[:, 4:].dropna()
        option_spec_df = option_spec_df[option_spec_df["option_filters"] != ""]
        option_spec_df["sales_forecast"] = [0 if np.isnan(i) else round(i) for i in option_spec_df["sales_forecast"]]
        option_spec_df["sales_forecast"] = np.where(
            option_spec_df["sales_forecast"] <= 0, 1, option_spec_df["sales_forecast"]
        )
        for col in [
            "shelf_capacity",
            "num_stores",
            "buy_quantity",
            # "start_inventory",
            "first_woc",
            # "shelf_capacity_level",
            "first_drop_shelf_cap",
            "max_first_woc_shelf_cap",
            "woc_to_be_maint",
            "smoq",
            "num_clearance_weeks",
            "no_straight_zero",  #should be woc to be maintained -1
        ]:
            option_spec_df[col] = option_spec_df[col].astype(int)

        option_spec_df = option_spec_df[
            option_spec_df["option_filters"].isin(options_to_run)
        ]
        unique_options = list(option_spec_df["option_filters"].unique())
        option_spec_df.drop("Send_to_optimizer", axis=1, inplace=True)

        # RUNNER_LOGGER.info("Data prep done !")
        # RUNNER_LOGGER.info(
        #     "There are a total of " + str(len(unique_options)) + " options" + "\n"
        # )
        # RUNNER_LOGGER.info(
        #     "COMPLETED FUNCTION : read_input_data(file, path) " + "\n" + "-" * 100 + "\n"
        # )

        return sls_fcst_df, option_spec_df, unique_options, to_remove

    # sales_forecast_data=sls_fcst_df
    # option_specs=option_spec_df
    # not_run_options=to_remove
    # option='113348-BLACK'unique_options[1]

    def subset_option_input_data(sales_forecast_data, option_specs, option):
        option_data = {}
        option_data["sales_fcst"] = [
            round(i)
            for i in sales_forecast_data[
                sales_forecast_data["option_sales_fcst"] == option
            ]["sales_forecast"].to_list()
        ]

        option_data["primary_colour"] = sales_forecast_data[
            sales_forecast_data["option_sales_fcst"] == option
        ]["primary_colour"].unique()[0]
        option_data["num_wks_drops_plan"] = len(option_data["sales_fcst"])
        temp_data = option_specs[option_specs["option_filters"] == option]
        option_data["option"] = option  # temp_data["option_filters"].to_list()[0]
        option_data["ub_drop_qty"] = temp_data["buy_quantity"].to_list()[0]
        option_data["smoq"] = temp_data["smoq"].to_list()[0]
        option_data["carryover"] = temp_data["carry_over"].to_list()[0]
        option_data["boh_invt"] = temp_data["start_inventory"].to_list()[0]
        option_data["no_straight_zero"] = int(temp_data["no_straight_zero"].to_list()[0])
        option_data["num_clearance_weeks"] = int(
            temp_data["num_clearance_weeks"].to_list()[0]
        )
        option_data["first_woc"] = int(temp_data["first_woc"].to_list()[0])
        option_data["first_drop_shelf_cap"] = temp_data["first_drop_shelf_cap"].to_list()[0]
        option_data["woc_to_be_maintained"] = int(temp_data["woc_to_be_maint"].to_list()[0])
        return option_data



    def build_model_object():
        model = cp.CpModel()
        return model

    # input_data_option=option_data

    def create_decision_varbs(model, input_data_option):
        num_wks_drops_plan = input_data_option["num_wks_drops_plan"]
        total_buy_quantity = input_data_option["ub_drop_qty"]
        model._total_buy_quantity = total_buy_quantity
        smoq = input_data_option["smoq"]

        model._dv_drop_qty_1 = [
            model.NewIntVarFromDomain(
                cp.Domain.FromIntervals([[0], [smoq, total_buy_quantity]]),
                "dv_drop_qty_wk_%i" % i,
            )
            for i in range(num_wks_drops_plan)
        ]

        model._dv_drop_gt_0 = [
            model.NewBoolVar("dv_drop_gt_0_wk_%i" % i) for i in range(num_wks_drops_plan)
        ]

        model._dv_invt = [
            model.NewIntVar(-1000000, 1000000, "dv_invt_wk_%i" % i)
            for i in range(num_wks_drops_plan)
        ]

        model._dv_int_scale = [
            model.NewIntVar(-100000000000, 100000000000, "dv_invt_scale_wk_%i" % i)
            for i in range(num_wks_drops_plan)
        ]

        model._dv_woc = [
            model.NewIntVar(-100000000000, 100000000000, "dv_woc_wk_%i" % i)
            for i in range(num_wks_drops_plan)
        ]
        model._dv_invt_zero = [
            model.NewBoolVar("dv_invt_zero_wk_%i" % i) for i in range(num_wks_drops_plan)
        ]

        model._dv_woc_lt_4 = [
            model.NewBoolVar("dv_woc_lt_4_wk_%i" % i) for i in range(num_wks_drops_plan)
        ]
        model._dv_woc_4 = [model.NewBoolVar("") for i in range(num_wks_drops_plan)]

        model._num_drops = model.NewIntVar(0, num_wks_drops_plan, name="num_drops")

        model._sum_drop_qty = model.NewIntVar(0, total_buy_quantity, name="sum_drop_qty")

        model._maxWoc = model.NewIntVar(-100000000000, 100000000000, "")
        model._minWoc = model.NewIntVar(-100000000000, 100000000000, "")
        model._maxMinWocDiff = model.NewIntVar(-100000000000, 100000000000, "")
        model._maxMinWocScale = model.NewIntVar(-100000000000, 100000000000, "")

        return model


    def add_constraints_handle_first_drop(model, input_data_option):
        first_woc = input_data_option["first_woc"]
        carryover = input_data_option["carryover"]
        num_wks_drops_plan = input_data_option["num_wks_drops_plan"]
        sales_fcst = input_data_option["sales_fcst"]
        smoq = input_data_option["smoq"]
        ub_drop_qty = input_data_option["ub_drop_qty"]
        woc_to_be_maintained = input_data_option["woc_to_be_maintained"]
        # sales_fcst = input_data_option["sales_fcst"]
        boh_invt = input_data_option["boh_invt"]
        first_drop_shelf_cap = min(input_data_option["first_drop_shelf_cap"], ub_drop_qty)

        if (first_woc == 0) & (first_drop_shelf_cap == 0):
            first_drop_shelf_cap = input_data_option["first_drop_shelf_cap"] = np.max(
                [first_woc * sales_fcst[0], smoq]
            )
    # Ques - Check if woc to be maitained is 6  min weeks of cover to be maintained
    # Ques - All carryovers getting dropped with dropna
        ##### Start from here
        if first_woc > 0:
            first_drop_size_nonCarry = sales_fcst[0] * (first_woc + 1)
            first_drop_size_carry = sales_fcst[0] * (woc_to_be_maintained + 1)
            if carryover == "CARRYOVER":
                if boh_invt >= first_drop_size_carry:
                    model.Add(model._dv_drop_qty_1[0] == 0)
                    fst_dp_sz = 0
                else:
                    if first_drop_size_carry - boh_invt >= smoq:
                        fst_dp_sz = first_drop_size_carry - boh_invt
                        if (fst_dp_sz > ub_drop_qty) | ((ub_drop_qty - fst_dp_sz) < smoq):
                            model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                            fst_dp_sz = ub_drop_qty
                        else:
                            model.Add(
                                model._dv_drop_qty_1[0]
                                == (first_drop_size_carry - boh_invt)
                            )

                    else:
                        if (ub_drop_qty - smoq) >= smoq:
                            model.Add(model._dv_drop_qty_1[0] == smoq)
                            fst_dp_sz = smoq
                        if (ub_drop_qty - smoq) < smoq:
                            fst_dp_sz = ub_drop_qty
                            model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                            model.Add(
                                sum(
                                    model._dv_drop_qty_1[i]
                                    for i in range(1, num_wks_drops_plan)
                                )
                                == 0
                            )
            else:
                if first_drop_size_nonCarry >= smoq:
                    if (first_drop_size_nonCarry > ub_drop_qty) | (
                        (ub_drop_qty - first_drop_size_nonCarry) < smoq
                    ):
                        model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                        fst_dp_sz = ub_drop_qty

                    else:
                        model.Add(model._dv_drop_qty_1[0] == first_drop_size_nonCarry)
                        fst_dp_sz = first_drop_size_nonCarry

                else:
                    if (ub_drop_qty - smoq) >= smoq:
                        model.Add(model._dv_drop_qty_1[0] == smoq)
                        fst_dp_sz = smoq
                    if (ub_drop_qty - smoq) < smoq:
                        fst_dp_sz = ub_drop_qty
                        model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                        model.Add(
                            sum(
                                model._dv_drop_qty_1[i]
                                for i in range(1, num_wks_drops_plan)
                            )
                            == 0
                        )

        if first_drop_shelf_cap > 0:
            if carryover == "CARRYOVER":
                if boh_invt == 0:

                    model.Add(model._dv_drop_qty_1[0] == first_drop_shelf_cap)
                    fst_dp_sz = first_drop_shelf_cap

                else:
                    initial_woc_boh = (boh_invt - sales_fcst[0]) / sales_fcst[0]
                    if initial_woc_boh >= woc_to_be_maintained:
                        model.Add(model._dv_drop_qty_1[0] == 0)
                        fst_dp_sz = 0

                    elif (initial_woc_boh < woc_to_be_maintained) & (boh_invt > 0):
                        reqd_drop_size = sales_fcst[0] * (woc_to_be_maintained + 1)
                        fst_dp_sz = reqd_drop_size - boh_invt
                        if fst_dp_sz < smoq:
                            if (ub_drop_qty - smoq) >= smoq:
                                model.Add(model._dv_drop_qty_1[0] == smoq)
                                fst_dp_sz = smoq
                            if (ub_drop_qty - smoq) < smoq:
                                fst_dp_sz = ub_drop_qty
                                model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                                model.Add(
                                    sum(
                                        model._dv_drop_qty_1[i]
                                        for i in range(1, num_wks_drops_plan)
                                    )
                                    == 0
                                )
                        else:
                            if (fst_dp_sz > ub_drop_qty) | (
                                (ub_drop_qty - fst_dp_sz) < smoq
                            ):
                                model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                                fst_dp_sz = ub_drop_qty
                                model.Add(
                                    sum(
                                        model._dv_drop_qty_1[i]
                                        for i in range(1, num_wks_drops_plan)
                                    )
                                    == 0
                                )
                            else:
                                model.Add(model._dv_drop_qty_1[0] == fst_dp_sz)

            else:
                if (first_drop_shelf_cap > ub_drop_qty) | (
                    (ub_drop_qty - first_drop_shelf_cap) < smoq
                ):
                    model.Add(model._dv_drop_qty_1[0] == ub_drop_qty)
                    fst_dp_sz = ub_drop_qty
                elif first_drop_shelf_cap < smoq:
                    fst_dp_sz = smoq
                    model.Add(model._dv_drop_qty_1[0] == smoq)
                else:
                    model.Add(model._dv_drop_qty_1[0] == first_drop_shelf_cap)
                    fst_dp_sz = first_drop_shelf_cap
        return model, fst_dp_sz

    # first_drop_size=fst_dp_sz

    def add_constraints_remaining_1(model, input_data_option, first_drop_size):

        num_wks_drops_plan = input_data_option["num_wks_drops_plan"]
        smoq = input_data_option["smoq"]
        ub_drop_qty = input_data_option["ub_drop_qty"]
        num_clearance_weeks = input_data_option["num_clearance_weeks"]
        sales_fcst = input_data_option["sales_fcst"]
        boh_invt = input_data_option["boh_invt"]

        for i in range(num_wks_drops_plan):
            model.Add(model._dv_drop_qty_1[i] == 0).OnlyEnforceIf(
                model._dv_drop_gt_0[i].Not()
            )
            model.Add(model._dv_drop_qty_1[i] > 0).OnlyEnforceIf(model._dv_drop_gt_0[i])

        # summation of all drop sizes <= buy quantity
        
        model.Add(cp.LinearExpr.Sum(model._dv_drop_qty_1) == ub_drop_qty)

        model.Add(model._num_drops == cp.LinearExpr.Sum(model._dv_drop_gt_0))

        model.Add(model._sum_drop_qty == cp.LinearExpr.Sum(model._dv_drop_qty_1))

        # if number of clearance weeks > 0
        # ensure that in thosemany weeks from last there are no drops (drop_size = 0)
        if (num_clearance_weeks > 0) & (num_wks_drops_plan > num_clearance_weeks + 1):

            if (first_drop_size > 0) | (num_wks_drops_plan > (num_clearance_weeks + 1)):
                model.Add(
                    sum(
                        model._dv_drop_qty_1[i]
                        for i in range(
                            num_wks_drops_plan - num_clearance_weeks, num_wks_drops_plan
                        )
                    )
                    == 0
                )
            if (num_wks_drops_plan - num_clearance_weeks - 4 > 0) & (
                (num_wks_drops_plan - num_clearance_weeks - 1) > 4
            ):
                model.Add(
                    model._dv_drop_qty_1[num_wks_drops_plan - num_clearance_weeks - 1]
                    <= smoq
                )
                model.Add(
                    model._dv_drop_qty_1[num_wks_drops_plan - num_clearance_weeks - 2]
                    <= int(1.5 * smoq)
                )
                model.Add(
                    model._dv_drop_qty_1[num_wks_drops_plan - num_clearance_weeks - 3]
                    <= int(1.75 * smoq)
                )
                model.Add(
                    model._dv_drop_qty_1[num_wks_drops_plan - num_clearance_weeks - 4]
                    <= int(2 * smoq)
                )

        # constraint to specify how inventory is calculated
        for i in range(num_wks_drops_plan):
            if i == 0:
                # print(i,boh_invt,model._dv_drop_qty_1[i],sales_fcst[i])
                model.Add(
                    model._dv_invt[i]
                    == (boh_invt + model._dv_drop_qty_1[i] - sales_fcst[i])
                )
            else:
                # print(i,boh_invt,model._dv_drop_qty_1[i],sales_fcst[i])
                model.Add(
                    model._dv_invt[i]
                    == (model._dv_invt[i - 1] + model._dv_drop_qty_1[i] - sales_fcst[i])
                )

        # constraint to calculate weeks of cover
        for i in range(num_wks_drops_plan):
            model.AddMultiplicationEquality(
                model._dv_int_scale[i], [model._dv_invt[i], 1000]
            )
            model.AddDivisionEquality(
                model._dv_woc[i], model._dv_int_scale[i], sales_fcst[i]
            )

        # if inventory at any time period is zero then
        # below constraint will turn the required decision variable = 1

        for i in range(num_wks_drops_plan):
            model.Add(model._dv_woc[i] >= 1500).OnlyEnforceIf(model._dv_invt_zero[i].Not())  #1500/1000(scaling factor) =1.5 WOC
            model.Add(model._dv_woc[i] < 1500).OnlyEnforceIf(model._dv_invt_zero[i])
        return model


    def add_constraints_remaining_2(model, input_data_option):

        num_wks_drops_plan = input_data_option["num_wks_drops_plan"]
        woc_to_be_maintained = input_data_option["woc_to_be_maintained"] * 1000
        model._WOC = input_data_option["woc_to_be_maintained"]
        model._numWeeks = input_data_option["num_wks_drops_plan"]
        num_clearance_weeks = input_data_option["num_clearance_weeks"]
        # below constraints will turn the decsion variable 1
        # if woc < 4 ; else 0
        for i in range(num_wks_drops_plan):
            model.Add(model._dv_woc[i] >= woc_to_be_maintained).OnlyEnforceIf(
                model._dv_woc_lt_4[i].Not()
            )
            model.Add(model._dv_woc[i] < woc_to_be_maintained).OnlyEnforceIf(
                model._dv_woc_lt_4[i]
            )

        if input_data_option["woc_to_be_maintained"] > 4:
            for i in range(num_wks_drops_plan):
                model.Add(model._dv_woc[i] >= 4000).OnlyEnforceIf(model._dv_woc_4[i].Not())  # Scaling * 1000 as ortools can't handle decimal values
                model.Add(model._dv_woc[i] < 4000).OnlyEnforceIf(model._dv_woc_4[i])
        else:
            model.Add(cp.LinearExpr.Sum(model._dv_woc_4) == 0)

        # penalty if inventory hits zero

        model._penalty_invt_zero = sum(
            model._dv_invt_zero[i] for i in range(num_wks_drops_plan - num_clearance_weeks)
        ) * (-100000)

        # penalty if woc goes below 4

        model._penalty_woc_lt_4 = sum(
            model._dv_woc_lt_4[i] for i in range(num_wks_drops_plan - num_clearance_weeks)
        ) * (-100)

        model._penalty_woc_4 = sum(
            model._dv_woc_4[i] for i in range(num_wks_drops_plan - num_clearance_weeks)
        ) * (-300)

        x = (
            num_wks_drops_plan
            if (num_wks_drops_plan - num_clearance_weeks) <= 2
            else (num_wks_drops_plan - num_clearance_weeks)
        )

        # model.AddMaxEquality(model._maxWoc, model._dv_woc[1:])
        model.AddMaxEquality(model._maxWoc, [model._dv_woc[i] for i in range(1, x)])

        # model.AddMinEquality(model._minWoc, model._dv_woc[1:])
        model.AddMinEquality(model._minWoc, [model._dv_woc[i] for i in range(1, x)])

        model.Add(model._maxMinWocDiff == model._maxWoc - model._minWoc)
        model.AddDivisionEquality(model._maxMinWocScale, model._maxMinWocDiff, 1000)

        return model


    def add_constraints_regular_drops(model, input_data_option, first_drop_size):
        num_wks_drops_plan = input_data_option["num_wks_drops_plan"]
        no_straight_zero = input_data_option["no_straight_zero"]
        smoq = input_data_option["smoq"]
        ub_drop_qty = input_data_option["ub_drop_qty"]
        num_clearance_weeks = input_data_option["num_clearance_weeks"]

        temp_var = False
        if (ub_drop_qty - first_drop_size) > int(
            num_wks_drops_plan / no_straight_zero
        ) * smoq:
            temp_var = True

            model._regular_drops = [
                model.NewBoolVar("regular_drops_%i" % i)
                for i in range(
                    num_wks_drops_plan - no_straight_zero + 1 - num_clearance_weeks
                )
            ]

            for i in range(num_wks_drops_plan - no_straight_zero + 1 - num_clearance_weeks):
                _rng = list(range(i, i + no_straight_zero))
                model.AddMaxEquality(
                    model._regular_drops[i], [model._dv_drop_gt_0[j] for j in _rng]
                )

            for i in range(num_wks_drops_plan - no_straight_zero + 1 - num_clearance_weeks):
                model.Add(model._regular_drops[i] >= 1)

        if (not temp_var) & (
            (ub_drop_qty - first_drop_size)
            > (int(num_wks_drops_plan / (no_straight_zero + 1)) * smoq)
        ):
            new_no_straight_zero = no_straight_zero + 1
            temp_var = True

            model._regular_drops = [
                model.NewBoolVar("regular_drops_%i" % i)
                for i in range(
                    num_wks_drops_plan - new_no_straight_zero + 1 - num_clearance_weeks
                )
            ]

            for i in range(
                num_wks_drops_plan - new_no_straight_zero + 1 - num_clearance_weeks
            ):
                _rng = list(range(i, i + new_no_straight_zero))
                model.AddMaxEquality(
                    model._regular_drops[i], [model._dv_drop_gt_0[j] for j in _rng]
                )

            for i in range(
                num_wks_drops_plan - new_no_straight_zero + 1 - num_clearance_weeks
            ):
                model.Add(model._regular_drops[i] >= 1)

        if (not temp_var) & (
            (ub_drop_qty - first_drop_size)
            > (int(num_wks_drops_plan / (no_straight_zero + 2)) * smoq)
        ):
            new_no_straight_zero = no_straight_zero + 2
            temp_var = True

            model._regular_drops = [
                model.NewBoolVar("regular_drops_%i" % i)
                for i in range(
                    num_wks_drops_plan - new_no_straight_zero + 1 - num_clearance_weeks
                )
            ]

            for i in range(
                num_wks_drops_plan - new_no_straight_zero + 1 - num_clearance_weeks
            ):
                _rng = list(range(i, i + new_no_straight_zero))
                model.AddMaxEquality(
                    model._regular_drops[i], [model._dv_drop_gt_0[j] for j in _rng]
                )

            for i in range(
                num_wks_drops_plan - new_no_straight_zero + 1 - num_clearance_weeks
            ):
                model.Add(model._regular_drops[i] >= 1)
        return model


    def build_objective_function(model):

        obj_prim = 95 * (
            (100 * model._num_drops)
            + model._penalty_invt_zero
            + model._penalty_woc_lt_4
            + model._penalty_woc_4
        ) + 5 * (-5 * (30 * model._maxMinWocScale))

        model.Maximize(obj_prim)
        return model


    def solve_optimization_model(model):

        solver = cp.CpSolver()
        solver.parameters.num_search_workers = 8
        # solver.parameters.max_time_in_seconds = 10
        solver.Solve(model)
        # if status == 4:
        #     RUNNER_LOGGER.info("MODEL IS FEASIBLE")
        #     RUNNER_LOGGER.info("Number of drops = " + str(solver.Value(model._num_drops)) + "\n") # noqa: E501
        # else:
        #     RUNNER_LOGGER.error("IMPORTANT !!! MODEL IS INFEASIBLE !!!" + "\n")
        # RUNNER_LOGGER.info(
        #     "COMPLETED FUNCTION : solve_optimization_model(model)" + "\n" + "-" * 100 + "\n" # noqa: E501
        # )
        return model, solver


    def run_optimization_workflow(sales_forecast_data, option_specs, option):
        input_data_option = subset_option_input_data(
            sales_forecast_data, option_specs, option
        )
        model = build_model_object()
        model = create_decision_varbs(model, input_data_option)
        model, first_drop_size = add_constraints_handle_first_drop(model, input_data_option)
        model = add_constraints_remaining_1(model, input_data_option, first_drop_size)
        model = add_constraints_regular_drops(model, input_data_option, first_drop_size)
        model = add_constraints_remaining_2(model, input_data_option)
        model = build_objective_function(model)

        model, solver = solve_optimization_model(model)
        return model, input_data_option, solver

    # opt=option
    def create_drop_schedule_report(
        model, solver, sales_forecast_data, input_data_option, opt
    ):

        num_wks_drops_plan = input_data_option["num_wks_drops_plan"]
        num_clearance_weeks = input_data_option["num_clearance_weeks"]
        sales_fcst = input_data_option["sales_fcst"]
        ub_drop_qty =input_data_option['ub_drop_qty']

        drops = [solver.Value(model._dv_drop_qty_1[i]) for i in range(num_wks_drops_plan)]
        assert model._total_buy_quantity == np.sum(drops)

        primary_colour = input_data_option["primary_colour"]

        drop_schedule = pd.DataFrame()
        drop_schedule["option"] = 0
        drop_schedule["primary_colour"] = 0
        drop_schedule["week_end_date"] = sales_forecast_data[
            sales_forecast_data["option_sales_fcst"] == opt
        ]["week_end_date"].to_list()

        drop_schedule["accounting_pd_wk"] = [
            int(i)
            for i in sales_forecast_data[sales_forecast_data["option_sales_fcst"] == opt][
                "accounting_pd_wk"
            ].to_list()
        ]

        drop_schedule["option"] = 0
        drop_schedule["sales_fcst"] = sales_fcst
        drop_schedule["drop_qty"] = drops
        drop_schedule['ub_drop_qty'] = ub_drop_qty 

        drop_schedule["inventory"] = [
            solver.Value(model._dv_invt[i]) for i in range(num_wks_drops_plan)
        ]

        drop_schedule["weeks_cover"] = [
            solver.Value(model._dv_woc[i]) / 1000 for i in range(num_wks_drops_plan)
        ]

        drop_schedule["option"] = opt
        drop_schedule["primary_colour"] = primary_colour
        drop_schedule["num_drops"] = len([i for i in drops if i > 1])
        drop_schedule["num_weeks"] = num_wks_drops_plan - num_clearance_weeks

        drop_schedule["pct_drops_week"] = (
            drop_schedule["num_drops"] / drop_schedule["num_weeks"]
        )

        if sum(drops) == 0:
            drop_schedule["avg_drop_size"] = 0
            drop_schedule["min_drop_size"] = 0

        else:
            drop_schedule["avg_drop_size"] = [
                round(j, 2) for j in [np.mean([i for i in drops if i > 1])]
            ][0]
            drop_schedule["min_drop_size"] = np.min([i for i in drops if i > 1])
        
        
        
        return drop_schedule
    return create_drop_schedule_report, read_input_data, \
        run_optimization_workflow,input_data_for_optimizer

# if __name__ == "__main__":
#     optimization_func()