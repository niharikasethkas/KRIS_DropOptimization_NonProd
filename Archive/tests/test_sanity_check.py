import math


def test_planner_equals_npdf(planner_sales, npdf_forecast):
    check_sales_fcst_match = planner_sales.merge(npdf_forecast)
    assert all(
        [
            math.isclose(i[0], i[1], rel_tol=1)
            for i in zip(
                check_sales_fcst_match["total_sales_fcst"],
                check_sales_fcst_match["sales_forecast"],
            )
        ]
    )

    assert len(planner_sales["option_sales_fcst"]) == len(
        npdf_forecast["option_sales_fcst"]
    )


def test_planner_equals_adj(planner_sales, adjusted_forecast):
    check_sales_fcst_match = planner_sales.merge(adjusted_forecast)
    assert all(
        [
            math.isclose(i[0], i[1], rel_tol=1)
            for i in zip(
                check_sales_fcst_match["total_sales_fcst"],
                check_sales_fcst_match["sales_forecast"],
            )
        ]
    )


# pytest.main()
