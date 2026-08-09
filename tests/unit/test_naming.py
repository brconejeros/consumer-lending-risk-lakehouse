import pytest

from src.lakehouse.naming import to_silver_column_name, to_silver_table_name


@pytest.mark.parametrize(
    ("raw_name", "expected"),
    [
        ("SK_ID_CURR", "CurrId"),
        ("SK_ID_BUREAU", "BureauId"),
        ("SK_ID_PREV", "PrevId"),
        ("AMT_INCOME_TOTAL", "IncomeTotalAmt"),
        ("AMT_CREDIT", "CreditAmt"),
        ("CNT_CHILDREN", "ChildrenCnt"),
        ("FLAG_OWN_CAR", "OwnCarFlg"),
        ("CODE_GENDER", "GenderCd"),
        ("NAME_CONTRACT_TYPE", "ContractTypeCd"),
        ("DAYS_BIRTH", "BirthDays"),
        ("APARTMENTS_AVG", "ApartmentsAvg"),
        ("FONDKAPREMONT_MODE", "FondkapremontMode"),
        ("YEARS_BEGINEXPLUATATION_MEDI", "YearsBeginexpluatationMedi"),
    ],
)
def test_prefix_and_stat_suffix_rules(raw_name, expected):
    assert to_silver_column_name(raw_name) == expected


def test_override_wins_over_prefix_rules():
    assert to_silver_column_name("SK_ID_CURR", overrides={"SK_ID_CURR": "ApplicationId"}) == (
        "ApplicationId"
    )


def test_unmapped_column_falls_back_to_plain_pascal_case():
    assert to_silver_column_name("TARGET") == "Target"
    assert to_silver_column_name("MONTHS_BALANCE") == "MonthsBalance"


@pytest.mark.parametrize(
    ("raw_table", "expected"),
    [
        ("application_train", "tb_application_train"),
        ("application_test", "tb_application_test"),
        ("bureau", "tb_bureau"),
        ("bureau_balance", "tb_bureau_balance"),
        ("previous_application", "tb_previous_application"),
        ("POS_CASH_balance", "tb_pos_cash_balance"),
        ("installments_payments", "tb_installments_payments"),
        ("credit_card_balance", "tb_credit_card_balance"),
    ],
)
def test_table_names(raw_table, expected):
    assert to_silver_table_name(raw_table) == expected
