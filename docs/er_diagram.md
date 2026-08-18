# Gold star schema — ER diagram

Grain: 1 row per applicant (`CurrId`, i.e. `SK_ID_CURR`) in `fact_application`.
Each dimension is pre-aggregated to `CurrId` grain in `src/lakehouse/gold.py`
(see [`GoldAggregationJob`](../src/lakehouse/gold.py) and its 5 subclasses),
so no fan-out happens when joining `fact_application` to a dimension.

```mermaid
erDiagram
    fact_application ||--o| dim_bureau : "CurrId"
    fact_application ||--o| dim_previous_application : "CurrId"
    fact_application ||--o| dim_installments_agg : "CurrId"
    fact_application ||--o| dim_credit_card_agg : "CurrId"

    fact_application {
        bigint CurrId PK
        bigint Target "nullable - null for TEST sample"
        string SampleTypeCd "TRAIN or TEST"
        decimal IncomeTotalAmt
        decimal CreditAmt
        decimal AnnuityAmt
        string GenderCd
        bigint BirthDays
        bigint EmployedDays "sentinel 365243 nulled in Silver"
        string EducationTypeCd
        string FamilyStatusCd
        string HousingTypeCd
        double ExtSource1Score
        double ExtSource2Score
        double ExtSource3Score
        string ___ "122 renamed application attributes total - see data_dictionary.md"
    }

    dim_bureau {
        bigint CurrId FK
        bigint BureauCnt
        bigint BureauActiveCnt
        bigint BureauClosedCnt
        decimal CreditSumAmtSum
        decimal CreditSumDebtAmtSum
        decimal CreditSumOverdueAmtSum
        decimal CreditMaxOverdueAmtMax
        bigint CreditDayOverdueMax
        bigint CreditProlongCntSum
        bigint CreditDaysMin
        bigint CreditDaysMax
        bigint BbDpdMonthsCntSum
        string BbWorstDpdCdMax
    }

    dim_previous_application {
        bigint CurrId FK
        bigint PrevApplicationCnt
        bigint ApprovedCnt
        bigint RefusedCnt
        bigint CanceledCnt
        decimal CreditAmtSum
        decimal CreditAmtAvg
        decimal AnnuityAmtAvg
        decimal ApplicationAmtSum
        bigint DecisionDaysMin
        bigint DecisionDaysMax
        bigint PosMaxSkDpdMax
        bigint PosDpdMonthsCntSum
    }

    dim_installments_agg {
        bigint CurrId FK
        bigint InstallmentCnt
        bigint LateInstallmentCnt
        bigint ShortPaymentCnt
        decimal PaymentAmtSum
        decimal InstalmentAmtSum
        double DaysLateAvg
        double PaymentRatioAvg
    }

    dim_credit_card_agg {
        bigint CurrId FK
        bigint CreditCardCnt
        decimal BalanceAmtAvg
        decimal BalanceAmtMax
        decimal CreditLimitActualAmtAvg
        decimal CreditLimitActualAmtMax
        double UtilizationRatioAvg
        decimal DrawingsAtmCurrentAmtSum
        bigint SkDpdMax
    }
```

## Why the relationships are optional (`o|`), not `||--||`

`CLAUDE.md`'s architecture summary describes each dimension as joining to
`fact_application` "1:1" — true for row *shape* (no fan-out), but not for row
*coverage*. Each `Gold*Job.transform()` (`src/lakehouse/gold.py`) does a plain
`groupBy("CurrId")` over rows that exist in that satellite table, so an
applicant with **zero** rows in a satellite table gets **no row at all** in
that dimension — not a zero-filled row. (Zero-fill only applies to count/sum
*columns* for a `CurrId` that already has a dimension row but is missing one
piece of it, e.g. bureau history with no `bureau_balance` months — see "Data
quality" in `CLAUDE.md`.)

This is visible directly in the verified row counts (`CLAUDE.md` "Status",
2026-08-15 run):

| Table | Rows | Coverage vs. `fact_application` |
|---|---|---|
| `fact_application` | 356,255 | 100% (356,255 = 307,511 train + 48,744 test) |
| `dim_previous_application` | 338,857 | ~95% — applicants with ≥1 prior Home Credit application |
| `dim_installments_agg` | 336,935 | ~95% — applicants with ≥1 installment payment record |
| `dim_bureau` | 305,811 | ~86% — applicants with ≥1 bureau-reported credit |
| `dim_credit_card_agg` | 92,447 | ~26% — applicants with ≥1 Home Credit credit card |

Practical implication for downstream consumers (Power BI / Databricks SQL):
join `fact_application` to each dimension with a **left join** on `CurrId`,
and treat `NULL` aggregate columns post-join as "no history in that table,"
not missing data.

## Source-table relationships

See [`data_dictionary.md`](data_dictionary.md#table-relationships) for the
Bronze/Silver-level ER diagram of the 8 raw source tables that these 5 Gold
outputs are built from.
