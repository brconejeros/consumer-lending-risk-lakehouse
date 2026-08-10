# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: what matters in each Bronze table
# MAGIC
# MAGIC Exploratory, not a pipeline stage - answers "what do we actually know
# MAGIC about this table, and why does it matter for predicting default?" for
# MAGIC each of the 8 Bronze tables before `01_silver_transform.py` conforms
# MAGIC them. Every section below covers: what the table is and its grain, why
# MAGIC it matters to the business problem, which columns carry the strongest
# MAGIC predictive signal, and any data-quality quirks worth knowing about
# MAGIC before renaming/deduping/checking referential integrity.
# MAGIC
# MAGIC See `docs/data_dictionary.md` for the full column-by-column reference -
# MAGIC this notebook is the "why it matters" companion to that "what it is"
# MAGIC reference, not a replacement for it.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import functions as F

CATALOG = "consumer_lending_risk_lakehouse"


def null_rate(df, columns):
    """Fraction of nulls per column, for a quick data-quality spot check."""
    return df.select(
        [F.round(F.mean(F.col(c).isNull().cast("int")), 4).alias(c) for c in columns]
    )


def fk_orphan_count(child_df, child_col, parent_df, parent_col):
    """Rows in `child_df` whose `child_col` has no match in
    `parent_df.parent_col` - what `SilverTransformJob.validate()` will raise
    on if this is ever non-zero."""
    return child_df.join(
        parent_df, child_df[child_col] == parent_df[parent_col], "left_anti"
    ).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## application_train / application_test
# MAGIC
# MAGIC **What it is:** the main table - one row per loan application, with the
# MAGIC applicant's demographics, income, employment, housing, and a set of
# MAGIC normalized building/region statistics. `application_train` carries
# MAGIC `TARGET` (1 = payment difficulties), `application_test` doesn't - it's
# MAGIC the held-out sample.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_CURR`, the primary key every other table
# MAGIC joins back to.
# MAGIC
# MAGIC **Business relevance:** this *is* the applicant snapshot at the moment
# MAGIC of application - the "no robust bank history" thin-file signal the whole
# MAGIC project is about comes from here (short `DAYS_EMPLOYED`, low
# MAGIC `EXT_SOURCE_*` coverage) combined with the bureau/previous-application
# MAGIC history tables.

# COMMAND ----------

application_train = spark.table(f"{CATALOG}.bronze.application_train")
application_test = spark.table(f"{CATALOG}.bronze.application_test")

for name, df in [("application_train", application_train), ("application_test", application_test)]:
    print(f"{name}: rows={df.count()}, distinct SK_ID_CURR={df.select('SK_ID_CURR').distinct().count()}")

display(application_train.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:**
# MAGIC - `EXT_SOURCE_1`, `EXT_SOURCE_2`, `EXT_SOURCE_3` - normalized scores from
# MAGIC   external data sources; consistently the strongest individual
# MAGIC   predictors of `TARGET` in this dataset, which is exactly why they get
# MAGIC   their own `Score` suffix in Silver rather than falling under the
# MAGIC   generic amount/code rules.
# MAGIC - `DAYS_BIRTH`, `DAYS_EMPLOYED` - age and job tenure; short employment
# MAGIC   relative to age is a classic thin-file instability signal.
# MAGIC - `AMT_CREDIT` / `AMT_ANNUITY` / `AMT_INCOME_TOTAL` - credit-to-income
# MAGIC   and annuity-to-income ratios (computed in Gold) are standard
# MAGIC   affordability signals.
# MAGIC - `NAME_EDUCATION_TYPE`, `NAME_INCOME_TYPE`, `OCCUPATION_TYPE` - stable
# MAGIC   categorical risk segments.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `EXT_SOURCE_1` has the highest null rate of the three (checked below) -
# MAGIC   expect Gold-layer imputation/handling, not a Silver-layer job.
# MAGIC - `AMT_REQ_CREDIT_BUREAU_{HOUR,DAY,WEEK,MON,QRT,YEAR}` are **counts** of
# MAGIC   bureau enquiries despite the `AMT_` prefix - a real naming quirk in
# MAGIC   the source dataset. The mechanical Silver naming rule would mislabel
# MAGIC   these `...Amt`; `01_silver_transform.py` overrides them to `...Cnt`.
# MAGIC - All `DAYS_*` columns are negative day-counts relative to the
# MAGIC   application date, not calendar dates - `DAYS_EMPLOYED` in particular
# MAGIC   has a well-known sentinel value (365243) for "not currently employed"
# MAGIC   that isn't a real day count.

# COMMAND ----------

display(null_rate(application_train, ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "OCCUPATION_TYPE"]))

print("DAYS_EMPLOYED sentinel (365243) rows:", application_train.filter(F.col("DAYS_EMPLOYED") == 365243).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## bureau
# MAGIC
# MAGIC **What it is:** every credit the applicant had reported to the Credit
# MAGIC Bureau by other institutions, as of the application date - not just
# MAGIC Home Credit's own history.
# MAGIC
# MAGIC **Grain:** one row per previous Credit Bureau credit
# MAGIC (`SK_ID_BUREAU`, unique per application); an applicant can have 0, 1, or
# MAGIC many rows.
# MAGIC
# MAGIC **Business relevance:** this is the "outside" credit history a thin-file
# MAGIC applicant may or may not have - active vs. closed credit mix, overdue
# MAGIC amounts, and how many institutions they've borrowed from are all
# MAGIC classic bureau-style risk signals, independent of anything Home Credit
# MAGIC has observed directly.

# COMMAND ----------

bureau = spark.table(f"{CATALOG}.bronze.bureau")
print(f"rows={bureau.count()}, distinct SK_ID_BUREAU={bureau.select('SK_ID_BUREAU').distinct().count()}")
display(bureau.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:**
# MAGIC - `CREDIT_ACTIVE` - active vs. closed vs. sold credit mix.
# MAGIC - `AMT_CREDIT_SUM_OVERDUE` / `CREDIT_DAY_OVERDUE` - direct delinquency
# MAGIC   signal from another lender.
# MAGIC - `CNT_CREDIT_PROLONG` - how often a credit was extended/renegotiated.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `CREDIT_ACTIVE`, `CREDIT_CURRENCY`, `CREDIT_TYPE` are coded categorical
# MAGIC   fields with no `NAME_`/`CODE_` prefix, so the mechanical naming rule
# MAGIC   wouldn't add a `Cd` suffix automatically - overridden explicitly in
# MAGIC   `01_silver_transform.py`.
# MAGIC - The source CSV header is `SK_ID_BUREAU`; `docs/data_dictionary.md`
# MAGIC   labels it `SK_BUREAU_ID` in its column tables (footnoted) - Bronze/
# MAGIC   Silver both use the real `SK_ID_BUREAU`.

# COMMAND ----------

display(null_rate(bureau, ["CREDIT_ACTIVE", "AMT_CREDIT_SUM_OVERDUE", "DAYS_CREDIT_ENDDATE"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## bureau_balance
# MAGIC
# MAGIC **What it is:** monthly balance snapshots for each `bureau` credit -
# MAGIC the payment-status history behind each bureau-reported credit line.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_BUREAU` + `MONTHS_BALANCE` (month offset
# MAGIC from the application date).
# MAGIC
# MAGIC **Business relevance:** turns `bureau`'s point-in-time snapshot into a
# MAGIC trend - months of consecutive DPD (`STATUS` buckets `1`-`5`) is a much
# MAGIC stronger signal than a single overdue-amount field, and is exactly the
# MAGIC kind of history a thin-file-at-Home-Credit applicant might still have
# MAGIC elsewhere.

# COMMAND ----------

bureau_balance = spark.table(f"{CATALOG}.bronze.bureau_balance")
print(f"rows={bureau_balance.count()}")
display(bureau_balance.groupBy("STATUS").count().orderBy("STATUS"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `STATUS` (DPD bucket per month) is
# MAGIC essentially the whole table's value - Gold will aggregate it into
# MAGIC something like "months at DPD 60+" or "worst DPD bucket observed" per
# MAGIC applicant.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `STATUS` is a coded field with no prefix - overridden to `StatusCd`.
# MAGIC - This is the table CLAUDE.md's "Data quality" FK check is written for:
# MAGIC   every `SK_ID_BUREAU` here must exist in `bureau`. Spot-checked below
# MAGIC   against the real Bronze data - `01_silver_transform.py` enforces this
# MAGIC   for every run via `FkCheck`.

# COMMAND ----------

orphans = fk_orphan_count(bureau_balance, "SK_ID_BUREAU", bureau, "SK_ID_BUREAU")
print(f"bureau_balance rows with no matching bureau.SK_ID_BUREAU: {orphans}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## previous_application
# MAGIC
# MAGIC **What it is:** every prior application the client made *to Home
# MAGIC Credit itself* - approved, cancelled, refused, or unused offer - not
# MAGIC just disbursed credit.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV`.
# MAGIC
# MAGIC **Business relevance:** this is Home Credit's own first-party memory of
# MAGIC the applicant - prior approval/refusal patterns and `CODE_REJECT_REASON`
# MAGIC are a direct signal the bureau tables can't provide, and it's the
# MAGIC parent table for POS/credit-card/installment history below.

# COMMAND ----------

previous_application = spark.table(f"{CATALOG}.bronze.previous_application")
print(f"rows={previous_application.count()}, distinct SK_ID_PREV={previous_application.select('SK_ID_PREV').distinct().count()}")
display(previous_application.groupBy("NAME_CONTRACT_STATUS").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:**
# MAGIC - `NAME_CONTRACT_STATUS` - was the prior application approved/refused/
# MAGIC   cancelled; refusal history is a strong repeat signal.
# MAGIC - `CODE_REJECT_REASON` - *why* it was refused, when applicable.
# MAGIC - `DAYS_DECISION` - recency of the prior application.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `NFLAG_LAST_APPL_IN_DAY`, `NFLAG_MICRO_CASH`, `NFLAG_INSURED_ON_APPROVAL`
# MAGIC   are boolean flags prefixed `NFLAG_`, not `FLAG_` - the mechanical rule
# MAGIC   only matches `FLAG_`, so these silently got no `Flg` suffix until
# MAGIC   overridden explicitly.
# MAGIC - `CHANNEL_TYPE` and `PRODUCT_COMBINATION` are categorical with no
# MAGIC   `NAME_`/`CODE_` prefix - overridden to `Cd`.
# MAGIC - This is the parent table for the FK checks on `POS_CASH_balance`,
# MAGIC   `credit_card_balance`, and `installments_payments` below.

# COMMAND ----------

display(null_rate(previous_application, ["CODE_REJECT_REASON", "AMT_DOWN_PAYMENT", "PRODUCT_COMBINATION"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## POS_CASH_balance
# MAGIC
# MAGIC **What it is:** monthly balance snapshots for the client's previous
# MAGIC point-of-sale and cash loans with Home Credit.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV` + `MONTHS_BALANCE`.
# MAGIC
# MAGIC **Business relevance:** `SK_DPD`/`SK_DPD_DEF` trended over months gives
# MAGIC the same "how did they actually behave over time" signal `bureau_balance`
# MAGIC gives for outside credit, but for Home Credit's own POS/cash products
# MAGIC specifically.

# COMMAND ----------

pos_cash_balance = spark.table(f"{CATALOG}.bronze.POS_CASH_balance")
print(f"rows={pos_cash_balance.count()}")
orphans = fk_orphan_count(pos_cash_balance, "SK_ID_PREV", previous_application, "SK_ID_PREV")
print(f"POS_CASH_balance rows with no matching previous_application.SK_ID_PREV: {orphans}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `SK_DPD` / `SK_DPD_DEF` (days past due, with
# MAGIC and without a small-balance tolerance) and `NAME_CONTRACT_STATUS`
# MAGIC trended across months.
# MAGIC
# MAGIC **Data-quality watch-outs:** none needed beyond the automatic naming
# MAGIC rules - every column here maps cleanly (`NAME_CONTRACT_STATUS`→`Cd`,
# MAGIC `CNT_INSTALMENT*`→`Cnt`, `SK_ID_PREV`/`SK_ID_CURR`→`Id`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## credit_card_balance
# MAGIC
# MAGIC **What it is:** monthly balance snapshots for the client's previous
# MAGIC Home Credit credit cards.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV` + `MONTHS_BALANCE`.
# MAGIC
# MAGIC **Business relevance:** revolving-credit behavior (drawings vs. limit,
# MAGIC minimum-payment-only patterns via `AMT_PAYMENT_CURRENT` vs.
# MAGIC `AMT_INST_MIN_REGULARITY`) is a different risk signal than the
# MAGIC installment-loan behavior the other tables capture.

# COMMAND ----------

credit_card_balance = spark.table(f"{CATALOG}.bronze.credit_card_balance")
print(f"rows={credit_card_balance.count()}")
orphans = fk_orphan_count(credit_card_balance, "SK_ID_PREV", previous_application, "SK_ID_PREV")
print(f"credit_card_balance rows with no matching previous_application.SK_ID_PREV: {orphans}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `AMT_BALANCE` relative to
# MAGIC `AMT_CREDIT_LIMIT_ACTUAL` (utilization), and `AMT_DRAWINGS_ATM_CURRENT`
# MAGIC (cash-advance usage is a common risk flag).
# MAGIC
# MAGIC **Data-quality watch-outs:** `AMT_RECIVABLE` is spelled that way in the
# MAGIC source CSV (missing the second "E") - kept as-is for traceability; the
# MAGIC mechanical rule still produces a correct `RecivableAmt`. Otherwise every
# MAGIC column maps cleanly - no overrides needed for this table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## installments_payments
# MAGIC
# MAGIC **What it is:** actual repayment history - one row per installment
# MAGIC payment made (or missed) against a previous Home Credit credit.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV` + `NUM_INSTALMENT_NUMBER`.
# MAGIC
# MAGIC **Business relevance:** the most direct behavioral signal in the whole
# MAGIC dataset - `DAYS_ENTRY_PAYMENT` vs. `DAYS_INSTALMENT` (paid late or on
# MAGIC time?) and `AMT_PAYMENT` vs. `AMT_INSTALMENT` (paid in full or short?)
# MAGIC is literally "did this person pay what they owed, when they owed it,"
# MAGIC for every installment Home Credit has ever billed them.

# COMMAND ----------

installments_payments = spark.table(f"{CATALOG}.bronze.installments_payments")
print(f"rows={installments_payments.count()}")
orphans = fk_orphan_count(installments_payments, "SK_ID_PREV", previous_application, "SK_ID_PREV")
print(f"installments_payments rows with no matching previous_application.SK_ID_PREV: {orphans}")

late_or_short = installments_payments.filter(
    (F.col("DAYS_ENTRY_PAYMENT") > F.col("DAYS_INSTALMENT"))
    | (F.col("AMT_PAYMENT") < F.col("AMT_INSTALMENT"))
).count()
print(f"payments late and/or short of the prescribed amount: {late_or_short}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `DAYS_ENTRY_PAYMENT` - `DAYS_INSTALMENT`
# MAGIC (days late) and `AMT_PAYMENT` / `AMT_INSTALMENT` (fraction paid) -
# MAGIC both computed in Gold from this table's raw columns.
# MAGIC
# MAGIC **Data-quality watch-outs:** `NUM_INSTALMENT_VERSION`/
# MAGIC `NUM_INSTALMENT_NUMBER` don't match any prefix rule (`NUM_` isn't one of
# MAGIC the rule prefixes) - overridden to `InstalmentVersionCd`/
# MAGIC `InstalmentNumberCnt` since they're this table's grain keys and are
# MAGIC worth naming deliberately rather than leaving to the plain-PascalCase
# MAGIC fallback.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Findings that turned directly into `01_silver_transform.py` decisions:
# MAGIC - `application_{train,test}`: `EXT_SOURCE_*`→`Score`,
# MAGIC   `AMT_REQ_CREDIT_BUREAU_*`→`Cnt` (fixes a real `AMT_`-prefix miscount).
# MAGIC - `bureau`: `CREDIT_ACTIVE`/`CREDIT_CURRENCY`/`CREDIT_TYPE`→`Cd`.
# MAGIC - `bureau_balance`: `STATUS`→`Cd`; FK check against `bureau.SK_ID_BUREAU`.
# MAGIC - `previous_application`: `NFLAG_*`→`Flg` (fixes a real `FLAG_`-prefix
# MAGIC   miss), `CHANNEL_TYPE`/`PRODUCT_COMBINATION`→`Cd`.
# MAGIC - `POS_CASH_balance`/`credit_card_balance`: no overrides needed; FK
# MAGIC   checks against `previous_application.SK_ID_PREV`.
# MAGIC - `installments_payments`: `NUM_INSTALMENT_VERSION`→`Cd`,
# MAGIC   `NUM_INSTALMENT_NUMBER`→`Cnt`; FK check against
# MAGIC   `previous_application.SK_ID_PREV`.
# MAGIC
# MAGIC All the orphan counts above are expected to be `0` against real Bronze
# MAGIC data (Home Credit's own dataset is internally consistent) - if
# MAGIC `01_silver_transform.py`'s `validate()` ever raises on one of these
# MAGIC checks, that's a real regression worth investigating, not a false
# MAGIC positive.
