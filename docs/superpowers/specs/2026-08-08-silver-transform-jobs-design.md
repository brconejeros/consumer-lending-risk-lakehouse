# Silver-layer transform jobs

## Purpose

Build the Silver layer of the medallion pipeline: 8 per-table notebooks
(mirroring Bronze's pattern) that read from `bronze`, clean/conform the data,
and write to `silver`, backed by a new shared `SilverTransformJob` class in
`src/lakehouse/`. This supersedes CLAUDE.md's current "Repo layout" wording,
which describes `01_silver_transform.py` as a single notebook covering the
whole layer — that description is now wrong and gets corrected as part of
this work.

## Scope decisions

These were settled through discussion before writing this spec; recorded
here so the "why" isn't lost:

- **Per-table, not single-notebook.** Silver becomes 8 notebooks
  (`notebooks/silver/<table>.py`), one class (`SilverTransformJob`),
  config-driven per table — same shape as Bronze's `BronzeIngestionJob`.
- **Transform depth: targeted, not exhaustive.** Handle the concrete,
  documented issues (sentinel values, de-duplication, referential integrity)
  rather than a bespoke rule for every column across all 8 tables.
- **Referential integrity: drop, don't fail.** `transform()` silently drops
  orphan rows (child row whose FK doesn't resolve in the parent); `validate()`
  then confirms zero orphans remain as a sanity net, not a normal-case path.
  Only the two relationships CLAUDE.md's "Data quality" section names
  explicitly are enforced: `bureau_balance` → `bureau` (`SK_ID_BUREAU`), and
  `{POS_CASH_balance,credit_card_balance,installments_payments}` →
  `previous_application` (`SK_ID_PREV`). The `SK_ID_CURR` dual-path FK
  (`bureau`/`previous_application`/etc. → `application_train`/`test`) is
  **not** enforced here — flagged as a follow-up, not built now.
- **De-duplication: exact full-row duplicates only.** `dropDuplicates()`
  with no subset — unambiguous, no tie-breaking logic needed. Rows sharing a
  key but differing elsewhere are left alone (no defined rule for which one
  would "win").
- **Column/table renaming: Silver only, Bronze unchanged.** Bronze keeps
  the original source-CSV casing (`SK_ID_CURR`, `AMT_INCOME_TOTAL`) as the
  traceability anchor, per CLAUDE.md's existing naming-convention rationale.
  Silver introduces a new "conformed" naming convention as part of becoming
  the cleaned/conformed layer (see below) — this doesn't contradict the
  existing rule, it scopes it to Bronze specifically, and CLAUDE.md's
  "Conventions" section gets a note clarifying that.
- **`DAYS_*` columns get the `Cnt` class word, not `Dt`.** They're
  day-offsets relative to the application date (explicitly documented as
  "time only relative to the application"), not real calendar dates — a
  `Dt` suffix would misleadingly imply an actual date exists.

## Naming convention (Silver only)

### Tables: `tb_<table_name>`, all lowercase

| Bronze | Silver |
|---|---|
| `application_train` | `tb_application_train` |
| `application_test` | `tb_application_test` |
| `bureau` | `tb_bureau` |
| `bureau_balance` | `tb_bureau_balance` |
| `previous_application` | `tb_previous_application` |
| `POS_CASH_balance` | `tb_pos_cash_balance` |
| `credit_card_balance` | `tb_credit_card_balance` |
| `installments_payments` | `tb_installments_payments` |

### Columns: PascalCase business term + class-word suffix

Also known as "class word"/"qualifier" suffixes (ISO 11179-style data-naming
convention): strip the source prefix that signals the value's kind,
PascalCase the remainder, append a short suffix naming what kind of value it
is.

| Class word | Meaning | Source pattern | Example |
|---|---|---|---|
| `Id` | identifier | `SK_ID_*` | `SK_ID_BUREAU` → `BureauCreditId` |
| `Amt` | monetary amount | `AMT_*` (when actually a monetary value — see gotcha below) | `AMT_INCOME_TOTAL` → `IncomeTotalAmt` |
| `Cnt` | count | `CNT_*`, `DAYS_*` (day-offset, not a date), `SK_DPD`/`SK_DPD_DEF` | `CNT_CHILDREN` → `ChildrenCnt`, `DAYS_BIRTH` → `BirthDaysCnt` |
| `Flg` | boolean flag | `FLAG_*`, `NFLAG_*`, the `REG_*_NOT_*`/`LIVE_*_NOT_*` comparison flags | `FLAG_OWN_CAR` → `OwnCarFlg` |
| `Cd` | categorical code | `NAME_*` (categorical, not free text), `CODE_*`, other enum-like fields (`STATUS`, `ORGANIZATION_TYPE`, `CREDIT_ACTIVE`, ...) | `NAME_CONTRACT_TYPE` → `ContractTypeCd`, `CODE_GENDER` → `GenderCd` |
| `Rt` | rate/ratio | `RATE_*`, `EXT_SOURCE_*` | `RATE_DOWN_PAYMENT` → `DownPaymentRt` |

**Not mechanical prefix-matching — checked against the column's actual
documented meaning** in `docs/data_dictionary.md`. Concrete gotcha already
found: `AMT_REQ_CREDIT_BUREAU_HOUR`/`_DAY`/`_WEEK`/`_MON`/`_QRT`/`_YEAR` all
carry the `AMT_` prefix but are documented as *counts* of Credit Bureau
enquiries, not monetary amounts — these get `Cnt`, not `Amt`
(`AMT_REQ_CREDIT_BUREAU_HOUR` → `ReqCreditBureauHourCnt`). Every column gets
this same check during implementation, not a blind regex pass.

The complete column-by-column mapping for all 8 tables gets generated during
implementation (in `writing-plans`/execution), grounded in
`docs/data_dictionary.md`'s per-column descriptions — not enumerated here,
since a spec documents the rule and rationale, not ~220 literal mappings.

## Architecture

### 1. Shared base-class refactor (`src/lakehouse/base.py`)

Bronze's `load()` currently inlines "write Delta full-overwrite + log row
count via `DESCRIBE HISTORY`." Silver needs the identical pattern, just a
different target schema/table. Promote it into `LakehouseLayerJob` as two
protected helpers, generalized to take `target_table` as a parameter instead
of reading `self.config` directly:

```python
def _write_delta_overwrite(self, df: DataFrame, target_table: str) -> None:
    """Full-overwrite write to `target_table` as Delta, logging the row
    count from Delta's commit metrics rather than an extra df.count() pass."""

def _last_write_row_count(self, target_table: str) -> int | None:
    """Row count of the last write to `target_table`, from DESCRIBE HISTORY."""
```

`BronzeIngestionJob.load()` shrinks to:

```python
def load(self, df: DataFrame) -> None:
    self._write_delta_overwrite(df, self.config.target_table)
```

Its own `_last_write_row_count` gets deleted (inherited from base now).
`BronzeTableConfig` is unchanged otherwise. Existing Bronze tests
(`tests/test_bronze.py`) should pass unmodified — this is an internal
refactor, not a behavior change.

### 2. New `src/lakehouse/silver.py`

```python
@dataclass(frozen=True)
class SilverTableConfig:
    table: str                          # Bronze source table name (original casing)
    silver_table: str                   # tb_<name> target table name
    catalog: str = "consumer_lending_risk_lakehouse"
    source_schema: str = "bronze"
    column_renames: dict[str, str] = field(default_factory=dict)  # {source: ClassWordName}
    sentinel_value: int | None = None
    sentinel_columns: tuple[str, ...] = ()
    parent_table: str | None = None     # Bronze table name (original casing)
    parent_key: str | None = None       # source column name (pre-rename)
    parent_schema: str = "bronze"

    @property
    def source_table(self) -> str:
        return f"{self.catalog}.{self.source_schema}.{self.table}"

    @property
    def target_table(self) -> str:
        return f"{self.catalog}.silver.{self.silver_table}"

    @property
    def parent_source_table(self) -> str | None:
        ...  # f"{catalog}.{parent_schema}.{parent_table}" or None


class SilverTransformJob(LakehouseLayerJob):
    layer = "silver"

    def extract(self) -> DataFrame:
        ...  # spark.table(config.source_table)

    def transform(self, df: DataFrame) -> DataFrame:
        df = self._null_sentinels(df)      # sentinel_value in sentinel_columns -> null
        df = df.dropDuplicates()           # exact full-row duplicates only
        df = self._drop_orphans(df)        # left_semi join against parent's distinct keys
        df = df.withColumnsRenamed(self.config.column_renames)  # last: renaming after logic
        return df

    def validate(self, df: DataFrame) -> None:
        ...  # raise if exact duplicates or (renamed) orphan key still present

    def load(self, df: DataFrame) -> None:
        self._write_delta_overwrite(df, self.config.target_table)
```

Key ordering choice: **renaming happens last**, after sentinel-nulling,
dedup, and orphan-dropping, which all reference original (Bronze) column
names — `sentinel_columns`, `parent_key` etc. in `SilverTableConfig` are
specified in *source* casing throughout, so config authors don't have to
mentally translate between two naming schemes when wiring up a table.
`validate()` runs against the already-renamed frame, so its checks reference
the parent's *source* key column (queried fresh from Bronze) joined against
the child's *renamed* key column — handled internally by the job, not
something each table's notebook needs to know about.

### 3. Per-table config

| Table | `sentinel_columns` | `parent_table` / `parent_key` |
|---|---|---|
| `application_train` | `DAYS_EMPLOYED` | — |
| `application_test` | `DAYS_EMPLOYED` | — |
| `bureau` | — | — |
| `bureau_balance` | — | `bureau` / `SK_ID_BUREAU` |
| `previous_application` | `DAYS_FIRST_DRAWING` | — |
| `POS_CASH_balance` | — | `previous_application` / `SK_ID_PREV` |
| `credit_card_balance` | — | `previous_application` / `SK_ID_PREV` |
| `installments_payments` | — | `previous_application` / `SK_ID_PREV` |

(`sentinel_value=365243` for both rows that have sentinel columns.)
`column_renames` is the full per-table mapping generated during
implementation per the naming convention above.

### 4. Notebooks: `notebooks/silver/<table>.py` × 8

Same shape as Bronze's, coexisting with the `.sql` profiling notebooks
already in that folder (different extensions, same directory — matches the
original intent of keeping profiling and transform work together per table):

```python
# Databricks notebook source
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from src.lakehouse.silver import SilverTransformJob, SilverTableConfig

SilverTransformJob(spark, SilverTableConfig(
    table="bureau_balance",
    silver_table="tb_bureau_balance",
    parent_table="bureau",
    parent_key="SK_ID_BUREAU",
    column_renames={...},  # full mapping, generated during implementation
)).run()
```

### 5. Tests

`tests/test_silver.py`, same real local Spark+Delta fixture as
`test_bronze.py`: config derivation (`source_table`/`target_table`
properties, `tb_` naming), extract, sentinel-nulling, dedup, orphan-dropping,
column renaming, `validate()` raising on each failure mode (duplicates
surviving, orphans surviving), full `run()` integration test. Plus one new
test in `test_base.py` for the shared `_write_delta_overwrite` helper.

### 6. CLAUDE.md updates

- "Repo layout": add a `silver/<table>.py × 8` bullet (mirroring Bronze's
  wording); the current line bundling `01_silver_transform.py` in with
  `02_gold_aggregation.py`/`03_quality_checks.py` as "single notebooks" gets
  split so only Gold/quality-checks keep that description.
- "Conventions": the naming-convention bullet ("Table/column naming stays in
  the source dataset's original casing... for traceability") gets a
  clarifying note that this governs Bronze specifically; Silver uses a
  PascalCase + class-word convention (`tb_`-prefixed tables, e.g.
  `BureauCreditId`) as part of becoming the conformed layer.

## Out of scope

- The `SK_ID_CURR` dual-path FK check (bureau/previous_application/etc. →
  application) — not enforced in this pass, noted as a follow-up.
- Type casting/standardization beyond sentinel-nulling — nothing in the data
  dictionary or profiling so far points at a concrete casting need.
- Creating the actual Databricks Job that runs these 8 notebooks (manual
  Databricks-side step, like `bronze_ingestion` was) — left for the user to
  do when ready to run this in Databricks.
- Renaming Bronze tables/columns — explicitly out of scope per the scope
  decision above.
