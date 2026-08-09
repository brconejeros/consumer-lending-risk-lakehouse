"""Silver-onward naming convention: PascalCase columns with a type suffix,
`tb_`-prefixed snake_case tables. Bronze keeps the source dataset's raw
casing (see CLAUDE.md) - these rules only apply from Silver on.

Pure Python, no Spark import - callable identically from local tests,
Databricks Connect tests, or a notebook running on a real cluster.
"""

from __future__ import annotations

PREFIX_SUFFIX_RULES: dict[str, str] = {
    "SK_ID_": "Id",
    "AMT_": "Amt",
    "CNT_": "Cnt",
    "FLAG_": "Flg",
    "CODE_": "Cd",
    "NAME_": "Cd",
    "DAYS_": "Days",
}

STAT_SUFFIXES: tuple[str, ...] = ("AVG", "MODE", "MEDI")


def _pascal_case(raw: str) -> str:
    return "".join(part.capitalize() for part in raw.strip("_").split("_") if part)


def to_silver_column_name(raw_name: str, overrides: dict[str, str] | None = None) -> str:
    """PascalCase + type suffix for one raw (Bronze-casing) column name.

    Checks `overrides` first (per-table exceptions), then the `_AVG`/`_MODE`/
    `_MEDI` stat suffixes, then the prefix->suffix rules (longest prefix
    wins). Falls back to a plain PascalCase of the whole name rather than
    raising, so an unmapped column still gets a usable name pending an
    explicit override.
    """
    if overrides and raw_name in overrides:
        return overrides[raw_name]

    for stat in STAT_SUFFIXES:
        suffix = f"_{stat}"
        if raw_name.endswith(suffix):
            return _pascal_case(raw_name[: -len(suffix)]) + stat.capitalize()

    for prefix in sorted(PREFIX_SUFFIX_RULES, key=len, reverse=True):
        if raw_name.startswith(prefix):
            return _pascal_case(raw_name[len(prefix) :]) + PREFIX_SUFFIX_RULES[prefix]

    return _pascal_case(raw_name)


def to_silver_table_name(raw_table: str) -> str:
    """`tb_`-prefixed, all-lowercase, underscore-separated table name."""
    return "tb_" + "_".join(part.lower() for part in raw_table.split("_") if part)
