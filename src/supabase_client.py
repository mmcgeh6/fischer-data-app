"""
Fischer Data App – Supabase Client
===================================
Handles transformation of wide DataFrames to long (EAV) format and
bulk upsert to Supabase Postgres via the supabase-py client.

Tables written:
  • combined_raw      – minute-level merged sensor readings
  • resampled_15min   – quarter-hour resampled readings with quality flags

Both tables are keyed on (bbl, timestamp, sensor_name) so re-processing
the same file safely overwrites existing rows.
"""

import os
import re
import logging
from typing import Optional, Callable

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLE_RAW = "combined_raw"
TABLE_RESAMPLED = "resampled_15min"
BATCH_SIZE = 1000

# Columns that exist in the wide DataFrames but are NOT sensor readings.
# These are excluded from the melt so they don't produce spurious sensor rows.
_META_COLS = frozenset(
    {"Date", "Stale_Data_Flag", "Stale_Sensors", "Zero_Value_Flag"}
)


# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

def get_supabase_client():
    """
    Initialise and return a Supabase client using env vars.

    Returns None (with a logged warning) when credentials are absent so that
    callers can gracefully skip the DB step rather than raising at import time.
    """
    try:
        from supabase import create_client  # deferred so app works without pkg
    except ImportError:
        logger.warning(
            "supabase-py is not installed. "
            "Run `pip install supabase` to enable database writes."
        )
        return None

    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()

    if not url or not key:
        logger.warning(
            "SUPABASE_URL or SUPABASE_SERVICE_KEY not set in .env – "
            "database writes will be skipped."
        )
        return None

    try:
        return create_client(url, key)
    except Exception as exc:
        logger.error("Failed to create Supabase client: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sensor_columns(df: pd.DataFrame) -> list:
    """Return sensor column names (all columns except known metadata columns)."""
    return [c for c in df.columns if c not in _META_COLS]


def _normalise_bbl(bbl: str) -> str:
    """Strip whitespace and dashes; preserve leading zeros."""
    return bbl.strip().replace("-", "")


def _check_tables_exist(client, table_names: list) -> list:
    """
    Probe each table with a lightweight ``SELECT`` (limit 1).

    Returns a list of table names that do NOT exist (empty list = all OK).
    PostgREST returns a 404-style error when the table is missing; we
    catch that and report it so the caller can show an actionable message.
    """
    missing: list = []
    for name in table_names:
        try:
            client.table(name).select("bbl").limit(1).execute()
        except Exception as exc:
            exc_str = str(exc)
            # PostgREST 404 or "relation does not exist" both indicate
            # the table hasn't been created yet.
            if "404" in exc_str or "does not exist" in exc_str.lower():
                missing.append(name)
                logger.warning("Table %s not found: %s", name, exc_str)
            else:
                # Some other error (auth, network, etc.) – don't mask it
                # as a missing table; let the caller proceed and hit the
                # real error during upsert.
                logger.warning(
                    "Unexpected error probing table %s (treating as OK): %s",
                    name, exc_str,
                )
    return missing


# ---------------------------------------------------------------------------
# Wide → Long transformations
# ---------------------------------------------------------------------------

def melt_wide_to_long_raw(combined_df: pd.DataFrame, bbl: str) -> pd.DataFrame:
    """
    Transform the minute-level combined (wide) DataFrame into long format
    suitable for the ``combined_raw`` table.

    Output columns: bbl, timestamp, sensor_name, reading_value, data_year

    ALL rows are kept (including NaN readings) so the database mirrors
    the full grid that appears in the exported Excel/CSV files.
    """
    sensor_cols = _sensor_columns(combined_df)
    if not sensor_cols:
        return pd.DataFrame(
            columns=["bbl", "timestamp", "sensor_name", "reading_value", "data_year"]
        )

    long = combined_df[["Date"] + sensor_cols].melt(
        id_vars="Date",
        value_vars=sensor_cols,
        var_name="sensor_name",
        value_name="reading_value",
    )

    long["reading_value"] = pd.to_numeric(long["reading_value"], errors="coerce")
    # NaN reading_value rows are KEPT — they represent intervals where the
    # sensor had no data, matching what appears in the exported files.

    long["bbl"] = _normalise_bbl(bbl)
    long["timestamp"] = pd.to_datetime(long["Date"])
    long["data_year"] = long["timestamp"].dt.year

    return long[["bbl", "timestamp", "sensor_name", "reading_value", "data_year"]].copy()


def melt_wide_to_long_resampled(
    resampled_df: pd.DataFrame,
    inexact_df: pd.DataFrame,
    bbl: str,
) -> pd.DataFrame:
    """
    Transform the 15-minute resampled (wide) DataFrame into long format
    suitable for the ``resampled_15min`` table.

    Output columns:
        bbl, timestamp, sensor_name, reading_value, data_year,
        is_stale, is_inexact, zero_flag

    is_stale  – True when this specific sensor appears in Stale_Sensors for
                that row (not just the row-level Stale_Data_Flag).
    is_inexact – per-sensor boolean from the inexact_df tracking matrix.
    zero_flag  – row-level Zero_Value_Flag (same value for all sensors in
                 that row; the flag is aggregate across sensors).

    Rows where reading_value is NaN are dropped.
    """
    sensor_cols = _sensor_columns(resampled_df)
    if not sensor_cols:
        return pd.DataFrame(
            columns=[
                "bbl", "timestamp", "sensor_name", "reading_value",
                "data_year", "is_stale", "is_inexact", "zero_flag",
            ]
        )

    n_rows = len(resampled_df)
    resampled = resampled_df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # 1. Melt sensor values
    # ------------------------------------------------------------------
    value_long = resampled[["Date"] + sensor_cols].melt(
        id_vars="Date",
        value_vars=sensor_cols,
        var_name="sensor_name",
        value_name="reading_value",
    )

    # ------------------------------------------------------------------
    # 2. Build per-sensor inexact flag matrix then melt
    # ------------------------------------------------------------------
    inexact_reset = (
        inexact_df.reset_index(drop=True)
        if not inexact_df.empty and len(inexact_df) == n_rows
        else pd.DataFrame()
    )

    if not inexact_reset.empty:
        avail = [s for s in sensor_cols if s in inexact_reset.columns]
        missing = [s for s in sensor_cols if s not in inexact_reset.columns]
        inexact_wide = inexact_reset[avail].copy()
        for s in missing:
            inexact_wide[s] = False
    else:
        inexact_wide = pd.DataFrame(False, index=range(n_rows), columns=sensor_cols)

    inexact_wide["Date"] = resampled["Date"].values
    inexact_long = inexact_wide.melt(
        id_vars="Date",
        value_vars=sensor_cols,
        var_name="sensor_name",
        value_name="is_inexact",
    )

    # ------------------------------------------------------------------
    # 3. Build per-sensor stale flag matrix then melt
    #    A sensor is stale on a row only if its name appears in
    #    Stale_Sensors (comma-separated) for that row.
    # ------------------------------------------------------------------
    if "Stale_Data_Flag" in resampled.columns and "Stale_Sensors" in resampled.columns:
        row_stale = resampled["Stale_Data_Flag"].astype(bool)
        stale_txt = resampled["Stale_Sensors"].fillna("")
    else:
        row_stale = pd.Series(False, index=resampled.index)
        stale_txt = pd.Series("", index=resampled.index)

    stale_wide_data: dict = {}
    for sensor in sensor_cols:
        # Only run str.contains on rows where row_stale=True to save work
        sensor_stale = row_stale & stale_txt.str.contains(
            re.escape(sensor), regex=True, na=False
        )
        stale_wide_data[sensor] = sensor_stale

    stale_wide = pd.DataFrame(stale_wide_data, index=resampled.index)
    stale_wide["Date"] = resampled["Date"].values
    stale_long = stale_wide.melt(
        id_vars="Date",
        value_vars=sensor_cols,
        var_name="sensor_name",
        value_name="is_stale",
    )

    # ------------------------------------------------------------------
    # 4. Zero flag – row-level, same for every sensor in that row
    # ------------------------------------------------------------------
    zero_flag_series = (
        resampled["Zero_Value_Flag"]
        if "Zero_Value_Flag" in resampled.columns
        else pd.Series("Clear", index=resampled.index)
    )
    zero_map = pd.DataFrame(
        {"Date": resampled["Date"].values, "zero_flag": zero_flag_series.values}
    )

    # ------------------------------------------------------------------
    # 5. Merge all columns together
    # ------------------------------------------------------------------
    long = (
        value_long
        .merge(inexact_long, on=["Date", "sensor_name"])
        .merge(stale_long, on=["Date", "sensor_name"])
        .merge(zero_map, on="Date")
    )

    # ------------------------------------------------------------------
    # 6. Final type enforcement and cleanup
    # ------------------------------------------------------------------
    long["reading_value"] = pd.to_numeric(long["reading_value"], errors="coerce")
    # NaN reading_value rows are KEPT — mirrors the full resampled Excel output.

    long["bbl"] = _normalise_bbl(bbl)
    long["timestamp"] = pd.to_datetime(long["Date"])
    long["data_year"] = long["timestamp"].dt.year
    long["is_stale"] = long["is_stale"].astype(bool)
    long["is_inexact"] = long["is_inexact"].astype(bool)
    long["zero_flag"] = long["zero_flag"].astype(str).fillna("Clear")

    return long[
        ["bbl", "timestamp", "sensor_name", "reading_value",
         "data_year", "is_stale", "is_inexact", "zero_flag"]
    ].copy()


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def _rows_to_json_safe(long_df: pd.DataFrame) -> list:
    """
    Convert a long DataFrame to a list of dicts ready for JSON serialisation.

    JSON does not allow NaN, Infinity, or -Infinity.  pandas' ``.where(notna())``
    misses ``inf`` and can leave behind numpy scalar types that the stdlib
    ``json`` encoder rejects.  We therefore do a final Python-level sweep on
    every dict to guarantee strict JSON compliance.
    """
    import math

    df = long_df.copy()
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # First pass: pandas-level NaN/NaT → None (fast, covers most cases)
    df = df.where(df.notna(), other=None)

    records = df.to_dict(orient="records")

    # Second pass: catch inf, -inf, and any surviving NaN at the Python level
    def _sanitise(val):
        if val is None:
            return None
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                return None
        return val

    return [
        {k: _sanitise(v) for k, v in row.items()}
        for row in records
    ]


def _upsert_batches(
    client,
    table: str,
    rows: list,
    batch_size: int = BATCH_SIZE,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """
    Upsert *rows* to *table* in chunks of *batch_size*.

    The PRIMARY KEY on (bbl, timestamp, sensor_name) drives conflict
    resolution – existing rows are updated, new rows are inserted.

    Returns a summary dict:
        rows_attempted  – total rows submitted
        rows_upserted   – rows successfully upserted
        errors          – list of error strings from failed batches
    """
    total = len(rows)
    upserted = 0
    errors: list = []

    for start in range(0, total, batch_size):
        batch = rows[start : start + batch_size]
        try:
            client.table(table).upsert(batch).execute()
            upserted += len(batch)
        except Exception as exc:
            err_msg = (
                f"Batch {start}–{start + len(batch) - 1}: {exc}"
            )
            errors.append(err_msg)
            logger.warning("Upsert error in %s – %s", table, err_msg)

        if progress_callback:
            progress_callback(
                upserted,
                total,
                f"Saving to {table}: {upserted:,} / {total:,} rows",
            )

    return {
        "rows_attempted": total,
        "rows_upserted": upserted,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def save_all_to_supabase(
    combined_df: pd.DataFrame,
    resampled_df: pd.DataFrame,
    inexact_df: pd.DataFrame,
    bbl: str,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """
    Save both raw and resampled sensor data to Supabase.

    This is the single entry point called from ``auto_process_and_export()``.
    A failure here is **non-fatal**: the caller still reports file export as
    successful and shows a warning for the DB step.

    Args:
        combined_df:    Wide raw/minute-level DataFrame (Date + sensor columns).
        resampled_df:   Wide resampled 15-min DataFrame (Date + flag + sensor columns).
        inexact_df:     Boolean DataFrame tracking per-cell inexact matches.
        bbl:            Raw BBL string (normalisation applied internally).
        progress_callback:
            Optional callable ``(current: int, total: int, message: str) → None``
            for progress reporting.  Uses a 4-step scale:
              0 / 4 – before raw melt
              1 / 4 – raw melt done, starting raw upsert
              2 / 4 – raw upsert done, starting resampled melt
              3 / 4 – resampled melt done, starting resampled upsert
              4 / 4 – all done

    Returns:
        dict with keys:
            success          bool
            raw_result       dict | None   (rows_attempted, rows_upserted, errors)
            resampled_result dict | None
            error            str | None    (top-level error message if failed early)
    """
    result: dict = {
        "success": False,
        "raw_result": None,
        "resampled_result": None,
        "error": None,
    }

    def _progress(step: int, message: str) -> None:
        if progress_callback:
            progress_callback(step, 4, message)

    # ------------------------------------------------------------------
    # Guard: credentials + BBL
    # ------------------------------------------------------------------
    bbl_clean = _normalise_bbl(bbl) if bbl else ""
    if not bbl_clean:
        result["error"] = "BBL is empty – skipping database save."
        logger.warning(result["error"])
        return result

    client = get_supabase_client()
    if client is None:
        result["error"] = (
            "Supabase client unavailable – check SUPABASE_URL and "
            "SUPABASE_SERVICE_KEY in .env, and that supabase-py is installed."
        )
        return result

    # ------------------------------------------------------------------
    # Guard: verify both tables exist before attempting bulk inserts.
    # PostgREST returns 404 when a table doesn't exist, which shows up
    # as silent batch errors. Catching it early gives the user a clear
    # actionable message.
    # ------------------------------------------------------------------
    missing_tables = _check_tables_exist(client, [TABLE_RAW, TABLE_RESAMPLED])
    if missing_tables:
        names = ", ".join(f'"{t}"' for t in missing_tables)
        result["error"] = (
            f"Supabase table(s) {names} not found (HTTP 404). "
            f"Please run supabase_schema.sql in the Supabase SQL Editor "
            f"to create the required tables and indexes, then re-process."
        )
        logger.error(result["error"])
        return result

    # ------------------------------------------------------------------
    # Phase A: raw data
    # ------------------------------------------------------------------
    try:
        _progress(0, "Preparing raw sensor data for database…")
        raw_long = melt_wide_to_long_raw(combined_df, bbl_clean)
        logger.info("Raw long DataFrame: %d rows", len(raw_long))

        _progress(1, f"Saving {len(raw_long):,} raw rows to Supabase…")
        raw_rows = _rows_to_json_safe(raw_long)
        raw_result = _upsert_batches(client, TABLE_RAW, raw_rows)
        result["raw_result"] = raw_result

        if raw_result["errors"]:
            logger.warning(
                "Raw upsert completed with %d batch error(s): %s",
                len(raw_result["errors"]),
                raw_result["errors"],
            )

    except Exception as exc:
        result["error"] = f"Raw data stage failed: {exc}"
        logger.exception("Unexpected error during raw data save")
        return result

    # ------------------------------------------------------------------
    # Phase B: resampled data
    # ------------------------------------------------------------------
    try:
        _progress(2, "Preparing resampled sensor data for database…")
        resampled_long = melt_wide_to_long_resampled(resampled_df, inexact_df, bbl_clean)
        logger.info("Resampled long DataFrame: %d rows", len(resampled_long))

        _progress(3, f"Saving {len(resampled_long):,} resampled rows to Supabase…")
        resampled_rows = _rows_to_json_safe(resampled_long)
        resampled_result = _upsert_batches(client, TABLE_RESAMPLED, resampled_rows)
        result["resampled_result"] = resampled_result

        if resampled_result["errors"]:
            logger.warning(
                "Resampled upsert completed with %d batch error(s): %s",
                len(resampled_result["errors"]),
                resampled_result["errors"],
            )

    except Exception as exc:
        result["error"] = f"Resampled data stage failed: {exc}"
        logger.exception("Unexpected error during resampled data save")
        # raw already succeeded – report partial success
        result["success"] = False
        return result

    _progress(4, "Database save complete!")
    result["success"] = True
    return result
