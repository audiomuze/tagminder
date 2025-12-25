import polars as pl
import sqlite3
import argparse
import sys
import logging
from typing import Optional, Tuple
from datetime import datetime, timezone

# ---------- Config ----------
SCRIPT_NAME = "alib_merge.py"

# ---------- Logging ----------
logging.basicConfig(level=logging.WARNING, format="%(levelname)s - %(message)s")

def import_alib_to_polars(db_path: str, table_name: str) -> pl.DataFrame:
    """
    Import an ALIB table to a Polars DataFrame with all columns as Utf8.
    """
    try:
        import pandas as pd
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        pandas_df = pd.read_sql_query(query, conn)
        conn.close()

        df = pl.from_pandas(pandas_df)
        df = df.with_columns([
            pl.col(col).cast(pl.Utf8, strict=False) for col in df.columns
        ])
        return df

    except sqlite3.Error as e:
        logging.error(f"Database error accessing {db_path}, table {table_name}: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)

def write_dataframe_to_db(df: pl.DataFrame, db_path: str, table_name: str, if_exists: str = "replace") -> None:
    """Write a Polars DataFrame to a SQLite database table."""
    try:
        import pandas as pd
        conn = sqlite3.connect(db_path)
        pandas_df = df.to_pandas()
        pandas_df = pandas_df.replace({'None': None})
        pandas_df.to_sql(table_name, conn, if_exists=if_exists, index=False, method=None)
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Database error writing to {db_path}, table {table_name}: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error writing to database: {e}")
        sys.exit(1)

def setup_changelog_table(conn: sqlite3.Connection) -> None:
    """Create changelog table if it doesn't exist."""
    cursor = conn.cursor()
    # # for this script only drop the changelog because its a merge operation between two tables and old changelog is irrelevant
    # cursor.execute("""
    #     DROP TABLE IF EXISTS changelog
    # """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid TEXT,
            alib_column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)
    conn.commit()

def analyze_merge_impact(df1: pl.DataFrame, df2: pl.DataFrame, merged_df: pl.DataFrame,
                        primary_key: str = "__path") -> dict:
    """
    Analyze the impact of the merge operation and return key metrics.
    """
    # Basic counts
    df1_rows, df1_cols = df1.shape
    df2_rows, df2_cols = df2.shape
    merged_rows, merged_cols = merged_df.shape

    # Column analysis
    df1_cols_set = set(df1.columns)
    df2_cols_set = set(df2.columns)
    common_cols = (df1_cols_set & df2_cols_set) - {primary_key}
    new_cols_added = len(df2_cols_set - df1_cols_set)

    # Enrichment analysis - compare df1 vs merged for common columns
    enriched_cells = 0
    enriched_rows = set()
    enriched_columns = set()

    if common_cols:
        # Join df1 with merged to compare values
        comparison = df1.join(merged_df, on=primary_key, how="inner", suffix="_merged")

        for col in common_cols:
            merged_col = f"{col}_merged"
            if merged_col in comparison.columns:
                # Count where df1 was empty/null but merged has value
                df1_empty = (pl.col(col).is_null()) | (pl.col(col).str.strip_chars().str.len_chars() == 0)
                merged_has_value = (pl.col(merged_col).is_not_null()) & (pl.col(merged_col).str.strip_chars().str.len_chars() > 0)

                enriched_mask = df1_empty & merged_has_value
                enriched_in_col = comparison.filter(enriched_mask)

                if enriched_in_col.height > 0:
                    enriched_cells += enriched_in_col.height
                    enriched_columns.add(col)
                    # Get the row IDs that were enriched
                    enriched_row_ids = enriched_in_col.select(primary_key).to_series().to_list()
                    enriched_rows.update(enriched_row_ids)

    # Count new rows added from df2
    new_rows_added = merged_rows - df1_rows

    return {
        'df1_rows': df1_rows,
        'df1_cols': df1_cols,
        'df2_rows': df2_rows,
        'df2_cols': df2_cols,
        'merged_rows': merged_rows,
        'merged_cols': merged_cols,
        'new_cols_added': new_cols_added,
        'new_rows_added': new_rows_added,
        'enriched_cells': enriched_cells,
        'enriched_rows_count': len(enriched_rows),
        'enriched_columns_count': len(enriched_columns),
        'enriched_columns': sorted(enriched_columns)
    }

def log_merge_changes(conn: sqlite3.Connection, df1: pl.DataFrame, df2: pl.DataFrame,
                     merged_df: pl.DataFrame, primary_key: str = "__path") -> int:
    """Log changes where df2 values filled empty/null df1 values."""
    timestamp = datetime.now(timezone.utc).isoformat()
    changelog_entries = []

    # 1. Handle enrichment of existing rows
    common_cols = (set(df1.columns) & set(df2.columns)) - {primary_key, 'sqlmodded'}
    common_cols = {col for col in common_cols if not col.startswith("__")}

    if common_cols:
        comparison = df1.join(df2, on=primary_key, how="inner", suffix="_df2")
        for col in common_cols:
            col_df2 = f"{col}_df2"
            changes = comparison.filter(
                ((pl.col(col).is_null()) | (pl.col(col).str.strip_chars().str.len_chars() == 0)) &
                (pl.col(col_df2).is_not_null()) &
                (pl.col(col_df2).str.strip_chars().str.len_chars() > 0)
            ).select([
                pl.col(primary_key).alias("alib_rowid"),
                pl.lit(col).alias("column"),
                pl.coalesce([pl.col(col), pl.lit("")]).alias("old_value"),
                pl.col(col_df2).alias("new_value"),
                pl.lit(timestamp).alias("timestamp"),
                pl.lit(SCRIPT_NAME).alias("script")
            ])
            if changes.height > 0:
                changelog_entries.append(changes)

    # 2. Handle new rows from df2
    df2_only = df2.join(df1.select(primary_key), on=primary_key, how="anti")
    if df2_only.height > 0:
        for col in [c for c in df2_only.columns if c != primary_key and not c.startswith("__")]:
            new_rows = df2_only.filter(
                (pl.col(col).is_not_null()) &
                (pl.col(col).str.strip_chars().str.len_chars() > 0)
            ).select([
                pl.col(primary_key).alias("alib_rowid"),
                pl.lit(col).alias("column"),
                pl.lit("").alias("old_value"),
                pl.col(col).alias("new_value"),
                pl.lit(timestamp).alias("timestamp"),
                pl.lit(SCRIPT_NAME).alias("script")
            ])
            if new_rows.height > 0:
                changelog_entries.append(new_rows)

    if not changelog_entries:
        return 0

    # Write to changelog
    all_changes = pl.concat(changelog_entries)
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
        [(row["alib_rowid"], row["column"], row["old_value"],
          row["new_value"], row["timestamp"], row["script"])
         for row in all_changes.to_dicts()]
    )
    conn.commit()
    return all_changes.height

# def merge_dataframes_with_precedence(df1: pl.DataFrame, df2: pl.DataFrame,
#                                    primary_key: str = "__path") -> pl.DataFrame:
#     """Merge two DataFrames with df1 taking precedence over df2 and track sqlmodded increments."""
#     df1_cols = set(df1.columns)
#     df2_cols = set(df2.columns)

#     if primary_key not in df1_cols:
#         raise ValueError(f"Primary key '{primary_key}' not found in DataFrame 1")
#     if primary_key not in df2_cols:
#         raise ValueError(f"Primary key '{primary_key}' not found in DataFrame 2")

#     # Prepare 'sqlmodded' column in df1 for numeric operations
#     if 'sqlmodded' not in df1.columns:
#         df1 = df1.with_columns(pl.lit(0).alias('sqlmodded').cast(pl.Int64))
#     else:
#         df1 = df1.with_columns(
#             pl.col('sqlmodded')
#             .cast(pl.Int64, strict=False)  # Direct cast to Int64
#             .fill_null(0)
#             .alias('sqlmodded')
#         )

#     # Perform full outer join
#     result = df1.join(df2, on=primary_key, how="full", suffix="_df2")

#     # Build expressions for final selection
#     select_exprs = []

#     # Handle primary key first
#     df2_primary_key = f"{primary_key}_df2"
#     select_exprs.append(
#         pl.coalesce([pl.col(primary_key), pl.col(df2_primary_key)]).alias(primary_key)
#     )

#     # Process all non-primary-key columns
#     for col in sorted((df1_cols | df2_cols) - {primary_key}):
#         df2_col = f"{col}_df2"

#         if col in df1_cols and col in df2_cols:
#             # Common column - apply precedence logic
#             if col == 'sqlmodded':
#                 # Handle sqlmodded separately
#                 select_exprs.append(
#                     pl.when(
#                         (pl.col('sqlmodded').is_null()) | (pl.col('sqlmodded') == 0)
#                     ).then(
#                         pl.coalesce([pl.col(f'{col}_df2').cast(pl.Int64), pl.lit(0)]) + 1
#                     ).otherwise(
#                         pl.col('sqlmodded')
#                     ).alias(col)
#                 )
#             else:
#                 select_exprs.append(
#                     pl.when(
#                         (pl.col(col).is_null()) | (pl.col(col).str.strip_chars().str.len_chars() == 0)
#                     ).then(
#                         pl.col(df2_col) # Simply take df2_col value if df1 is null/empty
#                     ).otherwise(
#                         pl.col(col)
#                     ).alias(col)
#                 )
#         elif col in df1_cols:
#             # Column only in df1
#             select_exprs.append(pl.col(col))
#         else:
#             # Column only in df2
#             select_exprs.append(pl.col(df2_col).alias(col) if df2_col in result.columns else pl.lit(None).alias(col))

#     # Apply all column selections
#     df3 = result.select(select_exprs)

#     # Ensure sqlmodded exists and is cast to Utf8 for consistency
#     if 'sqlmodded' not in df3.columns:
#         df3 = df3.with_columns(pl.lit(0).alias('sqlmodded').cast(pl.Utf8))
#     else:
#         df3 = df3.with_columns(pl.col('sqlmodded').cast(pl.Utf8).alias('sqlmodded'))

#     return df3


# def merge_dataframes_with_precedence(df1: pl.DataFrame, df2: pl.DataFrame,
#                                    primary_key: str = "__path") -> pl.DataFrame:
#     """Merge two DataFrames with df1 taking precedence over df2 and track sqlmodded increments."""
#     df1_cols = set(df1.columns)
#     df2_cols = set(df2.columns)

#     if primary_key not in df1_cols:
#         raise ValueError(f"Primary key '{primary_key}' not found in DataFrame 1")
#     if primary_key not in df2_cols:
#         raise ValueError(f"Primary key '{primary_key}' not found in DataFrame 2")

#     # Prepare 'sqlmodded' column in df1 for numeric operations
#     if 'sqlmodded' not in df1.columns:
#         df1 = df1.with_columns(pl.lit(0).alias('sqlmodded').cast(pl.Int64))
#     else:
#         df1 = df1.with_columns(
#             pl.col('sqlmodded')
#             .cast(pl.Int64, strict=False)  # Direct cast to Int64
#             .fill_null(0)
#             .alias('sqlmodded')
#         )

#     # Perform full outer join
#     result = df1.join(df2, on=primary_key, how="full", suffix="_df2")

#     # Build expressions for final selection
#     select_exprs = []

#     # 1. Handle primary key first (always included)
#     df2_primary_key = f"{primary_key}_df2"
#     select_exprs.append(
#         pl.coalesce([pl.col(primary_key), pl.col(df2_primary_key)]).alias(primary_key)
#     )

#     # 2. Process columns from df1 in their original order
#     for col in df1.columns: # Iterate through df1's columns to preserve order
#         if col == primary_key:
#             continue # Already handled

#         df2_col = f"{col}_df2"
#         if col in df2_cols: # Common column
#             if col == 'sqlmodded':
#                 select_exprs.append(
#                     pl.when(
#                         (pl.col('sqlmodded').is_null()) | (pl.col('sqlmodded') == 0)
#                     ).then(
#                         pl.coalesce([pl.col(f'{col}_df2').cast(pl.Int64), pl.lit(0)]) + 1
#                     ).otherwise(
#                         pl.col('sqlmodded')
#                     ).alias(col)
#                 )
#             else:
#                 select_exprs.append(
#                     pl.when(
#                         (pl.col(col).is_null()) | (pl.col(col).str.strip_chars().str.len_chars() == 0)
#                     ).then(
#                         pl.col(df2_col)
#                     ).otherwise(
#                         pl.col(col)
#                     ).alias(col)
#                 )
#         else: # Column only in df1
#             select_exprs.append(pl.col(col))

#     # 3. Append new columns that are only in df2 (and not already processed)
#     for col in df2.columns:
#         if col == primary_key:
#             continue # Already handled

#         if col not in df1_cols: # This is a new column from df2
#             df2_col_name_in_result = f"{col}_df2" # Name in the joined 'result' dataframe
#             # Ensure it exists in the result before selecting
#             if df2_col_name_in_result in result.columns:
#                 select_exprs.append(pl.col(df2_col_name_in_result).alias(col))
#             else:
#                 # This case might occur if a column exists in df2 but not df1,
#                 # and then there are no matching rows to bring it in via the join.
#                 # It's safer to ensure it's still added, even if all null.
#                 select_exprs.append(pl.lit(None).alias(col))


#     # Apply all column selections
#     df3 = result.select(select_exprs)

#     # Ensure sqlmodded exists and is cast to Utf8 for consistency
#     if 'sqlmodded' not in df3.columns:
#         df3 = df3.with_columns(pl.lit(0).alias('sqlmodded').cast(pl.Utf8))
#     else:
#         df3 = df3.with_columns(pl.col('sqlmodded').cast(pl.Utf8).alias('sqlmodded'))

#     return df3

def merge_dataframes_with_precedence(df1: pl.DataFrame, df2: pl.DataFrame,
                                   primary_key: str = "__path") -> pl.DataFrame:
    """Merge two DataFrames with df1 taking precedence over df2 and track sqlmodded increments."""
    df1_cols = set(df1.columns)
    df2_cols = set(df2.columns)

    if primary_key not in df1_cols:
        raise ValueError(f"Primary key '{primary_key}' not found in DataFrame 1")
    if primary_key not in df2_cols:
        raise ValueError(f"Primary key '{primary_key}' not found in DataFrame 2")

    # SURGICAL CHANGE 1: Don't fill nulls in sqlmodded during initialization
    # Prepare 'sqlmodded' column in df1 for numeric operations
    if 'sqlmodded' not in df1.columns:
        df1 = df1.with_columns(pl.lit(None).alias('sqlmodded').cast(pl.Int64))
    else:
        df1 = df1.with_columns(
            pl.col('sqlmodded')
            .cast(pl.Int64, strict=False)  # Direct cast to Int64
            # REMOVED: .fill_null(0)  # This was the problem!
            .alias('sqlmodded')
        )

    # Perform full outer join
    result = df1.join(df2, on=primary_key, how="full", suffix="_df2")

    # Build expressions for final selection
    select_exprs = []

    # 1. Handle primary key first (always included)
    df2_primary_key = f"{primary_key}_df2"
    select_exprs.append(
        pl.coalesce([pl.col(primary_key), pl.col(df2_primary_key)]).alias(primary_key)
    )

    # SURGICAL CHANGE 2: We'll build enrichment detection inline during column processing
    # to avoid referencing columns that don't exist yet

    # 2. Build enrichment conditions as we process columns
    enrichment_conditions = []

    # First pass: collect enrichment conditions from common columns
    for col in df1.columns:
        if col in {primary_key, 'sqlmodded'}:
            continue
        if col in df2_cols:
            df2_col = f"{col}_df2"
            if df2_col in result.columns:
                enrichment_conditions.append(
                    ((pl.col(col).is_null()) | (pl.col(col).str.strip_chars().str.len_chars() == 0)) &
                    (pl.col(df2_col).is_not_null()) &
                    (pl.col(df2_col).str.strip_chars().str.len_chars() > 0)
                )

    # Add conditions for new columns from df2
    for col in df2.columns:
        if col in {primary_key, 'sqlmodded'} or col in df1_cols:
            continue
        df2_col = f"{col}_df2"
        if df2_col in result.columns:
            enrichment_conditions.append(
                (pl.col(df2_col).is_not_null()) &
                (pl.col(df2_col).str.strip_chars().str.len_chars() > 0)
            )

    # Combine all enrichment conditions
    if enrichment_conditions:
        row_is_enriched = pl.fold(
            acc=pl.lit(False),
            function=lambda acc, x: acc | x,
            exprs=enrichment_conditions
        )
    else:
        row_is_enriched = pl.lit(False)

    # Process columns from df1 in their original order
    for col in df1.columns:
        if col == primary_key:
            continue

        df2_col = f"{col}_df2"
        if col in df2_cols:
            if col == 'sqlmodded':
                # SURGICAL CHANGE 3: Only increment sqlmodded when row is actually enriched
                select_exprs.append(
                    pl.when(row_is_enriched).then(
                        # Increment existing sqlmodded or start at 1 if null
                        pl.coalesce([pl.col('sqlmodded'), pl.lit(0)]) + 1
                    ).otherwise(
                        # Keep original sqlmodded value (including null)
                        pl.col('sqlmodded')
                    ).alias(col)
                )
            else:
                select_exprs.append(
                    pl.when(
                        (pl.col(col).is_null()) | (pl.col(col).str.strip_chars().str.len_chars() == 0)
                    ).then(
                        pl.col(df2_col)
                    ).otherwise(
                        pl.col(col)
                    ).alias(col)
                )
        else:
            select_exprs.append(pl.col(col))

    # 3. Append new columns that are only in df2
    for col in df2.columns:
        if col == primary_key:
            continue

        if col not in df1_cols:
            df2_col_name_in_result = f"{col}_df2"
            if df2_col_name_in_result in result.columns:
                select_exprs.append(pl.col(df2_col_name_in_result).alias(col))
            else:
                select_exprs.append(pl.lit(None).alias(col))

    # Apply all column selections
    df3 = result.select(select_exprs)

    # SURGICAL CHANGE 4: Handle sqlmodded for new rows from df2 (simplified)
    # For rows that came only from df2, set sqlmodded to 1 if they actually have data
    if 'sqlmodded' not in df3.columns:
        # If sqlmodded column doesn't exist, add it with proper logic
        df3 = df3.with_columns(pl.lit(None).cast(pl.Utf8).alias('sqlmodded'))

    # SURGICAL CHANGE 5: Cast to Utf8 but preserve nulls
    df3 = df3.with_columns(
        pl.col('sqlmodded').cast(pl.Utf8, strict=False).alias('sqlmodded')
    )

    return df3

def main():
    parser = argparse.ArgumentParser(description='Merge ALIB tables with precedence')
    parser.add_argument('--db-path', '-d', default='/tmp/alib/dbmerge.db',
                       help='Path to SQLite database file (default: /tmp/alib/dbmerge.db)')
    parser.add_argument('--table1', '-t1', default='alib1',
                       help='Name of first table (default: alib1)')
    parser.add_argument('--table2', '-t2', default='alib2',
                       help='Name of second table (default: alib2)')
    parser.add_argument('--output-table', '-ot', default='alib_merged',
                       help='Output table name in database (default: alib_merged)')
    parser.add_argument('--no-db-write', action='store_true',
                       help='Skip writing merged data back to database')
    parser.add_argument('--no-changelog', action='store_true',
                       help='Skip changelog logging')
    parser.add_argument('--output', '-o',
                       help='Output file path (optional, for CSV/Parquet export)')
    parser.add_argument('--primary-key', '-pk', default='__path',
                       help='Primary key column name (default: __path)')

    args = parser.parse_args()

    print(f"📊 ALIB MERGE OPERATION")
    print(f"{'='*50}")
    print(f"Database: {args.db_path}")
    print(f"Merging: {args.table1} + {args.table2} → {args.output_table}")
    print(f"Primary key: {args.primary_key}")

    # Import tables
    df1 = import_alib_to_polars(args.db_path, args.table1)
    df2 = import_alib_to_polars(args.db_path, args.table2)

    # Merge with precedence
    df3 = merge_dataframes_with_precedence(df1, df2, args.primary_key)

    # Analyze impact
    impact = analyze_merge_impact(df1, df2, df3, args.primary_key)

    # Report results
    print(f"\n📈 MERGE RESULTS")
    print(f"{'='*50}")
    print(f"Input Data:")
    print(f"  • {args.table1}: {impact['df1_rows']:,} rows × {impact['df1_cols']} columns")
    print(f"  • {args.table2}: {impact['df2_rows']:,} rows × {impact['df2_cols']} columns")

    print(f"\nMerged Data:")
    print(f"  • Final result: {impact['merged_rows']:,} rows × {impact['merged_cols']} columns")
    print(f"  • New columns added: {impact['new_cols_added']}")
    print(f"  • New rows added: {impact['new_rows_added']:,}")

    print(f"\nData Enrichment:")
    print(f"  • Enriched cells: {impact['enriched_cells']:,}")
    print(f"  • Enriched rows: {impact['enriched_rows_count']:,}")
    print(f"  • Enriched columns: {impact['enriched_columns_count']}")

    if impact['enriched_columns']:
        print(f"  • Columns with enrichment: {', '.join(impact['enriched_columns'][:10])}")
        if len(impact['enriched_columns']) > 10:
            print(f"    ... and {len(impact['enriched_columns']) - 10} more")

    # Log changes to changelog
    if not args.no_changelog:
        conn = sqlite3.connect(args.db_path)
        try:
            setup_changelog_table(conn)
            changes_count = log_merge_changes(conn, df1, df2, df3, args.primary_key)
            print(f"  • Changelog entries: {changes_count:,}")
        finally:
            conn.close()

    # Write merged data back to database
    if not args.no_db_write:
        write_dataframe_to_db(df3, args.db_path, args.output_table)
        print(f"\n💾 Data written to table '{args.output_table}'")

    # Export to file if specified
    if args.output:
        if args.output.endswith('.csv'):
            df3.write_csv(args.output)
            print(f"📁 Exported to CSV: {args.output}")
        elif args.output.endswith('.parquet'):
            df3.write_parquet(args.output)
            print(f"📁 Exported to Parquet: {args.output}")
        else:
            output_file = args.output + '.csv'
            df3.write_csv(output_file)
            print(f"📁 Exported to CSV: {output_file}")

    print(f"\n✅ MERGE COMPLETED SUCCESSFULLY!")
    return df3

if __name__ == "__main__":
    df3 = main()
