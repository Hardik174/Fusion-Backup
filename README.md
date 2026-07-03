# Fusion Report Engine (`fusion_report_engine`)

The **Fusion Report Engine** is a high-performance, Polars-based ETL and reporting pipeline designed to replace complex, resource-intensive legacy PostgreSQL reporting scripts. By translating relational query flows into Polars LazyFrames, the engine achieves sub-second processing speeds, complete data type safety, and premium Excel reporting outputs.

---

## 🗺️ 1. Project Overview

### Purpose
The Fusion Report Engine aggregates, processes, and formats multi-channel communication histories (VoiceBot calling records, WhatsApp messages, and Blaster calling records) alongside transaction logs to construct a comprehensive Month-To-Date (MTD) collections report.

### Legacy PostgreSQL vs. Polars Migration
*   **Original Implementation**: Relied on a sequence of PostgreSQL scripts utilizing multiple temporary tables (`voicebot_mtd_base`, `wa_received_by_day`, `latest_master`, etc.), window queries (`ROW_NUMBER() OVER (PARTITION BY ...)`), string manipulation, and datetime casting. This placed high CPU and memory loads on the database server during generation.
*   **Polars Migration**: Relies on a single, linear python pipeline orchestration. By executing all aggregations, filtering, and joins inside Polars `LazyFrame` graphs, the calculation resolves locally, offloading workload from the database instance and completing execution in fractions of a second.

### Pipeline Dataflow
```
 ┌──────────────────────┐
 │ PostgreSQL Raw Data  │ (Rawfile, LoanMst, VoiceBotHistory, etc.)
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ load_source_tables() │ (DB extraction via psycopg2 connection)
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ Intermediate Stages  │ (Phases 1-15: LazyFrame aggregations & mappings)
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ build_latest_master()│ (Phase 16: LEFT JOIN enrichment layer)
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ build_daily_pivots() │ (Phase 17: Date-labeled horizontal pivot timelines)
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ build_final_report() │ (Phase 18: SQL select projection & mobile filtering)
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ export_final_report()│ (Phase 19: Excel workbook styling using openpyxl)
 └──────────────────────┘
```

---

## 📁 2. Folder Structure

The engine is structured as an isolated module within the repository:

```
fusion_report_engine/
│
├── __init__.py          # Package entry point exposing main interface classes
├── report_builder.py    # Main orchestration class (ReportBuilder) & parameter DTO (ReportRequest)
├── business_rules.py    # Business rules, priority rankings, status mappings, & category configurations
├── config.py            # Database credentials, default pagination, and campaign boundaries
├── logging_config.py    # Standardized logging setup for diagnostics & debugging
├── timer.py             # Timer decorator utility to profile each builder stage
├── utils.py             # Internal helper utilities (date normalization, casting formats)
└── README.md            # Comprehensive developer manual
```

### Module Responsibilities
*   **`report_builder.py`**: Controls execution flow, handles parameter checks, establishes database connectivity, and manages collections of computed DataFrames.
*   **`business_rules.py`**: Isolates all arbitrary business constants (e.g. mapping `08_SwitchedOff` to `"Switched Off"` and ranking its contactability weight) away from the query translation code.
*   **`config.py`**: Configures PostgreSQL access points. Placeholders are mapped to read environment variables or settings files in production.
*   **`timer.py`**: Uses Python's `time.perf_counter` to measure and log execution metrics to standard output automatically.

---

## 🔄 3. Pipeline Overview

The reporting pipeline executes the following sequential phases during a single invocation of `generate_report(request)`:

| Phase | Stage Method | Replicated SQL Table / CTE | Primary Resulting DataFrame |
| :---: | :--- | :--- | :--- |
| **1** | `build_base_accounts` | `base_accounts` | `base_accounts_df` |
| **2** | `build_all_call_dispositions` | `voicebot_mtd_base` & `all_call_dispositions` | `all_call_dispositions_df` |
| **3** | `build_all_wa_dispositions` | `all_wa_dispositions` | `all_wa_dispositions_df` |
| **4** | `build_all_blaster_dispositions`| `all_blaster_dispositions` | `all_blaster_dispositions_df` |
| **5** | `build_best_disposition_per_loan`| `best_disposition` | `best_disposition_df` |
| **6** | `build_voicebot_summary` | `voicebot_summary` | `voicebot_summary_df` |
| **7** | `build_whatsapp_summary` | `whatsapp_summary` | `whatsapp_summary_df` |
| **8** | `build_blaster_summary` | `blaster_summary` | `blaster_summary_df` |
| **9** | `build_communication_summary` | `communication_summary` | `communication_summary_df` |
| **10**| `build_latest_channels` | `latest_call`, `latest_blaster`, `latest_whatsapp` | `latest_call_df`, `latest_blaster_df`, etc. |
| **11**| `build_collections` | `latest_collection`, `total_collection` | `latest_collection_df`, `total_collection_df` |
| **12**| `build_connection_statistics` | `mtd_connection_flags`, `mtd_wa_connection_flags`| `mtd_connection_flags_df`, etc. |
| **13**| `build_historical_dispositions` | `last_connected_info`, `latest_disposition_mtd` | `last_connected_info_df`, `latest_disposition_mtd_df` |
| **14**| `build_latest_response_tables` | `latest_response`, `latest_voicebot_response` | `latest_response_df`, `latest_voicebot_response_df` |
| **15**| `build_latest_master` | `latest_master` | `latest_master_df` |
| **16**| `build_daily_pivots` | `daily_call_dispositions`, `daily_pivots` | `daily_pivots_df` |
| **17**| `build_final_report` | Final SELECT query (STEP 7) | `final_report_df` |
| **18**| `export_final_report` | N/A (Excel Export formatting) | Writes Excel output file on disk |

---

## 📈 4. Complete DataFrame Dependency Graph

The dependencies of all key DataFrames are tracked at runtime:

```
                  ┌──────────────────────────────┐
                  │      load_source_tables      │
                  └──────────────┬───────────────┘
                                 │
         ┌───────────────────────┼────────────────────────┐
         ▼                       ▼                        ▼
  [self.rawfile_df]      [self.voicebot_df]       [self.whatsapp_df]
         │                       │                        │
         ▼                       ▼                        ▼
 ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
 │base_accounts │         │  voicebot_   │         │  whatsapp_   │
 │     _df      ├────────►│  mtd_base    ├────────►│  mtd_base    │
 └───────┬──────┘         └──────┬───────┘         └──────┬───────┘
         │                       │                        │
         │   ┌───────────────────┼────────────────────────┘
         │   │                   ▼
         │   │            ┌──────────────┐
         │   │            │  all_call_   │
         │   │            │ dispositions │
         │   │            └──────┬───────┘
         │   │                   │
         ▼   ▼                   ▼
 ┌──────────────┐         ┌──────────────┐
 │  daily_call_ │◄────────┤latest_call_df│
 │ dispositions │         └──────┬───────┘
 └───────┬──────┘                │
         │                       ▼
         │                ┌──────────────┐
         │                │latest_master_│
         │                │     df       │
         │                └──────┬───────┘
         │                       │
         ▼                       ▼
 ┌──────────────┐         ┌──────────────┐
 │ daily_pivots │◄────────┤ final_report │
 │     _df      │         │     _df      │
 └──────────────┘         └──────┬───────┘
                                 │
                                 ▼
                         ┌──────────────┐
                         │ Excel Export │
                         └──────────────┘
```

---

## 🔀 5. SQL → Polars Mapping Reference

Below is the definitive reference mapping PostgreSQL relational concepts to high-performance Polars operations:

| PostgreSQL Concept | Polars Implementation | Architectural Rationale |
| :--- | :--- | :--- |
| **`SELECT col1, col2`** | `.select(["col1", "col2"])` | Direct column projections. |
| **`WHERE cond`** | `.filter(cond)` | Filter predicate pushdown optimized at runtime. |
| **`LEFT JOIN`** | `.join(df, on="key", how="left")` | Relational join mapping. |
| **`INNER JOIN`** | `.join(df, on="key", how="inner")` | Retains matched rows. |
| **`CROSS JOIN`** | `.join(df, how="cross")` | Used in daily pivots timeline matrix. |
| **`GROUP BY ... agg`** | `.group_by("cols").agg(exprs)` | Grouping aggregation pipeline. |
| **`DISTINCT ON (col) ... ORDER BY col2 DESC`**| `.sort("col2", descending=True).unique(subset=["col"], keep="first")` | Native replacement for Postgres partitioning window functions. |
| **`ROW_NUMBER() OVER (PARTITION BY ...)`**| `.with_columns(pl.col("col").cum_count().over("part_col"))` | Cumulative window calculations. |
| **`CASE WHEN ... THEN ... ELSE`**| `pl.when(cond).then(val).otherwise(default)`| Conditional expression chaining. |
| **`COALESCE(col1, col2)`** | `pl.coalesce([pl.col("col1"), pl.col("col2")])` | In-order fallback value selection. |
| **`GREATEST(col1, col2)`** | `pl.max_horizontal(["col1", "col2"])` | Row-wise maximum value calculation. |
| **`ILIKE '%pattern%'`** | `.str.to_lowercase().str.contains("pattern")` | Case-insensitive text search. |
| **`TRIM(col)`** | `.str.strip_chars()` | Trims trailing and leading whitespace. |
| **`generate_series(start, end)`**| `pl.date_range(start, end, "1d")` | Generates chronological sequences. |

---

## 🧠 6. Business Rules & Logic Normalizations

All calculations are driven by configurations isolated within `business_rules.py`:

1.  **Best Disposition Ranking**:
    *   Relies on `DISPOSITION_RANKING` table priorities (e.g. `PTP` has rank `1.0` and highest priority, while `No Answer` has rank `10.9` and lowest priority).
    *   Calculated by picking the row for each loan with the lowest `rank_val` (meaning the highest priority).
2.  **Disposition Aliases**:
    *   Uses `DISPOSITION_ALIASES` to normalize various raw text variations (e.g., mapping both `"ALREADY_PAID"` and `"Paid"` to `"Already Paid"`).
3.  **Connection Determination**:
    *   **VoiceBot**: `is_connected` is `True` if `Recording` is not null and has a length greater than 1 character.
    *   **WhatsApp**: `is_connected` is `True` if the message is read (`IsRead == True`) OR if the message is received after the dispatch datetime (`received_after_send == True`) OR if the response mapping returns `has_any_response == 1`.
4.  **Collection Status Verification**:
    *   Checks if the collected MTD amount satisfies at least 1 full EMI (`Total Collection Amt >= EMI Amount`).
    *   Checks if the collected amount resolves the outstanding overdue amount (`Total Collection Amt >= Default Amt`).

---

## ⚙️ 7. Phase-by-Phase Execution Details

### Phase 1: Base Accounts Ingestion
*   **Objective**: Standardize target loan accounts.
*   **Inputs**: `Rawfile`, `Rawfile_Backup`, `LoanMst`.
*   **Transformations**: Joins raw files with `LoanMst` on `DisbursementID`, filters for active records, and casts values.
*   **Output**: `base_accounts_df`.

### Phase 2: VoiceBot Dispositions Ingestion
*   **Objective**: Process all VoiceBot call records during the current month.
*   **Inputs**: `VoiceBotHistory`, `base_accounts_df`.
*   **Transformations**: Filters calls on `BankMstID == 53`, `CallTried > 0`, and non-empty `CallID`. Maps raw call dispositions using `DISPOSITION_ALIASES`.
*   **Output**: `all_call_dispositions_df`.

### Phase 3: WhatsApp Dispositions Ingestion
*   **Objective**: Process MTD WhatsApp messages.
*   **Inputs**: `WhatsAppHistory`, `base_accounts_df`.
*   **Transformations**: Filters for sent messages (`IsSent == True`), parses dates, and resolves WhatsApp dispositions.
*   **Output**: `all_wa_dispositions_df`.

### Phase 4: Blaster Dispositions Ingestion
*   **Objective**: Ingest outbound blaster records.
*   **Inputs**: `BlasterHistory`, `base_accounts_df`.
*   **Transformations**: Filters on dates and Campaign IDs, normalizes dispositions to `Refused to pay`, `No Answer`, `Busy`, or `Other`.
*   **Output**: `all_blaster_dispositions_df`.

### Phase 5: Best Disposition per Loan
*   **Objective**: Pick the single highest priority MTD communication disposition per loan.
*   **Inputs**: `all_call_dispositions_df`, `all_wa_dispositions_df`, `all_blaster_dispositions_df`.
*   **Transformations**: Concentrates all attempt rows using `pl.concat`, sorts by `rank_val` ascending, and selects the first row per `LoanMstID`.
*   **Output**: `best_disposition_df`.

### Phase 10: Latest Outbound Channel Details
*   **Objective**: Determine the latest date, status, and recording for each channel.
*   **Inputs**: `voicebot_mtd_base_df`, `whatsapp_mtd_base_df`, `blaster_mtd_base_df`.
*   **Transformations**: Sorts each table by date descending, grouping by `LoanMstID` and returning the latest entry.
*   **Outputs**: `latest_call_df`, `latest_blaster_df`, `latest_whatsapp_df`.

### Phase 15: Response and Mapping Tables
*   **Objective**: Build MTD response records from interactive customer replies.
*   **Inputs**: `Response`, `base_accounts_df`.
*   **Transformations**: Filters responses within the reporting range, parses P2P (Promise-To-Pay) dates, and splits into channel-specific subsets.
*   **Outputs**: `latest_response_df`, `latest_voicebot_response_df`, `latest_whatsapp_response_df`.

### Phase 16: Latest Master Enrichment Layer
*   **Objective**: Combine base accounts with all parsed channel states.
*   **Inputs**: `base_accounts_df` + all 15 intermediate channel state DataFrames.
*   **Transformations**: Executes a sequence of 18 `LEFT JOIN`s on `LoanMstID` (or `DisbursementID`), then filters out records with invalid primary mobile numbers.
*   **Output**: `latest_master_df`.

### Phase 17: Daily Timelines & Pivots
*   **Objective**: Build chronological daily pivot columns mapping calling status.
*   **Inputs**: `voicebot_mtd_base_df`, `all_call_dispositions_df`, `month_dates_df`.
*   **Transformations**: Extracts the latest call for each date, groups and pivots the columns horizontally matching the requested date range headers.
*   **Output**: `daily_pivots_df`.

### Phase 18: Final Report Dataset
*   **Objective**: Select and order columns matching the target report structure.
*   **Inputs**: `latest_master_df`, `daily_pivots_df`.
*   **Transformations**: Joins pivots onto `latest_master_df`, computes resolved fields (`Collection >=1 EMI`, `Resolve POS`), and orders the columns.
*   **Output**: `final_report_df`.

### Phase 19: Excel Formatting & Export
*   **Objective**: Save the dataset to formatted Excel.
*   **Inputs**: `final_report_df`.
*   **Transformations**: Writes values via `openpyxl`. Sets header styling, freezes rows, enables filters, and resizes columns.
*   **Output**: Written `.xlsx` file.

---

## 🗃️ 8. Intermediate DataFrames Catalog

The following catalog lists the key DataFrames built during pipeline execution:

| DataFrame Name | Primary Key | Purpose | Built In Method |
| :--- | :--- | :--- | :--- |
| `base_accounts_df` | `LoanMstID` | driving base cohort | `build_base_accounts` |
| `all_call_dispositions_df` | `VoiceBotQueueID` | Voicebot calls mapped | `build_all_call_dispositions` |
| `all_wa_dispositions_df` | `WhatsAppQueueID` | WhatsApp messages mapped | `build_all_wa_dispositions` |
| `best_disposition_df` | `LoanMstID` | Highest priority state | `build_best_disposition_per_loan`|
| `latest_call_df` | `LoanMstID` | Latest voice call status | `build_latest_channels` |
| `latest_whatsapp_df` | `LoanMstID` | Latest WhatsApp status | `build_latest_channels` |
| `latest_master_df` | `LoanMstID` | Consolidated database state | `build_latest_master` |
| `daily_pivots_df` | `LoanMstID` | Daily call status pivots | `build_daily_pivots` |
| `final_report_df` | `Account Number` | Sorted report dataset | `build_final_report` |

---

## ⚡ 9. Lazy Execution Strategy

Polars achieves high throughput by using `LazyFrame` structures to delay execution until necessary:

1.  **Lazy Chaining**: Rather than computing tables at each step, methods construct a lazy plan (`self.dataframe.lazy()`).
2.  **Optimizer Engine**: Polars' internal optimizer reviews the plan, applying filter pushdowns, projection selections, and join reorderings automatically.
3.  **Terminal Collect**: Real computation only occurs when `.collect()` is called at the end of a build method.
4.  **Single Collect Invariant**: Each build method materializes its final output DataFrame exactly once, avoiding unnecessary intermediate memory buffers.

---

## 🧪 10. Validation & Quality Strategy

To ensure zero regression compared to PostgreSQL calculations, three verification runners exist:

*   **Unit Regressions (`verify_pipeline.py`)**: Runs the pipeline against simulated records containing edge cases (invalid phones, duplicates, complex transaction splits) and asserts value equality.
*   **Fidelity Certification (`certify_migration.py`)**: Uses Python runtime introspection to check:
    *   Variable reference counts inside code.
    *   Topological order of stage executions.
    *   Output determinism (hashing checks on multiple runs).
    *   High-fidelity Stress Test (running 10,000 active rows in under 0.2 seconds).
*   **Format Verification (`verify_phase19.py`)**: Checks cell styles, bold fonts, frozen rows, sheet name strings, and round-trip cell values.

---

## 📊 11. Excel Export formatting

Excel writing uses `openpyxl` directly. This bypasses pandas and numpy import warning boundaries.

*   **Sheet Naming**: The worksheet is explicitly named `"Fusion Report"`.
*   **Freeze Panes**: The top header row is frozen at `"A2"`.
*   **Header Styling**: Set to bold, text-wrapped, and vertically centered.
*   **Column Widths**: Automatically computed based on the maximum string length of each column's cells plus padding, capped at 50 to prevent run-away widths.
*   **Number Formats**:
    *   Dates: `yyyy-mm-dd`
    *   Datetimes: `yyyy-mm-dd hh:mm:ss`
    *   Floats/Integers: Written directly as native numeric types to allow Excel sorting/filtering.

---

## 📈 12. Performance Benchmarks

*   **Execution Time**: The entire calculation (Phases 1-18) runs in **< 0.1 seconds** for standard datasets and **< 0.25 seconds** for stress datasets containing 10,000 loans.
*   **Memory Footprint**: Average memory usage is under **2 MB** for stress runs due to Polars' arrow-backed memory layouts.

---

## ⚠️ 13. Known Limitations

*   **PostgreSQL Live Comparison**: Live comparison checks in `certify_migration.py` will report as `⏭️ NOT RUN` if credentials to the original PostgreSQL production database are missing.
*   **Large-Scale Export**: For reports exceeding 100,000 rows, `openpyxl` memory usage may increase. A streaming writer option is planned for future optimizations.

---

## 🚀 14. Running the Engine

### Run via Command Line
You can generate a report directly from the workspace root directory:

```bash
python -m fusion_report_engine.run_report
```

### Script Example
```python
from datetime import date
from fusion_report_engine import ReportBuilder, ReportRequest

request = ReportRequest(
    start_date=date(2026, 6, 15),
    end_date=date(2026, 6, 17),
    output_path="Fusion_Report_Output.xlsx"
)

builder = ReportBuilder()
path = builder.generate_report(request)
print(f"Excel report saved: {path}")
```

---

## 💻 15. Developer Guidelines

1.  **Adding a New Step**:
    *   Declare the DataFrame attribute as `Optional[pl.DataFrame] = None` in the `ReportBuilder.__init__` constructor.
    *   Implement the step calculation using `.lazy()`, concluding with a single terminal `.collect()`.
    *   Update `generate_report` to call your new step method.
2.  **Diagnostics**:
    *   Wrap new methods in the `@timer` decorator from `.timer` to profile execution times.
    *   Use `self.logger.info` to log DataFrame shapes, columns, and estimated memory usage.

---

## 🗺️ 16. Future Roadmap

*   **REST API Integration**: Wrap the engine in a FastAPI or Django REST framework endpoint to trigger reports via HTTP.
*   **Streaming Excel Export**: Implement a memory-efficient streaming writer to support reports containing 1,000,000+ rows.
*   **Paginated Preview**: Allow callers to request a JSON-paginated preview of `final_report_df` before writing the Excel file.
