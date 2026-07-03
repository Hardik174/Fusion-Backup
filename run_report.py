import time
from pathlib import Path

from fusion_report_engine import ReportBuilder, ReportRequest

request = ReportRequest(
    start_date="2026-06-01",
    end_date="2026-06-30",
    output_path="outputs/Fusion_Report_June.xlsx"
)

builder = ReportBuilder()

start = time.perf_counter()

excel_path = builder.generate_report(request)

end = time.perf_counter()

print("=" * 60)
print(f"Excel generated at : {excel_path}")
print(f"Total execution time : {end - start:.2f} seconds")
print("=" * 60)