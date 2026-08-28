from pathlib import Path


def test_dashboard_exposes_core_operational_evidence() -> None:
    html = (Path(__file__).parents[1] / "src/mobility_flow/static/index.html").read_text()
    for phrase in (
        "Data Freshness",
        "DQ / dbt",
        "END-TO-END LINEAGE",
        "REPLAYABLE OPERATIONS",
        "SYNTHETIC DATA",
    ):
        assert phrase in html
