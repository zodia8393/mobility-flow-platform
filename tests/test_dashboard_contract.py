from pathlib import Path


def test_dashboard_exposes_live_source_and_operational_controls() -> None:
    html = (Path(__file__).parents[1] / "src/mobility_flow/static/index.html").read_text()
    for phrase in (
        "Data Freshness",
        "SEOUL OPEN DATA · LIVE",
        "서울시 실시간 도시데이터 수집 현황",
        "DATA LINEAGE",
        "PIPELINE HISTORY",
    ):
        assert phrase in html

    assert "PORTFOLIO CONTINUITY" not in html
    assert "SYNTHETIC DATA" not in html
