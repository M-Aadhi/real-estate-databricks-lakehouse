"""Gold -> dashboard_data.json export."""
import json


def test_export_dashboard_json(fixture_lakehouse, tmp_path):
    from src.dashboard.export_dashboard import export

    lh, cfg, steps = fixture_lakehouse
    path = export(lh, cfg, out_dir=tmp_path)
    data = json.loads(path.read_text())
    assert set(data) >= {"meta", "kpis", "occupancy_by_property", "revenue_by_property", "value_by_property_type",
                         "lease_expiry_timeline", "collection_trend", "maintenance_by_property", "properties"}
    assert data["meta"]["as_of_date"] == "2026-09-30" and "Synthetic" in data["meta"]["data_note"]
    assert data["kpis"]["occupancy_rate"] == steps["batch_002"]["summary"]["occupancy_rate"]
    assert data["kpis"]["total_properties"] == 2 and len(data["properties"]) == 2
    assert isinstance(data["kpis"]["portfolio_value"], float)            # decimals serialised as numbers
    js = (tmp_path / "dashboard_data.js").read_text()
    assert js.startswith("window.DASHBOARD_DATA = {")                   # works when opened from file://
