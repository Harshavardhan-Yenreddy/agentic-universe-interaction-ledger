"""
Tests for Command Line Interface (CLI).
Tests init-db, ingest, report, pipeline subcommands and idempotency.
"""

from pathlib import Path
import pytest

from ledger.cli import main


def test_cli_init_db(tmp_path):
    """Test 'init-db' command initializes database."""
    db_file = tmp_path / "cli_init.db"
    ret = main(["--db", str(db_file), "init-db"])
    assert ret == 0
    assert db_file.is_file()


def test_cli_ingest_and_report(tmp_path):
    """Test 'ingest' and 'report' commands end-to-end via CLI."""
    if not Path("data/drop_1").is_dir():
        pytest.skip("data/drop_1 not found")

    db_file = tmp_path / "cli_test.db"
    reports_dir = tmp_path / "reports_cli"

    # Ingest drop 1
    ret1 = main(["--db", str(db_file), "ingest", "data/drop_1"])
    assert ret1 == 0

    # Ingest drop 2
    ret2 = main(["--db", str(db_file), "ingest", "data/drop_2"])
    assert ret2 == 0

    # Report
    ret3 = main(["--db", str(db_file), "report", "--out", str(reports_dir)])
    assert ret3 == 0

    # Verify all 5 files exist
    assert (reports_dir / "batch_summary.csv").is_file()
    assert (reports_dir / "daily_overview.csv").is_file()
    assert (reports_dir / "lead_report.csv").is_file()
    assert (reports_dir / "rejections.csv").is_file()
    assert (reports_dir / "data_quality.json").is_file()


def test_cli_pipeline_command(tmp_path):
    """Test all-in-one 'pipeline' command."""
    if not Path("data/drop_1").is_dir() or not Path("data/drop_2").is_dir():
        pytest.skip("Generated dataset not found")

    db_file = tmp_path / "pipeline.db"
    reports_dir = tmp_path / "pipeline_reports"

    ret = main([
        "--db", str(db_file),
        "pipeline",
        "--drops", "data/drop_1", "data/drop_2",
        "--out", str(reports_dir),
    ])
    assert ret == 0
    assert (reports_dir / "batch_summary.csv").is_file()
    assert (reports_dir / "data_quality.json").is_file()


def test_cli_idempotent_reingest(tmp_path):
    """Test re-running ingest via CLI skips files without errors."""
    if not Path("data/drop_1").is_dir():
        pytest.skip("data/drop_1 not found")

    db_file = tmp_path / "idempotent.db"

    # Run 1
    assert main(["--db", str(db_file), "ingest", "data/drop_1"]) == 0
    # Run 2 (re-ingest)
    assert main(["--db", str(db_file), "ingest", "data/drop_1"]) == 0


def test_cli_invalid_drop_dir(tmp_path):
    """Test that a non-existent drop directory fails safely with non-zero exit code."""
    ret = main(["ingest", "non_existent_drop_dir_12345"])
    assert ret != 0

