"""
tests/test_macro_scrapers.py

Tests for BCTScraper, INSScraper, BVMTScraper — macro / time-series scrapers.

Strategy (mirrors existing test_darcom_parser.py / test_tecnocasa_parser.py):
  - Zero network calls — every test uses inline HTML fixtures
  - Parser methods tested directly with BeautifulSoup
  - DB layer (_get_macro_db, _upsert_macro) tested against a temp file DB
  - run() tested end-to-end by monkeypatching _get() to return fake Responses

Run from EstateMindPi/data/:
    pytest tests/test_macro_scrapers.py -v
    pytest tests/test_macro_scrapers.py -v -k "BCT"       # BCT only
    pytest tests/test_macro_scrapers.py -v -k "INS"       # INS only
    pytest tests/test_macro_scrapers.py -v -k "BVMT"      # BVMT only
    pytest tests/test_macro_scrapers.py -v -k "DB"        # DB layer only
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from bs4 import BeautifulSoup

from scrapers.all_scrapers import (
    BCTScraper,
    INSScraper,
    BVMTScraper,
    _get_macro_db,
    _upsert_macro,
    run_all_macro_scrapers,
)


# =============================================================================
# SHARED HELPERS
# =============================================================================

def _fake_response(html: str, status_code: int = 200) -> MagicMock:
    """Build a fake requests.Response with a given HTML body."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = html
    resp.raise_for_status = MagicMock()
    return resp


# =============================================================================
# HTML FIXTURES  (exact HTML that the real sites produce)
# =============================================================================

BCT_TAUX_HTML = """
<table>
  <tr><th>Periode</th><th>Taux (%)</th></tr>
  <tr><td>2024-01</td><td>8,00</td></tr>
  <tr><td>2023-10</td><td>7,75</td></tr>
  <tr><td>2023-01</td><td>7,00</td></tr>
</table>
"""

BCT_MIXED_HTML = """
<table>
  <tr><th>Periode</th><th>Valeur</th></tr>
  <tr><td>N/A</td><td>n.d.</td></tr>
  <tr><td>2024-06</td><td>8,25</td></tr>
  <tr><td></td><td></td></tr>
</table>
"""

BCT_CREDITS_HTML = """
<table>
  <tr><th>Periode</th><th>Credits immobiliers (MDT)</th><th>Autres credits</th></tr>
  <tr><td>2024-01</td><td>14 523</td><td>8 200</td></tr>
  <tr><td>2023-12</td><td>14 310</td><td>8 050</td></tr>
</table>
"""

BCT_EUR_HTML = """
<table>
  <tr><th>Periode</th><th>USD</th><th>EUR</th><th>GBP</th></tr>
  <tr><td>2024-01</td><td>3,112</td><td>3,380</td><td>3,950</td></tr>
  <tr><td>2023-12</td><td>3,095</td><td>3,362</td><td>3,931</td></tr>
</table>
"""

INS_IPC_HTML = """
<table>
  <tr><th>Periode</th><th>IPC Construction (base 2015=100)</th></tr>
  <tr><td>2024-03</td><td>156,2</td></tr>
  <tr><td>2024-02</td><td>155,8</td></tr>
  <tr><td>2024-01</td><td>154,9</td></tr>
</table>
"""

INS_PERMIS_HTML = """
<table>
  <tr><th>Annee</th><th>Permis accordes</th><th>Superficie</th></tr>
  <tr><td>2023</td><td>42 150</td><td>3 200 000</td></tr>
  <tr><td>2022</td><td>39 800</td><td>3 050 000</td></tr>
</table>
"""

BVMT_TUNINDEX_HTML = """
<table>
  <tr><th>Date</th><th>TUNINDEX</th><th>Volume</th></tr>
  <tr><td>09/04/2026</td><td>9 254,32</td><td>12 000 000</td></tr>
  <tr><td>08/04/2026</td><td>9 198,11</td><td>11 500 000</td></tr>
</table>
"""

BVMT_HISTO_HTML = """
<table>
  <tr><th>Date</th><th>Valeur</th></tr>
  <tr><td>07/04/2026</td><td>9 150,00</td></tr>
  <tr><td>04/04/2026</td><td>9 100,50</td></tr>
</table>
"""

BVMT_STOCKS_HTML = """
<table>
  <tr><th>Valeur</th><th>Cours</th><th>Variation</th></tr>
  <tr><td>SAH</td><td>3,250</td><td>+0,5%</td></tr>
  <tr><td>SITS</td><td>12,400</td><td>-0,2%</td></tr>
  <tr><td>SPDIT</td><td>8,750</td><td>+0,1%</td></tr>
  <tr><td>SIM</td><td>5,100</td><td>0,0%</td></tr>
  <tr><td>AUTRE</td><td>1,000</td><td>0,0%</td></tr>
</table>
"""

BVMT_NO_TUNINDEX_HTML = """
<table>
  <tr><th>Valeur</th><th>Cours</th></tr>
  <tr><td>SAH</td><td>3,250</td></tr>
</table>
"""


# =============================================================================
# TestBCTScraper
# =============================================================================

class TestBCTScraper:

    # ------------------------------------------------------------------
    # _normalise_date
    # ------------------------------------------------------------------

    def test_normalise_date_yyyy_mm_dash(self):
        assert BCTScraper._normalise_date("2024-01") == "2024-01-01"

    def test_normalise_date_yyyy_mm_slash(self):
        assert BCTScraper._normalise_date("2024/03") == "2024-03-01"

    def test_normalise_date_mm_slash_yyyy(self):
        assert BCTScraper._normalise_date("03/2024") == "2024-03-01"

    def test_normalise_date_annual(self):
        assert BCTScraper._normalise_date("2024") == "2024-01-01"

    def test_normalise_date_quarter_T1(self):
        assert BCTScraper._normalise_date("T1-2024") == "2024-01-01"

    def test_normalise_date_quarter_T2(self):
        assert BCTScraper._normalise_date("T2-2024") == "2024-04-01"

    def test_normalise_date_quarter_T3(self):
        assert BCTScraper._normalise_date("T3-2023") == "2023-07-01"

    def test_normalise_date_quarter_T4(self):
        assert BCTScraper._normalise_date("T4-2022") == "2022-10-01"

    def test_normalise_date_garbage_returns_none(self):
        assert BCTScraper._normalise_date("garbage") is None

    def test_normalise_date_empty_returns_none(self):
        assert BCTScraper._normalise_date("") is None

    # ------------------------------------------------------------------
    # _parse_bct_table
    # ------------------------------------------------------------------

    def test_parse_bct_table_returns_correct_row_count(self):
        soup = BeautifulSoup(BCT_TAUX_HTML, "html.parser")
        assert len(BCTScraper()._parse_bct_table(soup)) == 3

    def test_parse_bct_table_dates_normalised(self):
        soup = BeautifulSoup(BCT_TAUX_HTML, "html.parser")
        dates = [d for d, _ in BCTScraper()._parse_bct_table(soup)]
        assert dates == ["2024-01-01", "2023-10-01", "2023-01-01"]

    def test_parse_bct_table_comma_decimal_parsed(self):
        soup = BeautifulSoup(BCT_TAUX_HTML, "html.parser")
        vals = [v for _, v in BCTScraper()._parse_bct_table(soup)]
        assert vals == [8.0, 7.75, 7.0]

    def test_parse_bct_table_skips_unparseable_rows(self):
        soup = BeautifulSoup(BCT_MIXED_HTML, "html.parser")
        result = BCTScraper()._parse_bct_table(soup)
        assert len(result) == 1
        assert result[0] == ("2024-06-01", 8.25)

    def test_parse_bct_table_empty_html_returns_empty_list(self):
        soup = BeautifulSoup("<html></html>", "html.parser")
        assert BCTScraper()._parse_bct_table(soup) == []

    # ------------------------------------------------------------------
    # _scrape_credits_immobiliers
    # ------------------------------------------------------------------

    def test_scrape_credits_finds_immobilier_column(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_CREDITS_HTML)
        rows = scraper._scrape_credits_immobiliers()
        assert len(rows) == 2

    def test_scrape_credits_space_thousands_parsed(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_CREDITS_HTML)
        rows = scraper._scrape_credits_immobiliers()
        assert rows[0]["value"] == 14523.0
        assert rows[1]["value"] == 14310.0

    def test_scrape_credits_correct_indicator_key(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_CREDITS_HTML)
        rows = scraper._scrape_credits_immobiliers()
        assert all(r["indicator"] == "credits_immobiliers" for r in rows)

    def test_scrape_credits_correct_unit(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_CREDITS_HTML)
        rows = scraper._scrape_credits_immobiliers()
        assert all(r["unit"] == "MDT" for r in rows)

    # ------------------------------------------------------------------
    # _scrape_exchange_rate
    # ------------------------------------------------------------------

    def test_scrape_exchange_rate_finds_eur_column(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_EUR_HTML)
        rows = scraper._scrape_exchange_rate()
        assert len(rows) == 2

    def test_scrape_exchange_rate_correct_value(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_EUR_HTML)
        rows = scraper._scrape_exchange_rate()
        assert rows[0]["value"] == 3.38
        assert rows[0]["indicator"] == "tnd_eur"

    def test_scrape_exchange_rate_no_eur_column_returns_empty(self):
        html = "<table><tr><th>Date</th><th>USD</th></tr><tr><td>2024-01</td><td>3,1</td></tr></table>"
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(html)
        assert scraper._scrape_exchange_rate() == []

    # ------------------------------------------------------------------
    # _scrape_taux_directeur / _scrape_ipc — indicator metadata
    # ------------------------------------------------------------------

    def test_scrape_taux_directeur_indicator_key(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        rows = scraper._scrape_taux_directeur()
        assert all(r["indicator"] == "taux_directeur" for r in rows)

    def test_scrape_taux_directeur_source_is_bct(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        rows = scraper._scrape_taux_directeur()
        assert all(r["source"] == "BCT" for r in rows)

    def test_scrape_taux_directeur_unit_is_percent(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        rows = scraper._scrape_taux_directeur()
        assert all(r["unit"] == "%" for r in rows)

    def test_scrape_ipc_indicator_key(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        rows = scraper._scrape_ipc()
        assert all(r["indicator"] == "ipc_general" for r in rows)

    # ------------------------------------------------------------------
    # _get returns None → sub-scrapers return []
    # ------------------------------------------------------------------

    def test_scrape_taux_returns_empty_when_get_fails(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_taux_directeur() == []

    def test_scrape_ipc_returns_empty_when_get_fails(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_ipc() == []

    def test_scrape_credits_returns_empty_when_get_fails(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_credits_immobiliers() == []

    def test_scrape_exchange_returns_empty_when_get_fails(self):
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_exchange_rate() == []

    # ------------------------------------------------------------------
    # run() — end-to-end with monkeypatched _get
    # ------------------------------------------------------------------

    def test_run_returns_empty_status_when_all_get_fail(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: None
        result = scraper.run()
        assert result["status"] == "empty"
        assert result["rows"] == 0
        assert result["source"] == "BCT"

    def test_run_returns_ok_status_when_data_collected(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        result = scraper.run()
        assert result["status"] == "ok"
        assert result["rows"] > 0

    def test_run_writes_rows_to_sqlite(self, monkeypatch, tmp_path):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", db_path)
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        scraper.run()
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM macro_indicators").fetchall()
        conn.close()
        assert len(rows) > 0

    def test_run_does_not_duplicate_on_second_call(self, monkeypatch, tmp_path):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", db_path)
        scraper = BCTScraper()
        scraper._get = lambda url, **kw: _fake_response(BCT_TAUX_HTML)
        scraper.run()
        count_after_first = sqlite3.connect(str(db_path)).execute(
            "SELECT COUNT(*) FROM macro_indicators"
        ).fetchone()[0]
        scraper.run()
        count_after_second = sqlite3.connect(str(db_path)).execute(
            "SELECT COUNT(*) FROM macro_indicators"
        ).fetchone()[0]
        assert count_after_first == count_after_second


# =============================================================================
# TestINSScraper
# =============================================================================

class TestINSScraper:

    # ------------------------------------------------------------------
    # _parse_ins_table
    # ------------------------------------------------------------------

    def test_parse_ins_table_returns_correct_count(self):
        soup = BeautifulSoup(INS_IPC_HTML, "html.parser")
        assert len(INSScraper()._parse_ins_table(soup)) == 3

    def test_parse_ins_table_dates_correct(self):
        soup = BeautifulSoup(INS_IPC_HTML, "html.parser")
        dates = [d for d, _ in INSScraper()._parse_ins_table(soup)]
        assert dates == ["2024-03-01", "2024-02-01", "2024-01-01"]

    def test_parse_ins_table_values_correct(self):
        soup = BeautifulSoup(INS_IPC_HTML, "html.parser")
        vals = [v for _, v in INSScraper()._parse_ins_table(soup)]
        assert vals == [156.2, 155.8, 154.9]

    def test_parse_ins_table_empty_returns_empty(self):
        soup = BeautifulSoup("<html></html>", "html.parser")
        assert INSScraper()._parse_ins_table(soup) == []

    # ------------------------------------------------------------------
    # _scrape_ipc_construction — key indicator
    # ------------------------------------------------------------------

    def test_scrape_ipc_construction_indicator_key(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_IPC_HTML)
        rows = scraper._scrape_ipc_construction()
        assert all(r["indicator"] == "ipc_construction" for r in rows)

    def test_scrape_ipc_construction_source_is_ins(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_IPC_HTML)
        rows = scraper._scrape_ipc_construction()
        assert all(r["source"] == "INS" for r in rows)

    def test_scrape_ipc_construction_unit(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_IPC_HTML)
        rows = scraper._scrape_ipc_construction()
        assert all(r["unit"] == "index_2015=100" for r in rows)

    def test_scrape_ipc_construction_values(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_IPC_HTML)
        rows = scraper._scrape_ipc_construction()
        assert rows[0]["value"] == 156.2
        assert rows[1]["value"] == 155.8

    # ------------------------------------------------------------------
    # _scrape_permis — annual dates, count values, space-thousands
    # ------------------------------------------------------------------

    def test_scrape_permis_finds_permis_column(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_PERMIS_HTML)
        rows = scraper._scrape_permis()
        assert len(rows) == 2

    def test_scrape_permis_space_thousands_parsed(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_PERMIS_HTML)
        rows = scraper._scrape_permis()
        assert rows[0]["value"] == 42150.0
        assert rows[1]["value"] == 39800.0

    def test_scrape_permis_annual_date_normalised(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_PERMIS_HTML)
        rows = scraper._scrape_permis()
        assert rows[0]["date"] == "2023-01-01"
        assert rows[1]["date"] == "2022-01-01"

    def test_scrape_permis_indicator_key(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_PERMIS_HTML)
        rows = scraper._scrape_permis()
        assert all(r["indicator"] == "permis_construire" for r in rows)

    def test_scrape_permis_unit_is_count(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_PERMIS_HTML)
        rows = scraper._scrape_permis()
        assert all(r["unit"] == "count" for r in rows)

    # ------------------------------------------------------------------
    # _get returns None → sub-scrapers return []
    # ------------------------------------------------------------------

    def test_scrape_ipc_construction_returns_empty_when_get_fails(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_ipc_construction() == []

    def test_scrape_ipc_general_returns_empty_when_get_fails(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_ipc_general() == []

    def test_scrape_pib_returns_empty_when_get_fails(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_pib() == []

    def test_scrape_permis_returns_empty_when_get_fails(self):
        scraper = INSScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_permis() == []

    # ------------------------------------------------------------------
    # run() — end-to-end
    # ------------------------------------------------------------------

    def test_run_empty_when_all_fail(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        scraper = INSScraper()
        scraper._get = lambda url, **kw: None
        result = scraper.run()
        assert result["status"] == "empty"
        assert result["source"] == "INS"

    def test_run_ok_when_data_collected(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_IPC_HTML)
        result = scraper.run()
        assert result["status"] == "ok"
        assert result["rows"] > 0

    def test_run_writes_ipc_construction_to_db(self, monkeypatch, tmp_path):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", db_path)
        scraper = INSScraper()
        scraper._get = lambda url, **kw: _fake_response(INS_IPC_HTML)
        scraper.run()
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT * FROM macro_indicators WHERE indicator='ipc_construction'"
        ).fetchall()
        conn.close()
        assert len(rows) == 3  # 3 months in fixture


# =============================================================================
# TestBVMTScraper
# =============================================================================

class TestBVMTScraper:

    # ------------------------------------------------------------------
    # _scrape_tunindex
    # ------------------------------------------------------------------

    def test_scrape_tunindex_finds_tunindex_column(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML)
        rows = scraper._scrape_tunindex()
        assert len(rows) == 2

    def test_scrape_tunindex_dd_mm_yyyy_date_parsed(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML)
        rows = scraper._scrape_tunindex()
        assert rows[0]["date"] == "2026-04-01"

    def test_scrape_tunindex_comma_thousands_parsed(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML)
        rows = scraper._scrape_tunindex()
        assert rows[0]["value"] == 9254.32
        assert rows[1]["value"] == 9198.11

    def test_scrape_tunindex_indicator_key(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML)
        rows = scraper._scrape_tunindex()
        assert all(r["indicator"] == "tunindex" for r in rows)

    def test_scrape_tunindex_source_is_bvmt(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML)
        rows = scraper._scrape_tunindex()
        assert all(r["source"] == "BVMT" for r in rows)

    def test_scrape_tunindex_unit_is_points(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML)
        rows = scraper._scrape_tunindex()
        assert all(r["unit"] == "points" for r in rows)

    # ------------------------------------------------------------------
    # _scrape_tunindex_historical — fallback parser
    # ------------------------------------------------------------------

    def test_scrape_tunindex_historical_dd_mm_yyyy_parsed(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_HISTO_HTML)
        rows = scraper._scrape_tunindex_historical()
        assert rows[0]["date"] == "2026-04-01"
        assert rows[0]["value"] == 9150.0

    def test_scrape_tunindex_historical_returns_empty_when_get_fails(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_tunindex_historical() == []

    def test_scrape_tunindex_falls_back_to_historical_when_no_tunindex_header(self):
        scraper = BVMTScraper()

        def fake_get(url, **kw):
            if "historique" in url:
                return _fake_response(BVMT_HISTO_HTML)
            return _fake_response(BVMT_NO_TUNINDEX_HTML)  # no TUNINDEX column

        scraper._get = fake_get
        rows = scraper._scrape_tunindex()
        # Fallback path returns 2 rows from BVMT_HISTO_HTML
        assert len(rows) == 2

    # ------------------------------------------------------------------
    # _scrape_re_stocks
    # ------------------------------------------------------------------

    def test_scrape_re_stocks_extracts_four_tickers(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_STOCKS_HTML)
        rows = scraper._scrape_re_stocks()
        indicators = {r["indicator"] for r in rows}
        assert indicators == {"sah_price", "sits_price", "spdit_price", "sim_price"}

    def test_scrape_re_stocks_non_re_ticker_excluded(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_STOCKS_HTML)
        rows = scraper._scrape_re_stocks()
        indicators = {r["indicator"] for r in rows}
        assert "autre_price" not in indicators

    def test_scrape_re_stocks_sah_price_correct(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_STOCKS_HTML)
        rows = scraper._scrape_re_stocks()
        sah = next(r for r in rows if r["indicator"] == "sah_price")
        assert sah["value"] == 3.25

    def test_scrape_re_stocks_sits_price_correct(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_STOCKS_HTML)
        rows = scraper._scrape_re_stocks()
        sits = next(r for r in rows if r["indicator"] == "sits_price")
        assert sits["value"] == 12.4

    def test_scrape_re_stocks_unit_is_tnd(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_STOCKS_HTML)
        rows = scraper._scrape_re_stocks()
        assert all(r["unit"] == "TND" for r in rows)

    def test_scrape_re_stocks_returns_empty_when_get_fails(self):
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: None
        assert scraper._scrape_re_stocks() == []

    # ------------------------------------------------------------------
    # run() — end-to-end
    # ------------------------------------------------------------------

    def test_run_empty_when_all_fail(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: None
        result = scraper.run()
        assert result["status"] == "empty"
        assert result["source"] == "BVMT"

    def test_run_ok_when_data_collected(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML + BVMT_STOCKS_HTML)
        result = scraper.run()
        assert result["status"] == "ok"
        assert result["rows"] > 0

    def test_run_writes_tunindex_to_db(self, monkeypatch, tmp_path):
        db_path = tmp_path / "test.db"
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", db_path)
        scraper = BVMTScraper()
        scraper._get = lambda url, **kw: _fake_response(BVMT_TUNINDEX_HTML + BVMT_STOCKS_HTML)
        scraper.run()
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT * FROM macro_indicators WHERE indicator='tunindex'"
        ).fetchall()
        conn.close()
        assert len(rows) > 0


# =============================================================================
# TestMacroDB  — _get_macro_db and _upsert_macro
# =============================================================================

class TestMacroDB:

    def test_get_macro_db_creates_table(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn = _get_macro_db()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        assert ("macro_indicators",) in tables

    def test_get_macro_db_table_has_correct_columns(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn = _get_macro_db()
        cols = [row[1] for row in conn.execute("PRAGMA table_info(macro_indicators)").fetchall()]
        conn.close()
        for expected in ["id", "date", "indicator", "value", "unit", "source", "scraped_at"]:
            assert expected in cols

    def test_upsert_macro_inserts_rows(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn = _get_macro_db()
        rows = [
            {"date": "2024-01-01", "indicator": "taux_directeur", "value": 8.0, "unit": "%", "source": "BCT"},
            {"date": "2024-01-01", "indicator": "ipc_general", "value": 162.3, "unit": "index_2015=100", "source": "BCT"},
        ]
        count = _upsert_macro(conn, rows)
        conn.close()
        assert count == 2

    def test_upsert_macro_deduplicates_same_date_and_indicator(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn = _get_macro_db()
        row = {"date": "2024-01-01", "indicator": "taux_directeur", "value": 8.0, "unit": "%", "source": "BCT"}
        _upsert_macro(conn, [row])
        _upsert_macro(conn, [row])  # same key, second insert
        total = conn.execute("SELECT COUNT(*) FROM macro_indicators").fetchone()[0]
        conn.close()
        assert total == 1

    def test_upsert_macro_updates_value_on_conflict(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn = _get_macro_db()
        _upsert_macro(conn, [{"date": "2024-01-01", "indicator": "taux_directeur", "value": 8.0}])
        _upsert_macro(conn, [{"date": "2024-01-01", "indicator": "taux_directeur", "value": 8.5}])
        val = conn.execute(
            "SELECT value FROM macro_indicators WHERE indicator='taux_directeur' AND date='2024-01-01'"
        ).fetchone()[0]
        conn.close()
        assert val == 8.5

    def test_upsert_macro_returns_count_inserted(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn = _get_macro_db()
        rows = [
            {"date": "2024-01-01", "indicator": "taux_directeur", "value": 8.0},
            {"date": "2024-02-01", "indicator": "taux_directeur", "value": 8.0},
            {"date": "2024-03-01", "indicator": "taux_directeur", "value": 8.0},
        ]
        assert _upsert_macro(conn, rows) == 3
        conn.close()

    def test_get_macro_db_is_idempotent(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")
        conn1 = _get_macro_db()
        conn1.close()
        conn2 = _get_macro_db()  # table already exists — should not raise
        tables = conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn2.close()
        assert ("macro_indicators",) in tables


# =============================================================================
# TestRunAllMacroScrapers
# =============================================================================

class TestRunAllMacroScrapers:

    def test_returns_list_of_three_results(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")

        def patched_run(self):
            return {"source": self.SOURCE, "rows": 0, "status": "empty"}

        monkeypatch.setattr(BCTScraper, "run", patched_run)
        monkeypatch.setattr(INSScraper, "run", patched_run)
        monkeypatch.setattr(BVMTScraper, "run", patched_run)

        results = run_all_macro_scrapers()
        assert len(results) == 3

    def test_result_sources_are_bct_ins_bvmt_in_order(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")

        def patched_run(self):
            return {"source": self.SOURCE, "rows": 0, "status": "empty"}

        monkeypatch.setattr(BCTScraper, "run", patched_run)
        monkeypatch.setattr(INSScraper, "run", patched_run)
        monkeypatch.setattr(BVMTScraper, "run", patched_run)

        results = run_all_macro_scrapers()
        assert [r["source"] for r in results] == ["BCT", "INS", "BVMT"]

    def test_crashed_scraper_does_not_stop_others(self, monkeypatch, tmp_path):
        monkeypatch.setattr("scrapers.all_scrapers._MACRO_DB_PATH", tmp_path / "test.db")

        def crash(self):
            raise RuntimeError("site down")

        def ok(self):
            return {"source": self.SOURCE, "rows": 5, "status": "ok"}

        monkeypatch.setattr(BCTScraper, "run", crash)
        monkeypatch.setattr(INSScraper, "run", ok)
        monkeypatch.setattr(BVMTScraper, "run", ok)

        results = run_all_macro_scrapers()
        assert len(results) == 3
        assert results[0]["status"].startswith("error")
        assert results[1]["status"] == "ok"
        assert results[2]["status"] == "ok"