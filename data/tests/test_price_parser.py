from core.base_scraper import parse_tunisian_price


def test_parse_tunisian_price_space_thousands():
    assert parse_tunisian_price("450 000 DT") == 450000.0


def test_parse_tunisian_price_mdt_millions_int():
    assert parse_tunisian_price("2 MDT") == 2_000_000.0


def test_parse_tunisian_price_mdt_millions_decimal():
    assert parse_tunisian_price("2,5 MDT") == 2_500_000.0


def test_parse_tunisian_price_mille_thousands():
    assert parse_tunisian_price("450 mille") == 450_000.0

