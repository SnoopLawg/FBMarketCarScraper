

def test_parse_msrp_from_base_price():
    from vin import _parse_msrp
    assert _parse_msrp("27545") == 27545.0
    assert _parse_msrp("25,700.00") == 25700.0
    assert _parse_msrp("$31150") == 31150.0
    assert _parse_msrp(None) is None
    assert _parse_msrp("0") is None          # placeholder/garbage
    assert _parse_msrp("500") is None        # implausibly low → reject
