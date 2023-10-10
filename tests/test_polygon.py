import pytest
from lib.utils import get_polygon_key
from lib.polygon import make_url, validate, make_urls

def test_make_url_sanity():
    url = make_url('AAPL', '2022-01-01', '2022-01-31', api_key='test_key')
    assert 'polygon.io' and 'AAPL' in url, "url should contain 'polygon.io' and 'AAPL' and dosent"

def test_make_url_invalid_ticker_type():
    with pytest.raises(AssertionError):
        make_url(12345, '2022-01-01', '2022-01-31', api_key='test_key')

def test_validate():
    invalid_lists = [
        [], # empty list
        None, # None
        12345, # wrong type
        [1, "AAPL", False] # wrong type
    ]
    for invalid_list in invalid_lists:
        with pytest.raises(AssertionError):
            validate(invalid_list)

def test_make_urls():
    pass