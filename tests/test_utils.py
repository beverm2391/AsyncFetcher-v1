from lib.utils import get_polygon_key

def test_get_polygon_key():
    """Test get_polygon_key()."""
    assert get_polygon_key() is not None, "polygon key should not be None. Check `get_polygon_key()` in `utils.py`"

def test_get_nyse_date_tups():
    #  make sure no weekdays or holidays
    pass