import logging
from config.config import get_gsheetes

logger = logging.getLogger(__name__)


def test_gsheets_config(context, clean_redis):
    try:
        gc = get_gsheetes()
    except Exception as e:
        assert False, 'Google Sheets configuration does not seem to be set up correctly'
