import logging
from typing import Dict
from processing.helpers import Context

logger = logging.getLogger(__name__)


def limit(context: Context, event: Dict) -> bool:
    return False
