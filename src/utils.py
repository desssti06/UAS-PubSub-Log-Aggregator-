import logging
from datetime import datetime,  timezone

def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

from datetime import datetime, timezone

def uptime_seconds(start_time: datetime) -> float:
    now = datetime.now(timezone.utc)
    return (now - start_time).total_seconds()
