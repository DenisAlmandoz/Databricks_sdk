from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import pandas as pd


class BaseIngestor(ABC):
    def __init__(self, lookback_days: int = 30):
        self.lookback_days = lookback_days
        self.ingestion_ts = datetime.utcnow().isoformat()

    @abstractmethod
    def ingest(self) -> dict[str, pd.DataFrame]:
        ...
