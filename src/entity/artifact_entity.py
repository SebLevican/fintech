from dataclasses import dataclass
from typing import Dict
import pandas as pd

@dataclass
class DataIngestionArtifact:
    files: Dict[str,pd.DataFrame]