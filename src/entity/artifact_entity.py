from dataclasses import dataclass
from typing import Dict, Optional
import pandas as pd

@dataclass
class DataIngestionArtifact:
    files: Dict[str,pd.DataFrame]


@dataclass
class DataValidationArtifact:
    validation_status: bool
    drift_report_file_path: Optional
    dataframes: Dict[str,pd.DataFrame]


@dataclass
class DataTransformationArtifact:
    dataframes: Dict[str,pd.DataFrame]