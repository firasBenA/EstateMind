import os
import json
from datetime import datetime
from pathlib import Path
from config.settings import settings
from config.logging_config import log

class FileStorage:
    def __init__(self, base_path: Path = settings.RAW_DATA_PATH):
        self.base_path = base_path

    def save_raw_data(self, source_name: str, source_id: str, content: str, extension: str = "html") -> str:
        """
        Saves raw data to file system.
        Structure: data/raw/{source_name}/{YYYY-MM-DD}/{source_id}.{extension}
        Returns the relative path to the file.
        """
        date_str = datetime.now().strftime("%Y-%m-%d")
        directory = self.base_path / source_name / date_str
        directory.mkdir(parents=True, exist_ok=True)
        
        filename = f"{source_id}.{extension}"
        file_path = directory / filename
        
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            # Return path relative to project root or base path, depending on preference.
            # Here returning absolute path as string or relative to raw dir.
            return str(file_path)
        except Exception as e:
            log.error(f"Failed to save raw file {file_path}: {e}")
            return ""

    def load_raw_data(self, file_path: str) -> str:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            log.error(f"Failed to read file {file_path}: {e}")
            return ""
