from APIMODULE import WeatherAPI
from DataProcessingModule import WeatherDataProcessor
from DataStorageModule import WeatherDatabase
from typing import List
import logging
# from LoggerConfig import loggerfrom LoggerConfig import logger
from LoggerConfig import logger
import pandas as pd


class WeatherSystem:
    """Main class that orchestrates the entire weather data system."""
    
    def __init__(self, db_path: str = "weather_data.db"):
        self.processor = WeatherDataProcessor()
        self.database = WeatherDatabase(db_path)
        self.api = WeatherAPI(db_path)
    
    def process_and_store_data_file(self, file_path: str, sheet_name: str = None):
        """Complete pipeline: load, process, and store data from CSV or Excel file."""
        try:
            logger.info("Starting data processing pipeline...")
            
            # Load data file (CSV or Excel)
            raw_data = self.processor.load_data_file(file_path, sheet_name)
            logger.info(f"Loaded {len(raw_data)} rows from file")
            
            # Process data
            processed_data = self.processor.clean_and_transform_data(raw_data)
            
            # Store in database
            self.database.store_data(processed_data)
            
            # Get summary
            summary = self.processor.get_data_summary()
            logger.info("Data processing pipeline completed successfully")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}")
            raise
    
    def list_excel_sheets(self, file_path: str) -> List[str]:
        """List all sheets in an Excel file."""
        try:
            excel_file = pd.ExcelFile(file_path)
            return excel_file.sheet_names
        except Exception as e:
            logger.error(f"Error reading Excel sheets: {e}")
            return []
