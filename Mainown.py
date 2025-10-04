import pandas as pd
import logging
from pathlib import Path
from APIMODULE import WeatherAPI
from DataProcessingModule import WeatherDataProcessor
from DataStorageModule import WeatherDatabase
from typing import List
from MainORCHESTRATION import WeatherSystem
import json
import logging
from LoggerConfig import logger



def main():
    """Example usage of the weather system with Excel/CSV files."""
    
    # Initialize the system
    weather_system = WeatherSystem()
    
    # Example 1: Check available sheets in Excel file
    print("=== Available Excel Sheets ===")
    excel_file_path = r"C:\Users\heman\Downloads\a271ffc7-fd1e-45ee-8288-684c6a77833e\Assessment 2 Weather Data V.1.0-20250326T055840Z-001\Assessment 2 Weather Data V.1.0\Weather data assessment V.1.0\Assessment 2\testset.xlsx"
  # Replace with your Excel file path
    sheets = weather_system.list_excel_sheets(excel_file_path)
    if sheets:
        print("Available sheets:")
        for i, sheet in enumerate(sheets):
            print(f"  {i+1}. {sheet}")
    
    # Example 2: Process Excel file (uncomment when you have the file)
    # Option A: Load from specific sheet
    summary = weather_system.process_and_store_data_file(excel_file_path, sheet_name="testset")
    print("Data Processing Summary:", summary)
    
    # Option B: Auto-detect sheet (loads first/most relevant sheet)
    # summary = weather_system.process_and_store_data_file("delhi_weather.xlsx")
    
    # Option C: Process CSV file
    # summary = weather_system.process_and_store_data_file("delhi_weather.csv")
    
    # print("Data Processing Summary:", json.dumps(summary, indent=2))
    
    # Example 3: API Usage Examples (after data is loaded)
    api = weather_system.api
    
    # Get weather for a specific date
    print("\n=== Weather for specific date ===")
    weather_data = api.get_weather_by_date("2010-01-15")
    if weather_data:
        print(f"Found {len(weather_data)} records for 2010-01-15")
        print("Sample record:", json.dumps(weather_data[0] if weather_data else {}, indent=2))
    
    # Get weather for a specific month across all years
    print("\n=== Weather for January across all years ===")
    january_data = api.get_weather_by_month(1)
    print(f"Found {len(january_data)} records for January")
    
    # Get weather for a specific month and year
    print("\n=== Weather for January 2010 ===")
    jan_2010_data = api.get_weather_by_month(1, 2010)
    print(f"Found {len(jan_2010_data)} records for January 2010")
    
    # Get temperature statistics for a year
    print("\n=== Temperature statistics for 2010 ===")
    temp_stats = api.get_monthly_temperature_stats(2010)
    if temp_stats:
        print("Sample month stats:", json.dumps(temp_stats.get(1, {}), indent=2))
    
    # Get weather events summary
    print("\n=== Weather events summary for 2010 ===")
    events = api.get_weather_events_summary(2010)
    print(json.dumps(events, indent=2))
    
    # Get extreme weather days
    print("\n=== Extreme weather days for 2010 ===")
    extreme_days = api.get_extreme_weather_days(2010, temperature_threshold=40.0)
    print(f"Hot days (>40°C): {len(extreme_days.get('hot_days', []))}")
    print(f"Cold days (<5°C): {len(extreme_days.get('cold_days', []))}")
    print(f"Very humid days (>90%): {len(extreme_days.get('humid_days', []))}")
    
    # Get available years
    print("\n=== Available years in dataset ===")
    years = api.get_available_years()
    print(years)
    
    # Get hourly patterns
    print("\n=== Hourly patterns for January 2010 ===")
    hourly_patterns = api.get_hourly_patterns(2010, 1)
    if hourly_patterns:
        print(f"Found hourly data for {len(hourly_patterns)} hours")
        print("Sample hour data:", json.dumps(hourly_patterns[0] if hourly_patterns else {}, indent=2))
    
    # Get comprehensive data summary
    print("\n=== Comprehensive Data Summary ===")
    summary = api.get_data_summary()
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
