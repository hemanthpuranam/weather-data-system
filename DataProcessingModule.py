import pandas as pd
import logging
from pathlib import Path
from LoggerConfig import logger


class WeatherDataProcessor:
    def __init__(self):
        self.processed_data = None
    
    def load_data_file(self, file_path: str, sheet_name: str = None) -> pd.DataFrame:
        """Load data from Excel file"""
        try:
            file_path = Path(file_path)
            file_extension = file_path.suffix.lower()
            
            if file_extension in ['.xlsx', '.xls']:
                if sheet_name:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                    logger.info(f"Loaded sheet '{sheet_name}' with {len(df)} rows")
                else:
                    df = pd.read_excel(file_path)
                    logger.info(f"Loaded Excel file with {len(df)} rows")
            elif file_extension == '.csv':
                df = pd.read_csv(file_path)
                logger.info(f"Loaded CSV file with {len(df)} rows")
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
            
            # Log column names for debugging
            logger.info(f"Columns found: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def clean_and_transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform weather data with specific column structure"""
        try:
            processed_df = df.copy()
            
            # Log original columns for debugging
            logger.info(f"Processing data with columns: {list(processed_df.columns)}")

            # Find the datetime column (case-insensitive)
            datetime_col = None
            possible_datetime_cols = ['datetimeutc', 'datetime_utc', 'DateTimeUTC', 'DateTime', 'datetime']
            
            for col in processed_df.columns:
                if col.lower().replace('_', '').replace(' ', '') in [c.lower().replace('_', '') for c in possible_datetime_cols]:
                    datetime_col = col
                    logger.info(f"Found datetime column: {datetime_col}")
                    break
            
            if datetime_col is None:
                logger.error(f"No datetime column found. Available columns: {list(processed_df.columns)}")
                raise ValueError("Cannot find datetime column in the dataset")

            # Process datetime column
            processed_df['datetime_utc'] = pd.to_datetime(processed_df[datetime_col], errors='coerce')
            
            # Check if datetime conversion was successful
            valid_dates = processed_df['datetime_utc'].notna().sum()
            logger.info(f"Successfully converted {valid_dates} out of {len(processed_df)} dates")
            
            if valid_dates == 0:
                raise ValueError("No valid dates found after conversion")
            
            # Extract time components
            processed_df['year'] = processed_df['datetime_utc'].dt.year
            processed_df['month'] = processed_df['datetime_utc'].dt.month
            processed_df['day'] = processed_df['datetime_utc'].dt.day
            processed_df['hour'] = processed_df['datetime_utc'].dt.hour
            processed_df['date'] = processed_df['datetime_utc'].dt.date
            processed_df['month_name'] = processed_df['datetime_utc'].dt.month_name()

            # Map numeric columns (case-insensitive matching)
            numeric_columns = {
                'tempm': 'temperature',
                'hum': 'humidity',
                'pressurem': 'pressure',
                'heatindexm': 'heat_index',
                'dewptm': 'dew_point',
                'precipm': 'precipitation',
                'vism': 'visibility',
                'wdird': 'wind_direction',
                'wspdm': 'wind_speed',
                'wgustm': 'wind_gust',
                'windchillm': 'wind_chill'
            }
            
            # Create a mapping of lowercase column names to actual column names
            col_mapping = {col.lower().replace('_', '').replace(' ', ''): col 
                          for col in processed_df.columns}
            
            for original_col, new_col in numeric_columns.items():
                # Try to find the column (case-insensitive)
                search_key = original_col.lower().replace('_', '').replace(' ', '')
                if search_key in col_mapping:
                    actual_col = col_mapping[search_key]
                    processed_df[new_col] = pd.to_numeric(processed_df[actual_col], errors='coerce')
                    logger.info(f"Mapped '{actual_col}' to '{new_col}'")
                else:
                    logger.warning(f"Column '{original_col}' not found in dataset")

            # Handle categorical weather conditions and events
            categorical_columns = {
                'conds': 'weather_condition',
                'wdire': 'wind_direction_text'
            }

            for original_col, new_col in categorical_columns.items():
                search_key = original_col.lower().replace('_', '').replace(' ', '')
                if search_key in col_mapping:
                    actual_col = col_mapping[search_key]
                    processed_df[new_col] = processed_df[actual_col].astype(str).str.strip()
                    processed_df[new_col] = processed_df[new_col].replace(['nan', '', 'None'], None)
                    logger.info(f"Mapped '{actual_col}' to '{new_col}'")
                else:
                    logger.warning(f"Column '{original_col}' not found in dataset")
            
            # Convert boolean event columns
            boolean_columns = ['fog', 'hail', 'rain', 'snow', 'thunder', 'tornado']
            for col in boolean_columns:
                search_key = col.lower()
                if search_key in col_mapping:
                    actual_col = col_mapping[search_key]
                    processed_df[f'{col}_event'] = processed_df[actual_col].fillna(0).astype(int)
                    logger.info(f"Mapped '{actual_col}' to '{col}_event'")
                else:
                    # Create empty event column if not found
                    processed_df[f'{col}_event'] = 0
                    logger.warning(f"Column '{col}' not found, creating empty '{col}_event' column")
            
            # Remove rows with invalid dates
            initial_count = len(processed_df)
            processed_df = processed_df.dropna(subset=['datetime_utc'])
            removed_count = initial_count - len(processed_df)
            if removed_count > 0:
                logger.info(f"Removed {removed_count} rows with invalid dates")
            
            # Handle missing values for numeric columns
            critical_columns = ['temperature', 'humidity', 'pressure']
            for col in critical_columns:
                if col in processed_df.columns:
                    missing_count = processed_df[col].isna().sum()
                    if missing_count > 0:
                        processed_df[col] = processed_df[col].ffill().fillna(processed_df[col].median())
                        logger.info(f"Filled {missing_count} missing values in '{col}'")
            
            # Other numeric columns - use median
            other_numeric = ['heat_index', 'dew_point', 'precipitation', 'visibility', 
                           'wind_direction', 'wind_speed', 'wind_gust', 'wind_chill']
            for col in other_numeric:
                if col in processed_df.columns:
                    missing_count = processed_df[col].isna().sum()
                    if missing_count > 0:
                        processed_df[col] = processed_df[col].fillna(processed_df[col].median())
                        logger.info(f"Filled {missing_count} missing values in '{col}'")
            
            # Sort by datetime
            processed_df = processed_df.sort_values('datetime_utc')
            
            self.processed_data = processed_df
            logger.info(f"Weather data processing completed. Shape: {processed_df.shape}")
            
            # Safe date range logging
            if 'year' in processed_df.columns and processed_df['year'].notna().any():
                min_year = processed_df['year'].min()
                max_year = processed_df['year'].max()
                logger.info(f"Date range: {min_year} to {max_year}")
            else:
                logger.warning("Year column not available for date range logging")
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Error in data transformation: {e}")
            raise
    
    def get_data_summary(self) -> dict:
        """Get summary statistics of the processed data."""
        if self.processed_data is None:
            return {"error": "No processed data available"}
        
        df = self.processed_data
        
        summary = {
            "total_records": len(df),
            "date_range": {
                "start": str(df['year'].min()) if 'year' in df.columns and df['year'].notna().any() else None,
                "end": str(df['year'].max()) if 'year' in df.columns and df['year'].notna().any() else None
            },
            "columns": list(df.columns),
            "missing_values": df.isnull().sum().to_dict()
        }
        
        return summary