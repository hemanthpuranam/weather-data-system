import sqlite3
from contextlib import contextmanager
import pandas as pd
from pathlib import Path
from LoggerConfig import logger

class WeatherDatabase:
    """Handles database operations for weather data."""
    
    def __init__(self, db_path: str = "weather_data.db"):
        self.db_path = db_path
        self._create_tables()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _create_tables(self):
        """Create the comprehensive weather data table for Delhi dataset."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    datetime_utc TIMESTAMP,
                    date TEXT,
                    year INTEGER,
                    month INTEGER,
                    day INTEGER,
                    hour INTEGER,
                    month_name TEXT,
                    
                    -- Core weather measurements
                    temperature REAL,
                    humidity REAL,
                    pressure REAL,
                    heat_index REAL,
                    dew_point REAL,
                    
                    -- Weather conditions
                    weather_condition TEXT,
                    
                    -- Precipitation and visibility
                    precipitation REAL,
                    visibility REAL,
                    
                    -- Wind measurements
                    wind_direction REAL,
                    wind_direction_text TEXT,
                    wind_speed REAL,
                    wind_gust REAL,
                    wind_chill REAL,
                    
                    -- Weather events (boolean flags)
                    fog_event INTEGER DEFAULT 0,
                    hail_event INTEGER DEFAULT 0,
                    rain_event INTEGER DEFAULT 0,
                    snow_event INTEGER DEFAULT 0,
                    thunder_event INTEGER DEFAULT 0,
                    tornado_event INTEGER DEFAULT 0,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for faster queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_datetime_utc ON weather_data(datetime_utc)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON weather_data(date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_year_month ON weather_data(year, month)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_year ON weather_data(year)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_weather_condition ON weather_data(weather_condition)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_temperature ON weather_data(temperature)")
            
            conn.commit()
            logger.info("Delhi weather database tables created/verified successfully")
    
    def store_data(self, df: pd.DataFrame):
        """Store processed Delhi weather data in the database."""
        try:
            with self.get_connection() as conn:
                # Prepare data for insertion based on the processed DataFrame structure
                records = []
                for _, row in df.iterrows():
                    record = {
                        'datetime_utc': row.get('datetime_utc').isoformat() if pd.notna(row.get('datetime_utc')) else None,
                        'date': str(row.get('date')) if pd.notna(row.get('date')) else None,
                        'year': row.get('year'),
                        'month': row.get('month'),
                        'day': row.get('day'),
                        'hour': row.get('hour'),
                        'month_name': row.get('month_name'),
                        
                        # Core measurements
                        'temperature': row.get('temperature'),
                        'humidity': row.get('humidity'),
                        'pressure': row.get('pressure'),
                        'heat_index': row.get('heat_index'),
                        'dew_point': row.get('dew_point'),
                        
                        # Conditions
                        'weather_condition': row.get('weather_condition'),
                        
                        # Precipitation and visibility
                        'precipitation': row.get('precipitation'),
                        'visibility': row.get('visibility'),
                        
                        # Wind
                        'wind_direction': row.get('wind_direction'),
                        'wind_direction_text': row.get('wind_direction_text'),
                        'wind_speed': row.get('wind_speed'),
                        'wind_gust': row.get('wind_gust'),
                        'wind_chill': row.get('wind_chill'),
                        
                        # Weather events
                        'fog_event': row.get('fog_event', 0),
                        'hail_event': row.get('hail_event', 0),
                        'rain_event': row.get('rain_event', 0),
                        'snow_event': row.get('snow_event', 0),
                        'thunder_event': row.get('thunder_event', 0),
                        'tornado_event': row.get('tornado_event', 0)
                    }
                    
                    records.append(record)
                
                # Insert data
                cursor = conn.cursor()
                cursor.executemany("""
                    INSERT INTO weather_data 
                    (datetime_utc, date, year, month, day, hour, month_name, 
                     temperature, humidity, pressure, heat_index, dew_point,
                     weather_condition, precipitation, visibility,
                     wind_direction, wind_direction_text, wind_speed, wind_gust, wind_chill,
                     fog_event, hail_event, rain_event, snow_event, thunder_event, tornado_event)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [(r['datetime_utc'], r['date'], r['year'], r['month'], r['day'], r['hour'], r['month_name'],
                       r['temperature'], r['humidity'], r['pressure'], r['heat_index'], r['dew_point'],
                       r['weather_condition'], r['precipitation'], r['visibility'],
                       r['wind_direction'], r['wind_direction_text'], r['wind_speed'], r['wind_gust'], r['wind_chill'],
                       r['fog_event'], r['hail_event'], r['rain_event'], r['snow_event'], r['thunder_event'], r['tornado_event']) 
                      for r in records])
                
                conn.commit()
                logger.info(f"Successfully stored {len(records)} Delhi weather records in database")
                
        except Exception as e:
            logger.error(f"Error storing data in database: {e}")
            raise
    
    def get_record_count(self) -> int:
        """Get total number of records in database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM weather_data")
            return cursor.fetchone()[0]
