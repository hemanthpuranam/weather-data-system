import DataProcessingModule
from DataStorageModule import WeatherDatabase
import sqlite3
from typing import List, Dict, Optional
from LoggerConfig import logger


class WeatherAPI:
    """API class for weather data queries."""
    
    def __init__(self, db_path: str = "weather_data.db"):
        self.db = WeatherDatabase(db_path)
    
    def get_available_years(self) -> list:
        """Return a list of all unique years available in the dataset."""
        try:
            # Open a fresh connection each time
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT year 
                    FROM weather_data 
                    WHERE year IS NOT NULL 
                    ORDER BY year
                """)
                results = cursor.fetchall()
                return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Error fetching available years: {e}")
            return []


    def get_weather_by_date(self, target_date: str) -> List[Dict]:
        """Get comprehensive weather details for a specific date."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        datetime_utc, weather_condition, temperature, humidity, pressure, heat_index,
                        dew_point, precipitation, visibility, wind_speed, wind_direction,
                        wind_direction_text, wind_gust, wind_chill,
                        fog_event, hail_event, rain_event, snow_event, thunder_event, tornado_event
                    FROM weather_data 
                    WHERE date = ? OR DATE(datetime_utc) = ?
                    ORDER BY datetime_utc
                """, (target_date, target_date))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error fetching data by date: {e}")
            return []
    
    def get_weather_by_month(self, month: int, year: Optional[int] = None) -> List[Dict]:
        """Get comprehensive weather details for a specific month."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                if year:
                    cursor.execute("""
                        SELECT 
                            datetime_utc, weather_condition, temperature, humidity, pressure, heat_index,
                            dew_point, precipitation, visibility, wind_speed, wind_direction,
                            wind_direction_text, wind_gust, wind_chill,
                            fog_event, hail_event, rain_event, snow_event, thunder_event, tornado_event
                        FROM weather_data 
                        WHERE month = ? AND year = ?
                        ORDER BY datetime_utc
                    """, (month, year))
                else:
                    cursor.execute("""
                        SELECT 
                            datetime_utc, year, weather_condition, temperature, humidity, pressure, heat_index,
                            dew_point, precipitation, visibility, wind_speed, wind_direction,
                            wind_direction_text, wind_gust, wind_chill,
                            fog_event, hail_event, rain_event, snow_event, thunder_event, tornado_event
                        FROM weather_data 
                        WHERE month = ?
                        ORDER BY year, datetime_utc
                    """, (month,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error fetching data by month: {e}")
            return []
    
    def get_monthly_temperature_stats(self, year: int) -> Dict:
        """Get high, median, and minimum temperature for each month of a given year."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        month,
                        month_name,
                        MAX(temperature) as max_temp,
                        MIN(temperature) as min_temp,
                        AVG(temperature) as avg_temp,
                        COUNT(*) as record_count
                    FROM weather_data 
                    WHERE year = ? AND temperature IS NOT NULL
                    GROUP BY month, month_name
                    ORDER BY month
                """, (year,))
                
                results = cursor.fetchall()
                
                # Calculate median separately (SQLite doesn't have built-in median)
                monthly_stats = {}
                for row in results:
                    month = row['month']
                    month_name = row['month_name']
                    
                    # Get all temperatures for this month to calculate median
                    cursor.execute("""
                        SELECT temperature 
                        FROM weather_data 
                        WHERE year = ? AND month = ? AND temperature IS NOT NULL
                        ORDER BY temperature
                    """, (year, month))
                    
                    temps = [r[0] for r in cursor.fetchall()]
                    median_temp = None
                    if temps:
                        n = len(temps)
                        median_temp = temps[n//2] if n % 2 == 1 else (temps[n//2-1] + temps[n//2]) / 2
                    
                    monthly_stats[month] = {
                        'month': month,
                        'month_name': month_name,
                        'max_temperature': row['max_temp'],
                        'min_temperature': row['min_temp'],
                        'median_temperature': median_temp,
                        'avg_temperature': round(row['avg_temp'], 2) if row['avg_temp'] else None,
                        'record_count': row['record_count']
                    }
                
                return monthly_stats
                
        except Exception as e:
            logger.error(f"Error fetching temperature statistics: {e}")
            return {}
    
    def get_weather_events_summary(self, year: Optional[int] = None, month: Optional[int] = None) -> Dict:
        """Get summary of weather events (fog, hail, rain, snow, thunder, tornado)."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Build WHERE clause dynamically
                where_conditions = []
                params = []
                
                if year:
                    where_conditions.append("year = ?")
                    params.append(year)
                    
                if month:
                    where_conditions.append("month = ?")
                    params.append(month)
                
                where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                
                query = f"""
                    SELECT 
                        SUM(fog_event) as fog_days,
                        SUM(hail_event) as hail_days,
                        SUM(rain_event) as rain_days,
                        SUM(snow_event) as snow_days,
                        SUM(thunder_event) as thunder_days,
                        SUM(tornado_event) as tornado_days,
                        COUNT(*) as total_records
                    FROM weather_data 
                    {where_clause}
                """
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                return dict(result) if result else {}
                
        except Exception as e:
            logger.error(f"Error fetching weather events summary: {e}")
            return {}
    
    def get_extreme_weather_days(self, year: int, temperature_threshold: float = 40.0) -> Dict:
        """Get days with extreme weather conditions for a given year."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Hot days
                cursor.execute("""
                    SELECT DATE(datetime_utc) as date, MAX(temperature) as max_temp
                    FROM weather_data 
                    WHERE year = ? AND temperature >= ?
                    GROUP BY DATE(datetime_utc)
                    ORDER BY max_temp DESC
                """, (year, temperature_threshold))
                hot_days = cursor.fetchall()
                
                # Cold days (below 5°C)
                cursor.execute("""
                    SELECT DATE(datetime_utc) as date, MIN(temperature) as min_temp
                    FROM weather_data 
                    WHERE year = ? AND temperature <= 5.0
                    GROUP BY DATE(datetime_utc)
                    ORDER BY min_temp ASC
                """, (year,))
                cold_days = cursor.fetchall()
                
                # High humidity days (above 90%)
                cursor.execute("""
                    SELECT DATE(datetime_utc) as date, MAX(humidity) as max_humidity
                    FROM weather_data 
                    WHERE year = ? AND humidity >= 90.0
                    GROUP BY DATE(datetime_utc)
                    ORDER BY max_humidity DESC
                """, (year,))
                humid_days = cursor.fetchall()
                
                return {
                    'hot_days': [dict(row) for row in hot_days],
                    'cold_days': [dict(row) for row in cold_days],
                    'humid_days': [dict(row) for row in humid_days]
                }
                
        except Exception as e:
            logger.error(f"Error fetching extreme weather days: {e}")
            return {}
    
    def get_hourly_patterns(self, year: int, month: int) -> list:
        """
    Get average temperature, humidity, and pressure for each hour
    in a given month and year.
    """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        hour,
                        AVG(temperature) AS avg_temp,
                        AVG(humidity) AS avg_humidity,
                        AVG(pressure) AS avg_pressure,
                        COUNT(*) AS record_count
                    FROM weather_data
                    WHERE year = ? AND month = ?
                    GROUP BY hour
                    ORDER BY hour
                """, (year, month))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error fetching hourly patterns: {e}")
            return []


    def get_data_summary(self) -> Dict:
        """Get summary of available data."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Basic stats
                cursor.execute("SELECT COUNT(*) FROM weather_data")
                total_records = cursor.fetchone()[0]
                
                cursor.execute("SELECT MIN(year), MAX(year) FROM weather_data WHERE year IS NOT NULL")
                year_range = cursor.fetchone()
                
                cursor.execute("""
                    SELECT 
                        COUNT(CASE WHEN temperature IS NOT NULL THEN 1 END) as temp_records,
                        COUNT(CASE WHEN humidity IS NOT NULL THEN 1 END) as humidity_records,
                        COUNT(CASE WHEN pressure IS NOT NULL THEN 1 END) as pressure_records
                    FROM weather_data
                """)
                data_availability = cursor.fetchone()
                
                return {
                    'total_records': total_records,
                    'year_range': {
                        'start': year_range[0],
                        'end': year_range[1]
                    },
                    'data_availability': {
                        'temperature_records': data_availability[0],
                        'humidity_records': data_availability[1],
                        'pressure_records': data_availability[2]
                    }
                }
        except Exception as e:
            logger.error(f"Error fetching data summary: {e}")
            return {}

