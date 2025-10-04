# LoggerConfig.py
import logging

# Configure global logging format and level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Create a named logger
logger = logging.getLogger("weather_system")
