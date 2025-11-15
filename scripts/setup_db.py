"""
database initiliation script
creates the necessary tables for the JESC Discord Bot
"""
import sys
import logging
from pathlib import Path

# parent dir
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import db
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """initialize the database"""
    try:
        logger.info("Validating configuration...")
        config.validate()

        # connect to database
        logger.info(f"Connecting to database at {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}...")
        db.connect()

        # create tables
        logger.info("Creating database tables...")
        db.create_tables()

        logger.info("Database setup complete!")
        logger.info("Next steps:")
        logger.info("  1. Download JESC corpus from Kaggle")
        logger.info("  2. Place train.ja and train.en in the data/ directory")
        logger.info("  3. Run: python -m src.loader")
    
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please check your .env file")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()