"""
Configuration management for JESC Discord Bot
Loads environment variables and provides configuration constants
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration"""
    
    DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
    COMMAND_PREFIX = os.getenv('COMMAND_PREFIX', '/')
    
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'jesc_bot')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    # database URL for SQLAlchemy
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # bot Configuration
    MAX_RESULTS = int(os.getenv('MAX_RESULTS', 5))
    
    # data paths
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    JESC_JA_PATH = os.path.join(DATA_DIR, 'train.ja')
    JESC_EN_PATH = os.path.join(DATA_DIR, 'train.en')
    
    def validate(self):
        """Validate that required configuration is set"""
        if not self.DISCORD_BOT_TOKEN:
            raise ValueError("DISCORD_BOT_TOKEN is not set in .env file")
        if not self.DB_PASSWORD:
            raise ValueError("DB_PASSWORD is not set in .env file")
        return True


# create a singleton instance
config = Config()