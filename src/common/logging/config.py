import sys
from pathlib import Path
from typing import Optional

from loguru import logger


class LoggerConfig:
    """Singleton class to manage logger configuration."""

    _instance: Optional["LoggerConfig"] = None
    _initialised: bool = False

    def __new__(cls) -> "LoggerConfig":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # Create logs directory if it doesn't exist
        Path("logs").mkdir(exist_ok=True)

    def configure(self, app_name: str = "app") -> None:
        """Configure logger with settings for specific app component."""
        if not self._initialised:
            # Remove default handler
            logger.remove()

            # Add file handler
            logger.add(
                f"logs/{app_name}_{{time}}.log",
                rotation="1 day",
                retention="1 week",
                compression="zip",
                level="INFO",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
                backtrace=True,
                diagnose=True,
            )

            # Add console handler with colors
            logger.add(
                sys.stderr,
                colorize=True,
                format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
                level="INFO",
                backtrace=True,
                diagnose=True,
            )

            self._initialised = True


def setup_logger(app_name: str) -> None:
    """Initialise logger configuration if not already initialised."""
    config = LoggerConfig()
    config.configure(app_name)
