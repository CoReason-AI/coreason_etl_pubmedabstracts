import os
import sys

from loguru import logger

# Remove default handler
logger.remove()

# Sink 1 (Stdout)
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Sink 2 (File)
logger.add(
    "logs/app.log",
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
    level="DEBUG",  # Assuming file should capture debug info as well
)

__all__ = ["logger"]
