import os
import logging
from logging.handlers import RotatingFileHandler
from telegram import Bot
from telegram.error import TelegramError

# Setup logging
log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
log_handler = RotatingFileHandler('test_hashtag_logs.log', maxBytes=1024*1024*5, backupCount=5)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

# Telegram Bot setup
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # Environment variable se token lo
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN must be set.")
    raise ValueError("TELEGRAM_BOT_TOKEN must be set.")
TARGET_GROUP = "-1002548213213"  # @TheWatchDraftChat ka group ID
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Hardcoded hashtags for testing
hashtags = [
    "#MovieNight",
    "#AnimeLovers",
    "#CinemaFans",
    "#SeriesBinge",
    "#PopCulture",
    "#TheWatchDraft"
]

def post_test_hashtags():
    """Post 5-6 hardcoded hashtags to the group."""
    try:
        message = " ".join(hashtags)
        if len(message) > 4096:  # Telegram message limit
            message = message[:4090] + "..."
        bot.send_message(chat_id=TARGET_GROUP, text=message)
        logger.info(f"Successfully posted {len(hashtags)} hashtags to {TARGET_GROUP}")
    except TelegramError as e:
        logger.error(f"Telegram error posting hashtags: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    logger.info("Starting test hashtag post...")
    post_test_hashtags()
    logger.info("Test complete.")
