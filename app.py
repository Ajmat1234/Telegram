import os
import time
import requests
import json
import sqlite3
from flask import Flask, jsonify, Response
from flask_cors import CORS
from datetime import datetime
import threading
from supabase import create_client, Client
import logging
from logging.handlers import RotatingFileHandler
import schedule
import re
import random
import telebot
import tweepy
import praw
import pytumblr
from mastodon import Mastodon

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Setup logging
log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
log_handler = RotatingFileHandler('app_logs.log', maxBytes=1024 * 1024 * 5, backupCount=5)
log_handler.setFormatter(log_formatter)
log_handler.setLevel(logging.DEBUG)
app.logger.setLevel(logging.DEBUG)
app.logger.addHandler(log_handler)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
app.logger.addHandler(stream_handler)

# Supabase setup
SUPABASE_URL = "https://wxsdvjohphpwdxbeioki.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind4c2R2am9ocGhwd2R4YmVpb2tpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDgzNDIxODksImV4cCI6MjA2MzkxODE4OX0.89p7DcWynu-96wfOagF_DE5Hbsff_cVc34JHMcd95J0"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Gemini API setup
GEMINI_API_KEY = "AIzaSyALVGk-yBmkohV6Wqei63NARTd9xD-O7TI"
GENINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
HEADERS = {"Content-Type": "application/json"}

# Telegram Bot setup
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Twitter (X) setup
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
twitter_client = tweepy.Client(
    bearer_token=TWITTER_BEARER_TOKEN,
    consumer_key=TWITTER_API_KEY,
    consumer_secret=TWITTER_API_SECRET,
    access_token=TWITTER_ACCESS_TOKEN,
    access_token_secret=TWITTER_ACCESS_TOKEN_SECRET
)

# Reddit setup
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    username=REDDIT_USERNAME,
    password=REDDIT_PASSWORD,
    user_agent=REDDIT_USER_AGENT
)

# Tumblr setup
TUMBLR_CONSUMER_KEY = os.getenv("TUMBLR_CONSUMER_KEY")
TUMBLR_CONSUMER_SECRET = os.getenv("TUMBLR_CONSUMER_SECRET")
TUMBLR_ACCESS_TOKEN = os.getenv("TUMBLR_ACCESS_TOKEN")
TUMBLR_ACCESS_TOKEN_SECRET = os.getenv("TUMBLR_ACCESS_TOKEN_SECRET")
tumblr = pytumblr.TumblrRestClient(
    TUMBLR_CONSUMER_KEY,
    TUMBLR_CONSUMER_SECRET,
    TUMBLR_ACCESS_TOKEN,
    TUMBLR_ACCESS_TOKEN_SECRET
)

# Mastodon setup
MASTODON_ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN")
MASTODON_API_BASE_URL = os.getenv("MASTODON_API_BASE_URL")
mastodon = Mastodon(
    access_token=MASTODON_ACCESS_TOKEN,
    api_base_url=MASTODON_API_BASE_URL
)

# SQLite setup
DB_PATH = os.path.join(os.getcwd(), "blogs.db")
db_lock = threading.Lock()

app.logger.info(f"Current working directory: {os.getcwd()}")
app.logger.info(f"Database path: {DB_PATH}")

def initialize_database():
    """Initialize SQLite database with retries and checks."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with db_lock:
                os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.', exist_ok=True)
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS blogs (
                        id INTEGER PRIMARY KEY,
                        title TEXT,
                        content TEXT,
                        category TEXT,
                        question TEXT
                    )
                """)
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blogs'")
                if cursor.fetchone():
                    app.logger.info("Table 'blogs' exists")
                else:
                    app.logger.error("Table 'blogs' does not exist after creation")
                    continue
                conn.commit()
                conn.close()
            if os.path.exists(DB_PATH):
                app.logger.info(f"Database file exists at {DB_PATH}")
            else:
                app.logger.error(f"Database file does not exist at {DB_PATH}")
                continue
            return True
        except sqlite3.Error as e:
            app.logger.error(f"Attempt {attempt + 1}/{max_retries} - Error initializing database: {e}")
            time.sleep(1)
    app.logger.error("Failed to initialize database after retries.")
    return False

def fetch_blogs_from_supabase():
    """Fetch all blogs from Supabase with pagination."""
    blogs = []
    page_size = 1000
    offset = 0
    while True:
        try:
            response = supabase.table('tables').select('id, title, content, category, question').range(offset, offset + page_size - 1).execute()
            fetched_blogs = response.data or []
            blogs.extend(fetched_blogs)
            if len(fetched_blogs) < page_size:
                break
            offset += page_size
        except Exception as e:
            app.logger.error(f"Error fetching from Supabase: {e}")
            break
    return blogs

def populate_database():
    """Populate SQLite database with Supabase data."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with db_lock:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                blogs = fetch_blogs_from_supabase()
                for blog in blogs:
                    cursor.execute("""
                        INSERT OR IGNORE INTO blogs (id, title, content, category, question)
                        VALUES (?, ?, ?, ?, ?)
                    """, (blog['id'], blog['title'], blog['content'], blog['category'], blog.get('question', '')))
                conn.commit()
                conn.close()
            app.logger.info(f"Populated SQLite with {len(blogs)} blogs.")
            return True
        except sqlite3.Error as e:
            app.logger.error(f"Attempt {attempt + 1}/{max_retries} - Error populating database: {e}")
            time.sleep(1)
    return False

def insert_blog_to_db(blog):
    """Insert a single blog into SQLite."""
    try:
        with db_lock:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blogs'")
            if not cursor.fetchone():
                app.logger.error("Table 'blogs' does not exist, cannot insert blog")
                return
            cursor.execute("""
                INSERT INTO blogs (id, title, content, category, question)
                VALUES (?, ?, ?, ?, ?)
            """, (blog['id'], blog['title'], blog['content'], blog['category'], blog['question']))
            conn.commit()
            conn.close()
    except sqlite3.Error as e:
        app.logger.error(f"Error inserting blog to SQLite: {e}")

def generate_unique_topic(existing_titles):
    """Generate unique topics dynamically."""
    movies = [
        "Interstellar: What If We Never Left Earth?", "Blade Runner 2049: Are Replicants Human?",
        "Mad Max: Fury Road - Could Immortan Joe Win?", "The Incredibles: Superhero Family Dynamics",
        "Back to the Future: Time Travel Paradoxes Explained"
    ]
    anime = [
        "Haikyuu: Can Determination Beat Talent?", "Spy x Family: Anya’s Secret Powers Analyzed",
        "Vinland Saga: Thorfinn’s Redemption Arc Breakdown", "Dr. Stone: Science vs. Strength",
        "Steins;Gate: Time Travel’s Dark Consequences"
    ]
    adventures = [
        "Jumanji: What If the Game Never Ended?", "The Mummy: Rick O’Connell’s Bravest Moments",
        "Treasure Planet: Jim Hawkins’ Galactic Quest", "King Kong: Could He Survive Today?",
        "Journey to the Center of the Earth: Is It Possible?"
    ]
    categories = {"Movie": movies, "Anime": anime, "Adventure": adventures}
    while True:
        category = random.choice(list(categories.keys()))
        topic = random.choice(categories[category])
        if topic not in existing_titles:
            return {"topic": topic, "category": category}

def clean_content(content):
    """Remove markdown symbols from content."""
    if not content:
        return content
    content = re.sub(r'\*\*([^*]+)\*\*', r'\1', content)
    content = re.sub(r'\*([^*]+)\*', r'\1', content)
    content = re.sub(r'#+\s*', '', content)
    content = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', content)
    content = re.sub(r'!\[([^\]]*)\]\([^\)]+\)', r'\1', content)
    content = re.sub(r'```[\s\S]*?```', '', content)
    content = re.sub(r'`([^`]+)`', r'\1', content)
    content = re.sub(r'^\s*[-*+]\s+', '', content, flags=re.MULTILINE)
    content = re.sub(r'\n\s*\n+', '\n\n', content)
    return content.strip()[:1500]

def humanize_content(content):
    """Humanize content using Gemini API."""
    prompt = f"""
    Turn the content below into a fun, engaging blog post for a 2025 audience.
    - Write in English only, with a conversational, female writer vibe.
    - Keep it short (500-800 words, max 1500 characters).
    - Start with a humorous or relatable hook.
    - Add humor and playful tone.
    - Reference credible sources (e.g., "A 2025 Pop Culture Institute study says...").
    - End with an engaging question to spark discussion.
    - Use simple, SEO-friendly language.
    - Output plain text, no markdown symbols.

    Original Content:
    {content}
    """
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.9, "topK": 40, "topP": 0.95, "maxOutputTokens": 1024}
    }
    try:
        response = requests.post(GENINI_URL, headers=HEADERS, json=payload, timeout=30)
        return clean_content(response.json()["candidates"][0]["content"]["parts"][0]["text"].strip())
    except Exception as e:
        app.logger.error(f"Failed to humanize content: {e}")
        return clean_content(content)

def generate_question(content):
    """Generate a short question based on content, max 150 characters."""
    prompt = f"""
    Based on the following blog post, generate a thought-provoking question that encourages discussion. 
    The question should be concise, no more than 150 characters.

    Blog Post:
    {content}

    Example:
    "Can Spider-Man’s webs outsmart zombies in an apocalypse?"
    """
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "topK": 40, "topP": 0.9, "maxOutputTokens": 50}
    }
    try:
        response = requests.post(GENINI_URL, headers=HEADERS, json=data, timeout=30)
        question = response.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        return question if question else None
    except Exception as e:
        app.logger.error(f"Error generating question: {e}")
        return None

def get_existing_titles():
    """Get existing titles from SQLite."""
    with db_lock:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blogs'")
            if not cursor.fetchone():
                app.logger.error("Table 'blogs' does not exist")
                return set()
            cursor.execute("SELECT title FROM blogs")
            titles = {row[0] for row in cursor.fetchall()}
            conn.close()
            return titles
        except sqlite3.Error as e:
            app.logger.error(f"Error fetching titles: {e}")
            return set()

def generate_hashtags(topic, category):
    """Generate 10 relevant hashtags."""
    base_tags = {
        "Anime": ["#AnimeVibes", "#AnimeFans", "#AnimeWorld", "#AnimeLife", "#AnimeLovers"],
        "Movie": ["#MovieNight", "#FilmFans", "#CinemaLovers", "#MovieMagic", "#Blockbuster"],
        "Adventure": ["#AdventureTime", "#EpicJourney", "#ThrillSeekers", "#AdventureAwaits", "#ExploreMore"]
    }
    topic_words = topic.lower().replace(":", "").replace("?", "").split()
    topic_tags = [f"#{word.capitalize()}" for word in topic_words[:3]]
    category_tags = base_tags.get(category, [])
    all_tags = list(set(category_tags + topic_tags + ["#TheWatchDraft", "#PopCulture2025", "#FanTheories"]))
    return all_tags[:10]

def generate_post_with_gemini(topic, category):
    """Generate blog post using Gemini API."""
    prompt = f"""
    You're a witty content creator for a 2025 audience.
    Create a blog post on: "{topic}".
    - Write a catchy, SEO-friendly title (10-12 words).
    - Keep it short (500-800 words, max 1500 characters).
    - Start with a funny or relatable hook.
    - Use a playful, female writer tone with humor.
    - Include a fan theory or hypothetical scenario.
    - Mention credible sources (e.g., "A 2025 Pop Culture Institute study says...").
    - End with an engaging question.
    - Write in English only, no markdown symbols.

    Example:
    Title: Could Spider-Man Survive a Zombie Apocalypse?
    Okay, picture this: Spider-Man swinging through a zombie-infested New York, web-slinging brains instead of bad guys. ...
    """
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.8, "topK": 40, "topP": 0.9, "maxOutputTokens": 1024}
    }
    try:
        response = requests.post(GENINI_URL, headers=HEADERS, json=data, timeout=30)
        content = response.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        return content if content else None
    except Exception as e:
        app.logger.error(f"Gemini error: {e}")
        return None

def post_to_all_platforms(post, topic):
    """Post content to all platforms."""
    hashtags = " ".join(generate_hashtags(topic, post['category']))
    full_content = f"{post['title']}\n\n{post['content']}\n\n{hashtags}"
    question = post['question']
    telegram_link = "Join us on Telegram: https://t.me/TheWatchDraft"

    # Telegram
    try:
        bot.send_message(TELEGRAM_CHANNEL, full_content[:4096])
        app.logger.info(f"Posted to Telegram: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Telegram: {e}")

    # Twitter (X)
    try:
        tweet_text = f"{question} {telegram_link}"[:280]
        twitter_client.create_tweet(text=tweet_text)
        app.logger.info(f"Posted to Twitter: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Twitter: {e} - Skipping")

    # Reddit
    try:
        subreddit = reddit.subreddit('u_' + REDDIT_USERNAME)
        subreddit.submit(title=post['title'], selftext=full_content)
        app.logger.info(f"Posted to Reddit profile: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Reddit: {e}")

    # Tumblr
    try:
        tumblr.create_text("the-watch-draft", state="published", title=post['title'], body=full_content, tags=generate_hashtags(topic, post['category']))
        app.logger.info(f"Posted to Tumblr: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Tumblr: {e}")

    # Mastodon
    try:
        mastodon_text = f"{question} {telegram_link}"[:500]
        mastodon.status_post(mastodon_text)
        app.logger.info(f"Posted to Mastodon: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Mastodon: {e} - Skipping")

def generate_unique_post():
    """Generate unique blog post."""
    try:
        existing_titles = get_existing_titles()
        topic_data = generate_unique_topic(existing_titles)
        topic = topic_data["topic"]
        category = topic_data["category"]
        content = generate_post_with_gemini(topic, category)
        if not content:
            app.logger.warning(f"Failed to generate content for: {topic}")
            return None
        cleaned_content = clean_content(humanize_content(content))
        if not cleaned_content:
            return None
        question = generate_question(cleaned_content)
        if not question:
            app.logger.warning(f"Failed to generate question for: {topic}")
            return None
        title = next((line.replace("Title: ", "").strip() for line in cleaned_content.split('\n') if line.startswith("Title: ")), topic[:50])
        i = 1
        original_title = title
        while title in existing_titles:
            title = f"{original_title} ({i})"
            i += 1
        new_post = {
            "title": title,
            "content": cleaned_content,
            "category": category,
            "question": question
        }
        response = supabase.table('tables').insert(new_post).execute()
        inserted_post = response.data[0]
        insert_blog_to_db(inserted_post)
        post_to_all_platforms(inserted_post, topic)
        app.logger.info(f"Generated post: {title}")
        return new_post
    except Exception as e:
        app.logger.error(f"Error in generate_unique_post: {e}")
        return None

def auto_generate_and_upload():
    """Auto-generate and upload post."""
    post = generate_unique_post()
    if post:
        app.logger.info(f"Generated post: {post['title']}")

def keep_alive():
    """Keep server alive."""
    while True:
        try:
            requests.get("https://telegram-yvmd.onrender.com/ping", timeout=10)
            app.logger.info("Keep-alive ping successful")
        except Exception as e:
            app.logger.error(f"Keep-alive error: {e}")
        time.sleep(300)

@app.route('/ping')
def ping():
    return jsonify({"status": "alive"}), 200

@app.route('/generate', methods=['GET', 'POST'])
def manual_generate():
    try:
        post = generate_unique_post()
        return jsonify({"message": "Post generated" if post else "Failed", "post": post}), 200 if post else 500
    except Exception as e:
        app.logger.error(f"Error in /generate: {e}")
        return jsonify({"message": "Error", "error": str(e)}), 500

@app.route('/logs')
def view_logs():
    try:
        with open('app_logs.log', 'r') as f:
            return f"<pre>{f.read()}</pre>"
    except Exception as e:
        return jsonify({"message": "Error reading logs", "error": str(e)}), 500

def run_scheduler():
    """Run scheduler for periodic tasks."""
    schedule.every(1).hours.do(auto_generate_and_upload)
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    app.logger.info("Starting application...")
    if not initialize_database():
        app.logger.error("Database initialization failed")
        raise RuntimeError("Database initialization failed")
    if not populate_database():
        app.logger.error("Failed to populate database from Supabase")
        raise RuntimeError("Failed to populate database")
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
