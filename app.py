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
if not SUPABASE_URL or not SUPABASE_KEY:
    app.logger.error("SUPABASE_URL and SUPABASE_KEY must be set.")
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set.")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Gemini API setup
GEMINI_API_KEY = "AIzaSyALVGk-yBmkohV6Wqei63NARTd9xD-O7TI"
if not GEMINI_API_KEY:
    app.logger.error("GEMINI_API_KEY must be set.")
    raise ValueError("GEMINI_API_KEY must be set.")
GENINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
HEADERS = {"Content-Type": "application/json"}

# Telegram Bot setup
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# SQLite setup
DB_PATH = "blogs.db"

def initialize_database():
    """Initialize SQLite database with retries."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.', exist_ok=True)
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS blogs (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    content TEXT,
                    category TEXT
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_title ON blogs(title)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_content ON blogs(content)")
            conn.commit()
            conn.close()
            app.logger.info(f"Initialized SQLite database at {DB_PATH}")
            return True
        except sqlite3.Error as e:
            app.logger.error(f"Attempt {attempt + 1}/{max_retries} - Error initializing database: {e}")
            time.sleep(1)
    app.logger.error("Failed to initialize database after retries.")
    return False

def check_database():
    """Check if SQLite database is valid."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blogs'")
        if not cursor.fetchone():
            app.logger.error("Table 'blogs' does not exist.")
            return False
        cursor.execute("SELECT 1 FROM blogs LIMIT 1")
        conn.close()
        app.logger.info("Database check passed.")
        return True
    except sqlite3.Error as e:
        app.logger.error(f"Database check failed: {e}")
        return False

def fetch_blogs_from_supabase():
    """Fetch all blogs from Supabase with pagination."""
    blogs = []
    page_size = 1000
    offset = 0
    while True:
        try:
            response = supabase.table('tables').select('id, title, content, category').range(offset, offset + page_size - 1).execute()
            fetched_blogs = response.data or []
            blogs.extend(fetched_blogs)
            app.logger.info(f"Fetched {len(fetched_blogs)} blogs from Supabase, total: {len(blogs)}")
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
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            blogs = fetch_blogs_from_supabase()
            for blog in blogs:
                cursor.execute("""
                    INSERT OR IGNORE INTO blogs (id, title, content, category)
                    VALUES (?, ?, ?, ?)
                """, (blog['id'], blog['title'], blog['content'], blog['category']))
            conn.commit()
            conn.close()
            app.logger.info(f"Populated SQLite with {len(blogs)} blogs.")
            return True
        except sqlite3.Error as e:
            app.logger.error(f"Attempt {attempt + 1}/{max_retries} - Error populating database: {e}")
            time.sleep(1)
    app.logger.error("Failed to populate database after retries.")
    return False

def insert_blog_to_db(blog):
    """Insert a single blog into SQLite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO blogs (id, title, content, category)
            VALUES (?, ?, ?, ?)
        """, (blog['id'], blog['title'], blog['content'], blog['category']))
        conn.commit()
        conn.close()
        app.logger.info(f"Inserted blog into SQLite: {blog['title']}")
    except sqlite3.Error as e:
        app.logger.error(f"Error inserting blog to SQLite: {e}")

# New unique topics generator
def generate_unique_topic():
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
    category = random.choice(list(categories.keys()))
    topic = random.choice(categories[category])
    return {"topic": topic, "category": category}

USED_TOPICS = set()
USED_CONTENTS = set()
LAST_CATEGORY = None

def clean_content(content):
    """Remove markdown symbols from content to ensure plain text."""
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
    return content.strip()[:1500]  # Reduced length for shorter posts

def humanize_content(content):
    """Humanize content using Gemini API with a fun, humorous tone."""
    prompt = f"""
    Turn the content below into a fun, engaging blog post for a 2025 audience.
    - Write in English only, with a conversational, female writer vibe.
    - Keep it short (500-800 words, max 1500 characters).
    - Start with a humorous or relatable hook (like a funny scenario or witty question).
    - Add humor and playful tone (e.g., sarcastic remarks, pop culture references).
    - Reference credible sources (e.g., "A 2025 Pop Culture Institute study says...").
    - End with an engaging question to spark discussion.
    - Use simple, SEO-friendly language with a logical flow.
    - Output plain text, no markdown symbols.

    Original Content:
    {content}
    """
    max_retries = 2
    for attempt in range(max_retries):
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.9, "topK": 40, "topP": 0.95, "maxOutputTokens": 1024}
            }
            response = requests.post(GENINI_URL, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            humanized_content = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            cleaned_content = clean_content(humanized_content)
            app.logger.info("Content humanized and cleaned successfully with Gemini.")
            return cleaned_content
        except Exception as e:
            app.logger.error(f"Attempt {attempt + 1}/{max_retries} - Failed to humanize content with Gemini: {e}")
            time.sleep(1)
    app.logger.error("Failed to humanize content with Gemini.")
    return clean_content(content)

def get_existing_data():
    """Get existing titles and contents from SQLite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT title, content FROM blogs")
        rows = cursor.fetchall()
        conn.close()
        return {
            'titles': {row[0] for row in rows},
            'contents': {row[1] for row in rows}
        }
    except sqlite3.Error as e:
        app.logger.error(f"Error fetching existing data: {e}")
        if "no such table" in str(e).lower():
            app.logger.info("Attempting to initialize database due to missing table.")
            if initialize_database():
                app.logger.info("Database initialized successfully.")
                return {'titles': set(), 'contents': set()}
        return {'titles': set(), 'contents': set()}

def generate_hashtags(topic, category):
    """Generate 10 relevant hashtags for the topic."""
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
    """Generate blog post using Gemini API with humor and theories."""
    app.logger.debug(f"Generating post for topic: {topic} with Gemini")
    prompt = f"""
    You're a witty content creator writing for a 2025 global audience.
    Create a blog post on: "{topic}".
    - Write a catchy, SEO-friendly title (10-12 words, fun and engaging).
    - Keep it short (500-800 words, max 1500 characters).
    - Start with a funny or relatable hook (e.g., a quirky scenario or sarcastic question).
    - Use a playful, female writer tone with humor (sarcastic remarks, pop culture references).
    - Include a fan theory or hypothetical scenario to make it unique.
    - Mention credible sources (e.g., "A 2025 Pop Culture Institute study says...").
    - End with an engaging question for the audience.
    - Write in English only, no markdown symbols, simple and SEO-friendly.

    Example:
    Title: Could Spider-Man Survive a Zombie Apocalypse?
    Okay, picture this: Spider-Man swinging through a zombie-infested New York, web-slinging brains instead of bad guys. Sounds like a wild ride, right? ... [Continue with humor, theories, and a question]
    """
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.8, "topK": 40, "topP": 0.9, "maxOutputTokens": 1024}
    }
    try:
        response = requests.post(GENINI_URL, headers=HEADERS, json=data, timeout=30)
        if response.status_code == 200:
            content = response.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            if content:
                app.logger.info(f"Generated content for topic: {topic} with Gemini")
                return content
            app.logger.warning(f"Empty content for topic: {topic}")
        else:
            app.logger.error(f"Gemini error: {response.status_code}")
        return None
    except requests.RequestException as e:
        app.logger.error(f"Gemini network error: {e}")
        return None

def post_to_telegram(post, topic):
    """Post content to Telegram channel with hashtags."""
    try:
        hashtags = " ".join(generate_hashtags(topic, post['category']))
        message = f"{post['title']}\n\n{post['content']}\n\n{hashtags}"
        if len(message) > 4096:
            message = f"{post['title']}\n\n{post['content'][:1000]}...\n\n{hashtags}"
        bot.send_message(TELEGRAM_CHANNEL, message)
        app.logger.info(f"Posted to Telegram: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Telegram: {e}")

def generate_unique_post():
    """Generate unique blog post with category rotation."""
    try:
        existing_data = get_existing_data()
        existing_titles = existing_data['titles']
        existing_contents = existing_data['contents']
        global USED_TOPICS, USED_CONTENTS, LAST_CATEGORY
        if not USED_TOPICS:
            USED_TOPICS = existing_titles
        if not USED_CONTENTS:
            USED_CONTENTS = existing_contents
        topic_data = generate_unique_topic()
        topic = topic_data["topic"]
        category = topic_data["category"]
        while topic in USED_TOPICS or category == LAST_CATEGORY:
            topic_data = generate_unique_topic()
            topic = topic_data["topic"]
            category = topic_data["category"]
        content = generate_post_with_gemini(topic, category)
        if not content or content in USED_CONTENTS or content in existing_contents:
            app.logger.warning(f"Content for topic {topic} is duplicate or empty, skipping.")
            return None
        humanized_content = humanize_content(content)
        cleaned_content = clean_content(humanized_content)
        if not cleaned_content:
            app.logger.warning(f"Failed to generate valid content for topic: {topic}")
            return None
        lines = cleaned_content.split('\n')
        title = next((line.replace("Title: ", "").strip() for line in lines if line.startswith("Title: ")), topic[:50])
        i = 1
        original_title = title
        while title in existing_titles:
            title = f"{original_title} ({i})"
            i += 1
        new_post = {
            "title": title,
            "content": cleaned_content,
            "category": category
        }
        response = supabase.table('tables').insert(new_post).execute()
        inserted_post = response.data[0]
        insert_blog_to_db(inserted_post)
        USED_TOPICS.add(topic)
        USED_CONTENTS.add(cleaned_content)
        LAST_CATEGORY = category
        post_to_telegram(inserted_post, topic)
        app.logger.info(f"Generated post: {title} with category: {category}")
        return new_post
    except Exception as e:
        app.logger.error(f"generate_unique_post error: {str(e)}")
        return None

def auto_generate_and_upload():
    """Auto-generate and upload post."""
    post = generate_unique_post()
    if post:
        app.logger.info(f"Generated post: {post['title']}")

def keep_alive():
    """Keep server alive with improved logging."""
    while True:
        try:
            url = "https://telegram-yvmd.onrender.com/ping"
            app.logger.debug(f"Sending keep-alive ping to {url}")
            response = requests.get(url, timeout=10)
            app.logger.info(f"Keep-alive ping, status: {response.status_code}")
        except Exception as e:
            app.logger.error(f"keep_alive error: {e}")
        time.sleep(300)  # 5 minutes

@app.route('/ping')
def ping():
    """Handle keep-alive pings."""
    app.logger.info("Received ping request.")
    return jsonify({"status": "alive"}), 200

@app.route('/generate', methods=['GET', 'POST'])
def manual_generate():
    try:
        post = generate_unique_post()
        return jsonify({"message": "Post generated" if post else "Failed to generate post", "post": post}), 200 if post else 500
    except Exception as e:
        app.logger.error(f"Error in /generate: {e}")
        return jsonify({"message": "Error generating post", "error": str(e)}), 500

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
        app.logger.error("Failed to initialize database on startup.")
        raise RuntimeError("Database initialization failed")
    if not check_database():
        app.logger.info("Database invalid, populating from Supabase.")
        if not populate_database():
            app.logger.error("Failed to populate database on startup.")
            raise RuntimeError("Database population failed")
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
