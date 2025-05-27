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
import asyncio

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
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL:
    app.logger.error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL must be set.")
    raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL must be set.")

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

# Categories for dynamic topic generation
CATEGORIES = ["Anime", "Movie", "Adventure"]

def generate_dynamic_topic():
    """Generate a unique topic using Gemini API."""
    prompt = """
    Suggest a unique topic related to movies, anime, or adventure for a 2025 audience. The topic should be specific, engaging, and suitable for a blog post. Focus on fresh ideas, theories, or hypothetical scenarios not commonly discussed. Avoid these topics: Dragon Ball Z power rankings, Avatar: The Last Airbender Ozai scenarios, Avengers: Secret Wars reviews, Jurassic Park dinosaur survival, Naruto vs Sasuke, Indiana Jones adventures, One Piece Luffy strength, Lion King 2025 remake, Harry Potter Voldemort wins, Tokyo Ghoul Kaneki powers, James Bond reviews, Attack on Titan Eren’s plan, Pirates of the Caribbean Jack Sparrow, Dune Part 3, Fullmetal Alchemist Edward vs Mustang, Matrix Resurrections Neo, Bleach Ichigo Bankai, Star Wars alternate endings, Demon Slayer Tanjiro vs Muzan, Witcher Geralt journey, Spider-Man multiverse, My Hero Academia Deku quirk, Lord of the Rings Frodo ring, Black Clover Asta anti-magic, Dark Knight Joker philosophy, Jujutsu Kaisen Gojo strength, Hobbit Bilbo adventures, Hunter x Hunter Gon vs Killua, Inception dream explanation, Sword Art Online Kirito journey, Narnia Aslan symbolism, Death Note Light morality, Godfather Michael transformation. Provide the topic and its category (Anime, Movie, or Adventure) in the format: {"topic": "<topic>", "category": "<category>"}.
    """
    try:
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.8, "topK": 40, "topP": 0.9, "maxOutputTokens": 100}
        }
        response = requests.post(GENINI_URL, headers=HEADERS, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        topic_data = json.loads(data["candidates"][0]["content"]["parts"][0]["text"].strip())
        app.logger.info(f"Generated dynamic topic: {topic_data['topic']}")
        return topic_data
    except Exception as e:
        app.logger.error(f"Failed to generate dynamic topic: {e}")
        return {"topic": "What if Spider-Man joined the X-Men?", "category": "Movie"}  # Fallback

def clean_content(content):
    """Remove markdown symbols from content to ensure plain text."""
    if notやる content:
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
    return content.strip()[:3000]

def humanize_content(content):
    """Humanize content using Gemini API with a fun, engaging tone."""
    prompt = f"""
    Turn the following content into an engaging, human-like blog post for a 2025 audience. Write in pure English with a fun, humorous, and conversational tone, like a witty friend chatting about movies or anime. Keep it concise (800-1000 words, max 3000 characters). Start with a catchy, relatable intro (e.g., a funny scenario or question). Add humor and light-hearted vibes to make it entertaining. Include credible sources (e.g., "A 2025 CinemaScope study says..." or "Experts at PopCultureLab claim..."). End with an engaging question to spark discussion. Ensure a logical flow and SEO-friendly language. Output plain text, no markdown symbols.

    Original Content:
    {content}
    """
    max_retries = 2
    for attempt in range(max_retries):
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.9, "topK": 40, "topP": 0.95, "maxOutputTokens": 2048}
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

def generate_post_with_gemini(topic, category):
    """Generate blog post using Gemini API with theories and hashtags."""
    app.logger.debug(f"Generating post for topic: {topic} with Gemini")
    prompt = f"""
    You are an expert content creator writing for a 2025 audience. Create a blog post on the topic: "{topic}".
    - Write a catchy, professional, SEO-friendly title (10-15 words).
    - Format: A concise article (800-1000 words, max 3000 characters).
    - Start with a fun, relatable intro (e.g., a humorous scenario or question).
    - Use pure English with a witty, conversational tone, like a friend joking about movies or anime.
    - Include credible sources (e.g., "A 2025 CinemaScope study says..." or "PopCultureLab experts claim...").
    - Add humor and light-hearted vibes to make it engaging.
    - Include theories or hypothetical scenarios to make the content unique.
    - End with an engaging question to spark discussion.
    - Generate 10 topic-specific hashtags relevant to the topic and category ({category}).
    - Output plain text, no markdown symbols, in this format:
    Title: <title>
    Content: <content>
    Hashtags: <hashtag1> <hashtag2> ... <hashtag10>

    Example:
    Title: Could Spider-Man Join the X-Men?
    Content: Ever wondered what would happen if Peter Parker ditched his solo gigs and teamed up with the X-Men? Picture this: Spidey swinging into Xavier’s School, cracking jokes while Wolverine growls at him. A 2025 CinemaScope study says crossovers like this are pure fanbait, and we’re here for it! ... [continues with humor and theories] So, what do you think – would Spidey fit with the mutants?
    Hashtags: #SpiderMan #XMen #MarvelMovies #SuperheroCrossover #PeterParker #MutantMayhem #MarvelTheory #ComicBookFun #MCU2025 #HeroVibes
    """
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "topK": 40, "topP": 0.9, "maxOutputTokens": 2048}
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

async def post_to_telegram(post):
    """Post content to Telegram channel, handling long content."""
    try:
        message = f"{post['title']}\n\n{post['content'][:1500]}...\n\nJoin @TheWatchDraft for more! {post['hashtags']}"
        if len(message) > 4096:  # Telegram's max message length
            message = f"{post['title']}\n\n{post['content'][:1000]}...\n\nJoin @TheWatchDraft for more! {post['hashtags']}"
        await bot.send_message(TELEGRAM_CHANNEL, message)
        app.logger.info(f"Posted to Telegram: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Telegram: {e}")

async def generate_unique_post():
    """Generate unique blog post with dynamic topics."""
    try:
        existing_data = get_existing_data()
        existing_titles = existing_data['titles']
        existing_contents = existing_data['contents']
        global USED_TOPICS, USED_CONTENTS, LAST_CATEGORY
        if not USED_TOPICS:
            USED_TOPICS = existing_titles
        if not USED_CONTENTS:
            USED_CONTENTS = existing_contents

        # Generate dynamic topic
        topic_data = generate_dynamic_topic()
        topic = topic_data["topic"]
        category = topic_data["category"]

        # Ensure category rotation
        if category == LAST_CATEGORY:
            app.logger.warning("Same category as last post, generating new topic.")
            topic_data = generate_dynamic_topic()
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

        # Parse title and hashtags
        lines = cleaned_content.split('\n')
        title = next((line.replace("Title: ", "").strip() for line in lines if line.startswith("Title: ")), topic[:50])
        hashtags = next((line.replace("Hashtags: ", "").strip() for line in lines if line.startswith("Hashtags: ")), "")
        content_lines = [line for line in lines if not line.startswith(("Title: ", "Hashtags: "))]
        cleaned_content = "\n".join(content_lines).strip()

        i = 1
        original_title = title
        while title in existing_titles:
            title = f"{original_title} ({i})"
            i += 1

        new_post = {
            "title": title,
            "content": cleaned_content,
            "category": category,
            "hashtags": hashtags
        }
        response = supabase.table('tables').insert(new_post).execute()
        inserted_post = response.data[0]
        insert_blog_to_db(inserted_post)
        USED_TOPICS.add(topic)
        USED_CONTENTS.add(cleaned_content)
        LAST_CATEGORY = category
        await post_to_telegram(inserted_post)
        app.logger.info(f"Generated post: {title} with category: {category}")
        return new_post
    except Exception as e:
        app.logger.error(f"generate_unique_post error: {str(e)}")
        return None

async def auto_generate_and_upload():
    """Auto-generate and upload post."""
    post = await generate_unique_post()
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
async def manual_generate():
    try:
        post = await generate_unique_post()
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

async def run_scheduler():
    """Run scheduler for periodic tasks."""
    schedule.every(1).hours.do(lambda: asyncio.run(auto_generate_and_upload()))
    while True:
        schedule.run_pending()
        await asyncio.sleep(60)

def start_scheduler():
    """Start the async scheduler in a separate thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_scheduler())
    loop.close()

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
    threading.Thread(target=start_scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
