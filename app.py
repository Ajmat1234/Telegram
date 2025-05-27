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
TELEGRAM_BOT_TOKEN = "7627792094:AAFGr_KxbimGv4qHzh86bDxCGWPhCgw9wbI"
TELEGRAM_CHANNEL = "https://t.me/TheWatchDraft"
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# SQLite setup
DB_PATH = "blogs.db"

def initialize_database():
    """Initialize SQLite database with retries."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
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
            response = supabase.table('blogs').select('id, title, content, category').range(offset, offset + page_size - 1).execute()
            fetched_blogs = response.data or []
            blogs.extend(fetched_blogs)
            app.logger.info(f"Fetched {len(fetched_blogs)} blogs, total: {len(blogs)}")
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

# Content topics for movies, anime, and adventure
CONTENT_TOPICS = [
    {"topic": "ड्रैगन बॉल Z में सबसे ताकतवर किरदार कौन है?", "category": "Anime"},
    {"topic": "अवतार: द लास्ट एयरबेंडर - क्या हुआ अगर आंग ने ओजाई को हराया नहीं होता?", "category": "Anime"},
    {"topic": "मार्वल की नई फिल्म 'एवेंजर्स: सीक्रेट वॉर्स' की समीक्षा", "category": "Movie"},
    {"topic": "जुरासिक पार्क: अगर डायनासोर वास्तव में बच गए होते तो क्या होता?", "category": "Adventure"},
    {"topic": "नारुतो बनाम सासुके: कौन है असली निंजा?", "category": "Anime"},
    {"topic": "इंडियाना जोन्स की नई साहसिक कहानी की समीक्षा", "category": "Adventure"},
    {"topic": "वन पीस में लफी की ताकत का विश्लेषण", "category": "Anime"},
    {"topic": "द लायन किंग (2025 रीमेक): क्या यह क्लासिक से बेहतर है?", "category": "Movie"},
    {"topic": "अगर हैरी पॉटर में वोल्डेमॉर्ट जीत जाता तो क्या होता?", "category": "Adventure"},
    {"topic": "टोक्यो घoul: कानेकी की ताकत का रहस्य", "category": "Anime"},
    {"topic": "जेम्स बॉन्ड 007 की नई फिल्म की समीक्षा", "category": "Movie"},
    {"topic": "अटैक ऑन टाइटन: क्या एरेन की योजना सही थी?", "category": "Anime"},
    {"topic": "पाइरेट्स ऑफ द कैरेबियन: अगर जैक स्पैरो ने पर्ल को नहीं खोया होता?", "category": "Adventure"},
    {"topic": "ड्यून पार्ट 3: पॉल एटराइड्स की कहानी का विश्लेषण", "category": "Movie"},
    {"topic": "फुलमेटल अल्केमिस्ट: एडवर्ड बनाम मस्टैंग - कौन अधिक शक्तिशाली?", "category": "Anime"},
]

USED_TOPICS = set()
USED_CONTENTS = set()

def generate_slug(title):
    """Generate URL-friendly slug."""
    if not title:
        return "untitled-post"
    slug = re.sub(r'[^\w\s-]', '', title.lower()).replace(' ', '-').strip('-')[:50]
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT title FROM blogs")
        existing_titles = {row[0] for row in cursor.fetchall()}
        conn.close()
        original_slug = slug
        counter = 1
        while slug in existing_titles:
            slug = f"{original_slug}-{counter}"
            counter += 1
        return slug
    except sqlite3.Error as e:
        app.logger.error(f"Error generating slug: {e}")
        return slug

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
    return content.strip()[:4000]  # Limit to 4000 characters

def humanize_content(content):
    """Humanize content using Gemini API."""
    prompt = f"""
    निम्नलिखित सामग्री को एक आकर्षक, मानव जैसी और 2025 के दर्शकों के लिए उपयुक्त ब्लॉग पोस्ट में बदलें।
    सामग्री को हिंदी में लिखें, जो 1000-1500 शब्दों की हो, और अधिकतम 4000 अक्षरों तक सीमित हो।
    लेख को रोचक, जानकारीपूर्ण और प्राकृतिक बनाएं, जिसमें रोबोटिक या AI-जनरेटेड भाषा न हो।
    शुरुआत एक आकर्षक परिचय से करें (जैसे कोई वास्तविक परिदृश्य, सवाल या छोटी कहानी)।
    विश्वसनीय स्रोतों या अध्ययनों का उल्लेख करें (जैसे "2025 में [संस्थान] का अध्ययन" या "[संगठन] के विशेषज्ञों के अनुसार")।
    सरल, SEO-अनुकूल भाषा का उपयोग करें और तार्किक प्रवाह बनाए रखें।
    आउटपुट सादा टेक्स्ट हो, बिना मार्कडाउन प्रतीकों (जैसे **, *, #, या लिंक) के।

    मूल सामग्री:
    {content}
    """
    max_retries = 2
    for attempt in range(max_retries):
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.9, "topK": 40, "topP": 0.95, "maxOutputTokens": 1500}
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
        return {'titles': set(), 'contents': set()}

def generate_post_with_gemini(topic, category):
    """Generate blog post using Gemini API."""
    app.logger.debug(f"Generating post for topic: {topic} with Gemini")
    prompt = f"""
    आप एक विशेषज्ञ सामग्री निर्माता हैं, जो 2025 के दर्शकों के लिए आकर्षक और मानव जैसी ब्लॉग पोस्ट लिखते हैं।
    विषय पर एक ब्लॉग पोस्ट बनाएं: "{topic}"।
    - एक आकर्षक, पेशेवर और SEO-अनुकूल शीर्षक बनाएं (10-15 शब्द, विषय से संबंधित और रोचक)।
    - प्रारूप: एक विस्तृत लेख (लगभग 1000-1500 शब्द, अधिकतम 4000 अक्षर)।
    - शुरुआत एक आकर्षक परिचय से करें (जैसे कोई वास्तविक परिदृश्य, सवाल या छोटी कहानी)।
    - विश्वसनीय स्रोतों या अध्ययनों का उल्लेख करें (जैसे "2025 में [संस्थान] का अध्ययन" या "[संगठन] के विशेषज्ञों के अनुसार")।
    - सरल, बातचीत जैसी, SEO-अनुकूल हिंदी भाषा का उपयोग करें जो प्राकृतिक लगे।
    - आउटपुट सादा टेक्स्ट हो, बिना मार्कडाउन प्रतीकों (जैसे **, *, #, या लिंक) के।
    - लेख को {category} श्रेणी के लिए उपयुक्त बनाएं।

    उदाहरण:
    शीर्षक: ड्रैगन बॉल Z में सबसे ताकतवर किरदार कौन है?
    परिचय: क्या आपने कभी सोचा कि अगर गोकू और वेजिटा आमने-सामने लड़ें तो कौन जीतेगा? [आगे रोचक सामग्री...]
    """
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "topK": 40, "topP": 0.9, "maxOutputTokens": 1500}
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

def post_to_telegram(post):
    """Post content to Telegram channel."""
    try:
        message = f"{post['title']}\n\n{post['content'][:1000]}...\n\n#TheWatchDraft #{post['category']}"
        bot.send_message(TELEGRAM_CHANNEL, message)
        app.logger.info(f"Posted to Telegram: {post['title']}")
    except Exception as e:
        app.logger.error(f"Error posting to Telegram: {e}")

def generate_unique_post():
    """Generate unique blog post with shuffled topics."""
    try:
        existing_data = get_existing_data()
        existing_titles = existing_data['titles']
        existing_contents = existing_data['contents']
        global USED_TOPICS, USED_CONTENTS
        if not USED_TOPICS:
            USED_TOPICS = existing_titles
        if not USED_CONTENTS:
            USED_CONTENTS = existing_contents
        all_topics = CONTENT_TOPICS
        available_topics = [t for t in all_topics if t["topic"] not in USED_TOPICS]
        random.shuffle(available_topics)
        if not available_topics:
            app.logger.warning("All topics used. Resetting.")
            USED_TOPICS.clear()
            available_topics = all_topics
            random.shuffle(available_topics)
        for topic_data in available_topics:
            topic = topic_data["topic"]
            category = topic_data["category"]
            content = generate_post_with_gemini(topic, category)
            if not content or content in USED_CONTENTS or content in existing_contents:
                continue
            humanized_content = humanize_content(content)
            cleaned_content = clean_content(humanized_content)
            if not cleaned_content:
                app.logger.warning(f"Failed to generate valid content for topic: {topic}")
                continue
            lines = cleaned_content.split('\n')
            title = next((line.replace("शीर्षक: ", "").strip() for line in lines if line.startswith("शीर्षक: ")), topic[:50])
            slug = generate_slug(title)
            i = 1
            while title in existing_titles:
                title = f"{title} ({i})"
                slug = f"{generate_slug(title)}-{i}"
                i += 1
            new_post = {
                "title": title,
                "content": cleaned_content,
                "category": category
            }
            response = supabase.table('blogs').insert(new_post).execute()
            inserted_post = response.data[0]
            insert_blog_to_db(inserted_post)
            USED_TOPICS.add(topic)
            USED_CONTENTS.add(cleaned_content)
            post_to_telegram(inserted_post)
            app.logger.info(f"Generated post: {title} with category: {category}")
            return new_post
        app.logger.error("Could not generate unique post.")
        return None
    except Exception as e:
        app.logger.error(f"generate_unique_post error: {e}")
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
            requests.get("https://your-app-url.onrender.com/ping")
            app.logger.info("Keep-alive ping.")
        except Exception as e:
            app.logger.error(f"keep_alive error: {e}")
        time.sleep(300)

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
