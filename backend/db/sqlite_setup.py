"""
Sunday Study Club - Database Setup Script
==========================================
Parses raw Bible data (KJV, ASV, Webster), Strong's Concordance (Greek/Hebrew),
and cross-references, then loads everything into a single SQLite database.

Usage:
    python sqlite_setup.py

Run from the backend/db/ directory, or adjust DATA_RAW_PATH below.
"""

import sqlite3
import json
import re
import os
import sys

# ============================================================
# CONFIGURATION - adjust these paths if your folder structure differs
# ============================================================

# Path to the data_raw folder (relative to where this script lives)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
DATA_RAW_PATH = os.path.join(PROJECT_ROOT, "data_raw")

# Bible translation database files
BIBLE_DBS = {
    "KJV": os.path.join(DATA_RAW_PATH, "bible_texts", "KJV.db"),
    "ASV": os.path.join(DATA_RAW_PATH, "bible_texts", "ASV.db"),
    "Webster": os.path.join(DATA_RAW_PATH, "bible_texts", "Webster.db"),
}

# Strong's dictionary JS files
STRONGS_GREEK_JS = os.path.join(DATA_RAW_PATH, "strongs", "strongs-master", "greek", "strongs-greek-dictionary.js")
STRONGS_HEBREW_JS = os.path.join(DATA_RAW_PATH, "strongs", "strongs-master", "hebrew", "strongs-hebrew-dictionary.js")
STRONGS_HEBREW_XML = os.path.join(DATA_RAW_PATH, "strongs", "strongs-master", "hebrew", "StrongHebrewG.xml")

# Cross-references file
CROSS_REFS_FILE = os.path.join(DATA_RAW_PATH, "cross_references.txt")

# Output database
OUTPUT_DB = os.path.join(SCRIPT_DIR, "bible.db")

# ============================================================
# BOOK MAPPING - maps book numbers to names and testament
# ============================================================

BOOKS = {
    1: ("Genesis", "OT"), 2: ("Exodus", "OT"), 3: ("Leviticus", "OT"),
    4: ("Numbers", "OT"), 5: ("Deuteronomy", "OT"), 6: ("Joshua", "OT"),
    7: ("Judges", "OT"), 8: ("Ruth", "OT"), 9: ("1 Samuel", "OT"),
    10: ("2 Samuel", "OT"), 11: ("1 Kings", "OT"), 12: ("2 Kings", "OT"),
    13: ("1 Chronicles", "OT"), 14: ("2 Chronicles", "OT"), 15: ("Ezra", "OT"),
    16: ("Nehemiah", "OT"), 17: ("Esther", "OT"), 18: ("Job", "OT"),
    19: ("Psalms", "OT"), 20: ("Proverbs", "OT"), 21: ("Ecclesiastes", "OT"),
    22: ("Song of Solomon", "OT"), 23: ("Isaiah", "OT"), 24: ("Jeremiah", "OT"),
    25: ("Lamentations", "OT"), 26: ("Ezekiel", "OT"), 27: ("Daniel", "OT"),
    28: ("Hosea", "OT"), 29: ("Joel", "OT"), 30: ("Amos", "OT"),
    31: ("Obadiah", "OT"), 32: ("Jonah", "OT"), 33: ("Micah", "OT"),
    34: ("Nahum", "OT"), 35: ("Habakkuk", "OT"), 36: ("Zephaniah", "OT"),
    37: ("Haggai", "OT"), 38: ("Zechariah", "OT"), 39: ("Malachi", "OT"),
    40: ("Matthew", "NT"), 41: ("Mark", "NT"), 42: ("Luke", "NT"),
    43: ("John", "NT"), 44: ("Acts", "NT"), 45: ("Romans", "NT"),
    46: ("1 Corinthians", "NT"), 47: ("2 Corinthians", "NT"),
    48: ("Galatians", "NT"), 49: ("Ephesians", "NT"), 50: ("Philippians", "NT"),
    51: ("Colossians", "NT"), 52: ("1 Thessalonians", "NT"),
    53: ("2 Thessalonians", "NT"), 54: ("1 Timothy", "NT"),
    55: ("2 Timothy", "NT"), 56: ("Titus", "NT"), 57: ("Philemon", "NT"),
    58: ("Hebrews", "NT"), 59: ("James", "NT"), 60: ("1 Peter", "NT"),
    61: ("2 Peter", "NT"), 62: ("1 John", "NT"), 63: ("2 John", "NT"),
    64: ("3 John", "NT"), 65: ("Jude", "NT"), 66: ("Revelation", "NT"),
}


def create_tables(conn):
    """Create all database tables."""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS verses")
    cursor.execute("DROP TABLE IF EXISTS strongs_greek")
    cursor.execute("DROP TABLE IF EXISTS strongs_hebrew")
    cursor.execute("DROP TABLE IF EXISTS cross_references")
    cursor.execute("DROP TABLE IF EXISTS users")

    cursor.execute("""
        CREATE TABLE verses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_number INTEGER NOT NULL,
            book_name TEXT NOT NULL,
            chapter INTEGER NOT NULL,
            verse INTEGER NOT NULL,
            text TEXT NOT NULL,
            translation TEXT NOT NULL,
            testament TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE strongs_greek (
            strongs_number TEXT PRIMARY KEY,
            greek_word TEXT,
            transliteration TEXT,
            pronunciation TEXT,
            definition TEXT,
            short_definition TEXT,
            usage_count INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE strongs_hebrew (
            strongs_number TEXT PRIMARY KEY,
            hebrew_word TEXT,
            transliteration TEXT,
            pronunciation TEXT,
            definition TEXT,
            short_definition TEXT,
            usage_count INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE cross_references (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_verse TEXT NOT NULL,
            to_verse TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY,
            email TEXT,
            daily_question_count INTEGER DEFAULT 0,
            last_reset_date TEXT,
            tier TEXT DEFAULT 'free'
        )
    """)

    # Create indexes for faster queries
    cursor.execute("CREATE INDEX idx_verses_translation ON verses(translation)")
    cursor.execute("CREATE INDEX idx_verses_book ON verses(book_number)")
    cursor.execute("CREATE INDEX idx_verses_testament ON verses(testament)")
    cursor.execute("CREATE INDEX idx_verses_ref ON verses(book_name, chapter, verse)")
    cursor.execute("CREATE INDEX idx_cross_refs_from ON cross_references(from_verse)")
    cursor.execute("CREATE INDEX idx_cross_refs_to ON cross_references(to_verse)")

    conn.commit()
    print("[OK] Tables created successfully.")


def load_bible_translation(conn, translation_name, db_path):
    """Load a Bible translation from a scrollmapper SQLite database."""
    if not os.path.exists(db_path):
        print(f"[SKIP] {translation_name}: file not found at {db_path}")
        return 0

    source_conn = sqlite3.connect(db_path)
    source_cursor = source_conn.cursor()

    # The scrollmapper databases typically have a table called 't_kjv', 't_asv', etc.
    # or a generic table called 'verses' or 't_web'
    # Let's find the actual table name
    source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in source_cursor.fetchall()]
    print(f"  Tables found in {translation_name}.db: {tables}")

    # Try common table name patterns
    verse_table = None
    for table in tables:
        # scrollmapper format: KJV_verses, ASV_verses, Webster_verses
        if "_verses" in table.lower():
            verse_table = table
            break
        elif table.startswith("t_") or table == "verses" or table.lower() == translation_name.lower():
            verse_table = table
            break

    if not verse_table and tables:
        # Use the first non-system table that isn't 'translations', 'sqlite_sequence', or '*_books'
        for table in tables:
            if table not in ('translations', 'sqlite_sequence') and '_books' not in table:
                verse_table = table
                break

    if not verse_table:
        print(f"[ERROR] {translation_name}: no verse table found")
        source_conn.close()
        return 0

    print(f"  Using table: {verse_table}")

    # Get column names to understand the structure
    source_cursor.execute(f"PRAGMA table_info({verse_table})")
    columns = [row[1] for row in source_cursor.fetchall()]
    print(f"  Columns: {columns}")

    # Read all verses - the scrollmapper format typically has columns:
    # id, b (book number), c (chapter), v (verse), t (text)
    # But some may use 'book', 'chapter', 'verse', 'text'
    # We need to figure out which column mapping to use

    # Try the common short format first (b, c, v, t)
    book_col = None
    chapter_col = None
    verse_col = None
    text_col = None

    for col in columns:
        col_lower = col.lower()
        if col_lower in ('b', 'book', 'book_number', 'book_id'):
            book_col = col
        elif col_lower in ('c', 'chapter', 'chapter_number', 'chapter_id'):
            chapter_col = col
        elif col_lower in ('v', 'verse', 'verse_number', 'verse_id') and 'text' not in col_lower:
            verse_col = col
        elif col_lower in ('t', 'text', 'scripture', 'verse_text', 'content'):
            text_col = col

    if not all([book_col, chapter_col, verse_col, text_col]):
        print(f"[ERROR] {translation_name}: could not identify columns. Found: {columns}")
        source_conn.close()
        return 0

    query = f"SELECT {book_col}, {chapter_col}, {verse_col}, {text_col} FROM {verse_table}"
    source_cursor.execute(query)
    rows = source_cursor.fetchall()

    # Insert into our database
    cursor = conn.cursor()
    count = 0
    for row in rows:
        book_num, chapter, verse, text = row
        book_num = int(book_num)

        if book_num in BOOKS:
            book_name, testament = BOOKS[book_num]
        else:
            book_name = f"Book {book_num}"
            testament = "OT" if book_num <= 39 else "NT"

        cursor.execute("""
            INSERT INTO verses (book_number, book_name, chapter, verse, text, translation, testament)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (book_num, book_name, int(chapter), int(verse), str(text).strip(), translation_name, testament))
        count += 1

    conn.commit()
    source_conn.close()
    print(f"[OK] {translation_name}: loaded {count:,} verses.")
    return count


def parse_strongs_js(filepath):
    """
    Parse a Strong's dictionary .js file.
    These files export a JavaScript object. We need to extract the JSON data from them.
    The format is typically: var defined_variable = { "G1": {...}, "G2": {...}, ... }
    """
    if not os.path.exists(filepath):
        print(f"[SKIP] Strong's file not found: {filepath}")
        return {}

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Remove the JavaScript variable assignment to get pure JSON
    # The file typically starts with something like:
    # var defined = { or module.exports = {
    # Find the first '{' and take everything from there
    first_brace = content.find('{')
    if first_brace == -1:
        print(f"[ERROR] Could not find JSON data in {filepath}")
        return {}

    json_str = content[first_brace:]

    # Remove trailing semicolons or other JS syntax
    json_str = json_str.rstrip().rstrip(';').rstrip()

    # The JS file may have multiple top-level objects or extra code after the main object.
    # We need to find just the first complete JSON object by matching braces.
    brace_depth = 0
    end_pos = 0
    for i, char in enumerate(json_str):
        if char == '{':
            brace_depth += 1
        elif char == '}':
            brace_depth -= 1
            if brace_depth == 0:
                end_pos = i + 1
                break

    if end_pos > 0:
        json_str = json_str[:end_pos]

    # Try to parse as JSON
    try:
        data = json.loads(json_str)
        return data
    except json.JSONDecodeError as e:
        print(f"[WARNING] Standard JSON parse failed: {e}")
        print("  Attempting to fix common JSON issues...")

        # Sometimes the JS files have trailing commas or other issues
        # Try removing trailing commas before } and ]
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)

        try:
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e2:
            print(f"[WARNING] Comma fix didn't help: {e2}")
            print("  Attempting character-by-character repair...")

            # Fix unescaped characters that break JSON
            # Common issues in Hebrew files: unescaped quotes, special chars
            # Strategy: fix unescaped double quotes inside string values
            fixed = []
            in_string = False
            escape_next = False
            for i, char in enumerate(json_str):
                if escape_next:
                    fixed.append(char)
                    escape_next = False
                    continue
                if char == '\\':
                    fixed.append(char)
                    escape_next = True
                    continue
                if char == '"':
                    if not in_string:
                        in_string = True
                        fixed.append(char)
                    else:
                        # Check if this quote ends the string or is inside it
                        # Look ahead: if next non-whitespace is : , } ] then it ends the string
                        rest = json_str[i+1:i+20].lstrip()
                        if rest and rest[0] in (':', ',', '}', ']'):
                            in_string = False
                            fixed.append(char)
                        else:
                            # This is an unescaped quote inside a string - escape it
                            fixed.append('\\"')
                    continue
                fixed.append(char)

            try:
                data = json.loads(''.join(fixed))
                print("  [OK] Character repair succeeded!")
                return data
            except json.JSONDecodeError as e3:
                print(f"[ERROR] All parse attempts failed for {filepath}: {e3}")
                print("  Try opening the file manually and checking around line 3969.")
                return {}


def load_strongs_greek(conn, filepath):
    """Load Strong's Greek dictionary into the database."""
    data = parse_strongs_js(filepath)
    if not data:
        return 0

    cursor = conn.cursor()
    count = 0

    for key, entry in data.items():
        strongs_num = key  # e.g., "G1", "G2", etc.

        # Extract fields - the structure varies but commonly includes:
        # lemma (Greek word), translit, pronounce, strongs_def, kjv_def
        greek_word = entry.get("lemma", entry.get("word", ""))
        transliteration = entry.get("translit", entry.get("transliteration", ""))
        pronunciation = entry.get("pronounce", entry.get("pronunciation", ""))

        # Definition - try multiple field names
        definition = entry.get("strongs_def", entry.get("strongsdef",
                     entry.get("definition", entry.get("derivation", ""))))

        # Short definition from KJV usage
        short_def = entry.get("kjv_def", entry.get("kjvdef", ""))

        cursor.execute("""
            INSERT OR REPLACE INTO strongs_greek
            (strongs_number, greek_word, transliteration, pronunciation, definition, short_definition)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (strongs_num, greek_word, transliteration, pronunciation, definition, short_def))
        count += 1

    conn.commit()
    print(f"[OK] Strong's Greek: loaded {count:,} entries.")
    return count


def load_strongs_hebrew(conn, filepath):
    """Load Strong's Hebrew dictionary. Try JS first, fall back to XML."""
    # First try the JS file
    data = parse_strongs_js(filepath)
    
    if not data:
        # Fall back to XML file
        print("  Falling back to Hebrew XML file...")
        import xml.etree.ElementTree as ET
        
        xml_path = STRONGS_HEBREW_XML
        if not os.path.exists(xml_path):
            print(f"[ERROR] Hebrew XML file not found: {xml_path}")
            return 0
        
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            print(f"  XML root tag: {root.tag}")
            print(f"  XML children: {[child.tag for child in root][:5]}")
        except ET.ParseError as e:
            print(f"[ERROR] Could not parse Hebrew XML: {e}")
            return 0
        
        # Parse with namespace
        NS = '{http://www.bibletechnologies.net/2003/OSIS/namespace}'
        
        cursor = conn.cursor()
        count = 0
        
        # Count all divs first
        all_divs = list(root.iter(NS + 'div'))
        print(f"  Total div elements found: {len(all_divs)}")
        entry_divs = [d for d in all_divs if d.get('type') == 'entry']
        print(f"  Entry div elements found: {len(entry_divs)}")

        # Find ALL div elements and filter for type="entry"
        for entry in root.iter(NS + 'div'):
            # print(f"  Found div: type={entry.get('type')}, n={entry.get('n')}")
            if entry.get('type') != 'entry':
                continue
            
            # Get the <w> element with Hebrew word data
            w_elem = entry.find(NS + 'w')
            if w_elem is None:
                continue
            
            strongs_id = w_elem.get('ID', '')
            if not strongs_id:
                continue
            
            hebrew_word = w_elem.get('lemma', '') or (w_elem.text or '').strip()
            transliteration = w_elem.get('xlit', '')
            pronunciation = w_elem.get('POS', '')
            
            definition = ''
            short_def = ''
            
            for note in entry.findall(NS + 'note'):
                note_type = note.get('type', '')
                note_text = ''.join(note.itertext()).strip()
                
                if note_type == 'exegesis':
                    definition = note_text
                elif note_type == 'explanation':
                    if definition:
                        definition = definition + ' — ' + note_text
                    else:
                        definition = note_text
                elif note_type == 'translation':
                    short_def = note_text
            
            # Grab list items as detailed meanings
            list_elem = entry.find(NS + 'list')
            if list_elem is not None:
                items = []
                for item in list_elem.findall(NS + 'item'):
                    item_text = ''.join(item.itertext()).strip()
                    if item_text:
                        items.append(item_text)
                if items:
                    list_text = '; '.join(items)
                    if definition:
                        definition = definition + ' | Meanings: ' + list_text
                    else:
                        definition = list_text
            
            cursor.execute("""
                INSERT OR REPLACE INTO strongs_hebrew
                (strongs_number, hebrew_word, transliteration, pronunciation, definition, short_definition)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (strongs_id, hebrew_word, transliteration, pronunciation, definition, short_def))
            count += 1
        
        conn.commit()
        print(f"[OK] Strong's Hebrew (from XML): loaded {count:,} entries.")
        return count
    
    # If JS parsing worked, use that data
    cursor = conn.cursor()
    count = 0

    for key, entry in data.items():
        strongs_num = key
        hebrew_word = entry.get("lemma", entry.get("word", ""))
        transliteration = entry.get("translit", entry.get("transliteration", ""))
        pronunciation = entry.get("pronounce", entry.get("pronunciation", ""))
        definition = entry.get("strongs_def", entry.get("strongsdef",
                     entry.get("definition", entry.get("derivation", ""))))
        short_def = entry.get("kjv_def", entry.get("kjvdef", ""))

        cursor.execute("""
            INSERT OR REPLACE INTO strongs_hebrew
            (strongs_number, hebrew_word, transliteration, pronunciation, definition, short_definition)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (strongs_num, hebrew_word, transliteration, pronunciation, definition, short_def))
        count += 1

    conn.commit()
    print(f"[OK] Strong's Hebrew: loaded {count:,} entries.")
    return count

def load_cross_references(conn, filepath):
    """
    Load cross-references from the OpenBible.info TSK file.
    Format is tab-separated: from_verse  to_verse  votes
    Verse format is like: Gen.1.1 or Matt.5.3
    """
    if not os.path.exists(filepath):
        print(f"[SKIP] Cross-references file not found: {filepath}")
        return 0

    cursor = conn.cursor()
    count = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('From'):
                continue  # Skip comments and headers

            parts = line.split('\t')
            if len(parts) >= 2:
                from_verse = parts[0].strip()
                to_verse = parts[1].strip()

                cursor.execute("""
                    INSERT INTO cross_references (from_verse, to_verse)
                    VALUES (?, ?)
                """, (from_verse, to_verse))
                count += 1

    conn.commit()
    print(f"[OK] Cross-references: loaded {count:,} entries.")
    return count


def main():
    print("=" * 60)
    print("  Sunday Study Club - Database Setup")
    print("=" * 60)
    print()

    # Check that data_raw folder exists
    if not os.path.exists(DATA_RAW_PATH):
        print(f"[ERROR] data_raw folder not found at: {DATA_RAW_PATH}")
        print("  Make sure you run this script from the backend/db/ directory")
        print("  or that data_raw/ exists at the project root.")
        sys.exit(1)

    # Remove old database if it exists
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
        print(f"[INFO] Removed old database: {OUTPUT_DB}")

    # Connect to new database
    conn = sqlite3.connect(OUTPUT_DB)
    print(f"[INFO] Creating database: {OUTPUT_DB}")
    print()

    # Step 1: Create tables
    print("--- Step 1: Creating tables ---")
    create_tables(conn)
    print()

    # Step 2: Load Bible translations
    print("--- Step 2: Loading Bible translations ---")
    total_verses = 0
    for name, path in BIBLE_DBS.items():
        total_verses += load_bible_translation(conn, name, path)
    print(f"  Total verses across all translations: {total_verses:,}")
    print()

    # Step 3: Load Strong's Concordance
    print("--- Step 3: Loading Strong's Concordance ---")
    greek_count = load_strongs_greek(conn, STRONGS_GREEK_JS)
    hebrew_count = load_strongs_hebrew(conn, STRONGS_HEBREW_JS)
    print(f"  Total Strong's entries: {greek_count + hebrew_count:,}")
    print()

    # Step 4: Load cross-references
    print("--- Step 4: Loading cross-references ---")
    cross_ref_count = load_cross_references(conn, CROSS_REFS_FILE)
    print()

    # Summary
    print("=" * 60)
    print("  DATABASE SETUP COMPLETE")
    print("=" * 60)
    print(f"  Bible verses:       {total_verses:,}")
    print(f"  Strong's Greek:     {greek_count:,}")
    print(f"  Strong's Hebrew:    {hebrew_count:,}")
    print(f"  Cross-references:   {cross_ref_count:,}")
    print(f"  Database location:  {OUTPUT_DB}")
    print(f"  Database size:      {os.path.getsize(OUTPUT_DB) / (1024*1024):.1f} MB")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
