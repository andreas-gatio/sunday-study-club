"""
Sunday Study Club - Data Verification Script
=============================================
Runs quality checks on the loaded database to make sure
all data was imported correctly.

Usage:
    python verify_data.py

Run from the backend/db/ directory (same location as bible.db).
"""

import sqlite3
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "bible.db")

EXPECTED_VERSE_COUNT = 31102  # Standard Bible verse count (approximate)
EXPECTED_GREEK_MIN = 5500     # Strong's Greek entries (approximately 5,624)
EXPECTED_HEBREW_MIN = 8500    # Strong's Hebrew entries (approximately 8,674)
EXPECTED_CROSS_REFS_MIN = 300000  # Cross-references (approximately 340,000)


def check(label, passed, detail=""):
    """Print a check result."""
    status = "PASS" if passed else "FAIL"
    icon = "[OK]" if passed else "[!!]"
    msg = f"  {icon} {label}"
    if detail:
        msg += f" -- {detail}"
    print(msg)
    return passed


def main():
    print("=" * 60)
    print("  Sunday Study Club - Data Verification")
    print("=" * 60)
    print()

    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Database not found at: {DB_PATH}")
        print("  Run sqlite_setup.py first to create the database.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    all_passed = True

    # ---- CHECK 1: Verse counts per translation ----
    print("--- Check 1: Verse counts per translation ---")

    cursor.execute("SELECT translation, COUNT(*) FROM verses GROUP BY translation")
    translation_counts = cursor.fetchall()

    if not translation_counts:
        all_passed = check("Any translations loaded", False, "No verses found!")
    else:
        for translation, count in translation_counts:
            # Allow some flexibility (some translations may have slight variations)
            passed = count >= (EXPECTED_VERSE_COUNT - 200) and count <= (EXPECTED_VERSE_COUNT + 200)
            all_passed &= check(
                f"{translation} verse count",
                passed,
                f"{count:,} verses (expected ~{EXPECTED_VERSE_COUNT:,})"
            )
    print()

    # ---- CHECK 2: Both testaments present ----
    print("--- Check 2: Testament coverage ---")

    for translation, _ in translation_counts:
        cursor.execute("SELECT testament, COUNT(*) FROM verses WHERE translation=? GROUP BY testament", (translation,))
        testament_counts = dict(cursor.fetchall())

        ot_count = testament_counts.get("OT", 0)
        nt_count = testament_counts.get("NT", 0)

        all_passed &= check(
            f"{translation} OT",
            ot_count > 20000,
            f"{ot_count:,} verses"
        )
        all_passed &= check(
            f"{translation} NT",
            nt_count > 7000,
            f"{nt_count:,} verses"
        )
    print()

    # ---- CHECK 3: All 66 books present ----
    print("--- Check 3: Book coverage ---")

    for translation, _ in translation_counts:
        cursor.execute("SELECT COUNT(DISTINCT book_number) FROM verses WHERE translation=?", (translation,))
        book_count = cursor.fetchone()[0]
        all_passed &= check(
            f"{translation} books",
            book_count == 66,
            f"{book_count} books (expected 66)"
        )
    print()

    # ---- CHECK 4: Strong's Greek entries ----
    print("--- Check 4: Strong's Greek ---")

    cursor.execute("SELECT COUNT(*) FROM strongs_greek")
    greek_count = cursor.fetchone()[0]
    all_passed &= check(
        "Greek entry count",
        greek_count >= EXPECTED_GREEK_MIN,
        f"{greek_count:,} entries (expected ~5,624)"
    )

    # Spot check known entries
    spot_checks_greek = [
        ("G26", "agape"),    # love
        ("G2316", "theos"),  # God
        ("G5547", "Christos"),  # Christ
        ("G4102", "pistis"),  # faith
        ("G5485", "charis"),  # grace
    ]

    for strongs_num, expected_translit in spot_checks_greek:
        cursor.execute("SELECT transliteration, definition FROM strongs_greek WHERE strongs_number=?", (strongs_num,))
        row = cursor.fetchone()
        if row:
            translit = row[0] or ""
            has_data = len(translit) > 0 or len(row[1] or "") > 0
            all_passed &= check(
                f"Greek {strongs_num}",
                has_data,
                f"transliteration: '{translit}'"
            )
        else:
            all_passed &= check(f"Greek {strongs_num}", False, "NOT FOUND")
    print()

    # ---- CHECK 5: Strong's Hebrew entries ----
    print("--- Check 5: Strong's Hebrew ---")

    cursor.execute("SELECT COUNT(*) FROM strongs_hebrew")
    hebrew_count = cursor.fetchone()[0]
    all_passed &= check(
        "Hebrew entry count",
        hebrew_count >= EXPECTED_HEBREW_MIN,
        f"{hebrew_count:,} entries (expected ~8,674)"
    )

    spot_checks_hebrew = [
        ("H430", "elohiym"),   # God
        ("H3068", "Yehovah"),  # LORD (Yahweh)
        ("H7965", "shalowm"),  # peace
        ("H2617", "checed"),   # lovingkindness/mercy
        ("H1285", "beriyth"),  # covenant
    ]

    for strongs_num, expected_translit in spot_checks_hebrew:
        cursor.execute("SELECT transliteration, definition FROM strongs_hebrew WHERE strongs_number=?", (strongs_num,))
        row = cursor.fetchone()
        if row:
            translit = row[0] or ""
            has_data = len(translit) > 0 or len(row[1] or "") > 0
            all_passed &= check(
                f"Hebrew {strongs_num}",
                has_data,
                f"transliteration: '{translit}'"
            )
        else:
            all_passed &= check(f"Hebrew {strongs_num}", False, "NOT FOUND")
    print()

    # ---- CHECK 6: Cross-references ----
    print("--- Check 6: Cross-references ---")

    cursor.execute("SELECT COUNT(*) FROM cross_references")
    cross_ref_count = cursor.fetchone()[0]
    all_passed &= check(
        "Cross-reference count",
        cross_ref_count >= EXPECTED_CROSS_REFS_MIN,
        f"{cross_ref_count:,} entries (expected ~340,000)"
    )

    # Check a well-known cross-reference exists (Genesis 1:1 should have references)
    cursor.execute("SELECT COUNT(*) FROM cross_references WHERE from_verse LIKE '%Gen.1.1%' OR from_verse LIKE '%Gen 1:1%'")
    gen_refs = cursor.fetchone()[0]
    all_passed &= check(
        "Genesis 1:1 has cross-refs",
        gen_refs > 0,
        f"{gen_refs} cross-references found"
    )
    print()

    # ---- CHECK 7: Spot-check verse content ----
    print("--- Check 7: Spot-check verse content ---")

    # Check John 3:16 in KJV
    cursor.execute("""
        SELECT text FROM verses
        WHERE book_name='John' AND chapter=3 AND verse=16 AND translation='KJV'
    """)
    row = cursor.fetchone()
    if row:
        text = row[0]
        has_key_phrase = "God so loved" in text or "so loved the world" in text
        all_passed &= check("John 3:16 (KJV)", has_key_phrase, f"'{text[:80]}...'")
    else:
        all_passed &= check("John 3:16 (KJV)", False, "NOT FOUND")

    # Check Genesis 1:1 in KJV
    cursor.execute("""
        SELECT text FROM verses
        WHERE book_name='Genesis' AND chapter=1 AND verse=1 AND translation='KJV'
    """)
    row = cursor.fetchone()
    if row:
        text = row[0]
        has_key_phrase = "beginning" in text.lower() and "heaven" in text.lower()
        all_passed &= check("Genesis 1:1 (KJV)", has_key_phrase, f"'{text[:80]}...'")
    else:
        all_passed &= check("Genesis 1:1 (KJV)", False, "NOT FOUND")

    # Check Psalm 23:1 in KJV
    cursor.execute("""
        SELECT text FROM verses
        WHERE book_name='Psalms' AND chapter=23 AND verse=1 AND translation='KJV'
    """)
    row = cursor.fetchone()
    if row:
        text = row[0]
        has_key_phrase = "shepherd" in text.lower()
        all_passed &= check("Psalm 23:1 (KJV)", has_key_phrase, f"'{text[:80]}...'")
    else:
        all_passed &= check("Psalm 23:1 (KJV)", False, "NOT FOUND")

    # Check Romans 8:28 in ASV
    cursor.execute("""
        SELECT text FROM verses
        WHERE book_name='Romans' AND chapter=8 AND verse=28 AND translation='ASV'
    """)
    row = cursor.fetchone()
    if row:
        text = row[0]
        has_key_phrase = "work together" in text.lower() or "all things" in text.lower()
        all_passed &= check("Romans 8:28 (ASV)", has_key_phrase, f"'{text[:80]}...'")
    else:
        all_passed &= check("Romans 8:28 (ASV)", False, "NOT FOUND")

    # Check Revelation 22:21 exists (last verse of the Bible)
    cursor.execute("""
        SELECT text FROM verses
        WHERE book_name='Revelation' AND chapter=22 AND verse=21 AND translation='KJV'
    """)
    row = cursor.fetchone()
    if row:
        all_passed &= check("Revelation 22:21 (KJV)", True, "Last verse present")
    else:
        all_passed &= check("Revelation 22:21 (KJV)", False, "NOT FOUND - Bible may be incomplete")
    print()

    # ---- CHECK 8: Database size ----
    print("--- Check 8: Database size ---")
    db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    all_passed &= check(
        "Database size reasonable",
        0.5 < db_size_mb < 200,
        f"{db_size_mb:.1f} MB"
    )
    print()

    # ---- FINAL RESULT ----
    print("=" * 60)
    if all_passed:
        print("  ALL CHECKS PASSED - Database is ready!")
    else:
        print("  SOME CHECKS FAILED - Review the issues above.")
        print("  Failed checks may indicate missing data or format issues.")
        print("  Fix the issues and re-run sqlite_setup.py.")
    print("=" * 60)

    conn.close()
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
