"""
JESC Corpus Loader
"""

import sys
import logging
import csv
from pathlib import Path
from tqdm import tqdm
from typing import List, Tuple

from src.database import db
from src.tokenizer import get_lemmas_with_surface
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_jesc_file(csv_path: str, limit: int = None) -> List[Tuple[str, str]]:
    """
    read parallel JESC files and return sentence pairs

    args:
        ja_path: path to japanese sentence file
        en_path: path to english sentence file
        limit: optional limit on number of sentences to read (testing)

    returns:
        list of (jap, eng) sentence pairs
    """
    logger.info(f"Reading JESC CSV file...")
    logger.info(f"   FIle: {csv_path}")

    sentences = []

    try:
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)

            for line_num, row in enumerate(reader, 1):
                if limit and line_num > limit:
                    break

                if len(row) < 3:
                    continue

                # get eng and jap from cols
                en_text = row[1].strip()
                ja_text = row[2].strip()

                # skip empty lines
                if not ja_text or not en_text:
                    continue

                # skip rows with newlines or tabs
                if '\n' in ja_text or '\t' in ja_text or '\n' in en_text or '\t' in en_text:
                    continue   

                # skip overly long sentences (likely corrupt)
                if len(ja_text) > 300 or len(en_text) > 300:
                    continue 

                sentences.append((ja_text, en_text))

                # progress update every 100k lines
                if line_num % 100000 == 0:
                    logger.info(f"   Read {line_num:,} lines...")

        logger.info(f"Read {len(sentences):,} sentence pairs")
        return sentences
    except FileNotFoundError as e:
        logger.error(f" File not found {e}")
        logger.error(" Please download JESC corupus and place train.csv in data/ directory")
        raise
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise

def build_word_index(sentences: List[Tuple[str, str]], start_id: int = 1) -> List[Tuple[str, int]]:
    """
    build word index for all sentences
    tokenizes each japanese sentence and creates word->sentence_id mappings

    args:
        sentences: list of  (jap, eng) sentence pairs
        start_id: starting sentence ID in database

    returns:
        list of (word, sentence_id) tuples for the word_index table
    """
    logger.info("Building word index (this may take a moment)...")
    word_index = []

    for idx, (ja_text, _) in enumerate(tqdm(sentences, desc="Tokenizing"), start=start_id):
        # all words (lemmas + surface forms) from sentence
        words = get_lemmas_with_surface(ja_text)

        # create index entries for each word
        for word in words:
            word_index.append((word, idx))

    logger.info(f"Created {len(word_index):,} word index entries")
    return word_index

def load_jesc_to_database(limit: int = None):
    """
    main function to load JESC corupus into the database

    args:
        limit: optional limit on nmber of sentences for testing
    """
    try:
        config.validate()

        logger.info("Connecting to database...")
        db.connect()

        existing_count = db.get_sentence_count()
        if existing_count > 0:
            logger.warning(f"Database already contains {existing_count:,} sentences")
            response = input("Do you want to continue and add more (y/n): ")
            if response.lower() != 'y':
                logger.info("Cancelled by user")
                return
            
        # read JESC file
        sentences = read_jesc_file(
            config.JESC_CSV_PATH,
            limit=limit
        )

        if not sentences:
            logger.error("No sentences to load")
            return

        # Insert sentences
        logger.info("Inserting sentences into database...")
        start_id = existing_count + 1
        db.bulk_insert_sentences(sentences)
        
        # Build and insert word index
        word_index = build_word_index(sentences, start_id=start_id)
        logger.info("Inserting word index into database...")
        db.bulk_insert_word_index(word_index)
        
        # Final statistics
        total_sentences = db.get_sentence_count()
        logger.info("=" * 50)
        logger.info("✅ JESC corpus loaded successfully!")
        logger.info(f"  Total sentences in database: {total_sentences:,}")
        logger.info(f"  Total word index entries: {len(word_index):,}")
        logger.info("=" * 50)
        logger.info("You can now run the bot with: python -m src.bot")
        
    except Exception as e:
        logger.error(f"❌ Failed to load JESC corpus: {e}")
        raise

def main():
    """entry point for loader"""
    import argparse

    parser = argparse.ArgumentParser(description='Load JESC corpus into database')
    parser.add_argument('--limit', type=int, help='Limit number of sentences (for testing)')
    args = parser.parse_args()

    if args.limit:
        logger.info(f"⚠️  Running in test mode with limit={args.limit}")

    load_jesc_to_database(limit=args.limit)


if __name__ == "__main__":
    main()