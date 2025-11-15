"""
Rebuild word index from existing sentences
"""
import sys
import logging
from sqlalchemy import text
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, '.')

from src.database import db
from src.tokenizer import get_lemmas_with_surface
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def rebuild_word_index():
    """Rebuild word index from existing sentences in database"""
    try:
        # Connect to database
        logger.info("Connecting to database...")
        db.connect()

        # debug
        session = db.Session()
        result = session.execute(text("SHOW client_encoding")).scalar()
        logger.info(f"🔍 Database client encoding: {result}")
        session.close()
        
        # Get total sentence count
        total_sentences = db.get_sentence_count()
        logger.info(f"Found {total_sentences:,} sentences in database")
        
        # Fetch all sentences
        logger.info("Fetching sentences from database...")
        from sqlalchemy import select
        from src.database import Sentence
        
        session = db.Session()
        stmt = select(Sentence.id, Sentence.japanese)
        results = session.execute(stmt).all()
        session.close()
        
        logger.info(f"Retrieved {len(results):,} sentences")
        
        # Build word index
        logger.info("Building word index...")
        word_index = []
        
        for sentence_id, ja_text in tqdm(results, desc="Tokenizing"):
            words = get_lemmas_with_surface(ja_text)
            for word in words:
                word_index.append((word, sentence_id))
        
        logger.info(f"✅ Created {len(word_index):,} word index entries")
        
        # debugging:
        logger.info("Sample word index entries:")
        for i in range(min(10, len(word_index))):
            word, sid = word_index[i]
            logger.info(f"  Word: '{word}' (bytes: {word.encode('utf-8')[:20]}...), Sentence ID: {sid}")


        # Insert word index
        logger.info("Inserting word index into database...")
        db.bulk_insert_word_index(word_index)
        
        logger.info("=" * 50)
        logger.info("✅ Word index rebuilt successfully!")
        logger.info(f"  Total word index entries: {len(word_index):,}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Failed to rebuild word index: {e}")
        raise


if __name__ == "__main__":
    rebuild_word_index()