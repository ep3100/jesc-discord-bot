from sqlalchemy import create_engine, String, Text, Index, select, func
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column, Session
from typing import List, Tuple
import logging

from config import config

logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    """base class for all models"""
    pass

class Sentence(Base):
    """table storing japanese-english sentence pairs"""
    __tablename__ = 'sentences'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    japanese: Mapped[str] = mapped_column(Text, nullable=False)
    english: Mapped[str] = mapped_column(Text, nullable=False)

    # indexing for faster searching
    __table_args__ = (
        Index('idx_japanese_txt', 'japanese')
    )

    def __repr__(self) -> str:
        return f"<Sentence(id={self.id}, ja='{self.japanese[:30]}...')>"
    

class WordIndex(Base):
    """maps words to their index in sentences for search"""
    __tablename__ = 'word_index'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    word: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    sentence_id: Mapped[int] = mapped_column(nullable=False, index=True)

    __table_args__ = (
        Index('idx_word_sentence', 'word', 'sentence_id')
    )

    def __repr__(self):
        return f"<WordIndex(word='{self.word}', sentence_id={self.sentence_id})>"
    
class Database:
    """database connection + query manager"""

    def __init__(self):
        self.engine = None
        self.Session = None

    def connect(self):
        """init database connection"""
        try:
            self.engine = create_engine(config.DATABASE_URL, echo=False)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def create_tables(self):
        """create all tables if they don't exists"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def drop_table(self):
        """drop all tables (be careful duh)"""
        Base.metadata.drop_all(self.engine)
        logger.warning("All tables dropped")
    
    def search_by_word(self, word: str, limit: int = 5) -> List[Tuple[str, str]]:
        """
        search for sentences containing a specific word
        
        args:
            word: jap word to search for
            limit:  max number of results to return

        returns:
            list of (jap, eng) sentene pairs
        """
        with Session(self.engine) as session:
            try:
                # get query with select()
                stmt = (
                    select(Sentence.japanese, Sentence.english)
                    .join(WordIndex, Sentence.id == WordIndex.sentence_id)
                    .where(WordIndex.word == word)
                    .limit(limit)
                )
                results = session.execute(stmt).all()
                return [(jp, en) for jp, en in results]
            except Exception as e:
                logger.erorr(f"Error searching for word '{word}': {e}")
                return []
            
    def search_by_partial_word(self, word: str, limit: int = 5) -> List[Tuple[str, str]]:
        """
        search for sentences containing words that start with the given string
        useful for related words, or when the exact match fails

        same args and return
        """
        with Session(self.engine) as session:
            try:
                # get query with select()
                stmt = (
                    select(Sentence.japanese, Sentence.english)
                    .join(WordIndex, Sentence.id == WordIndex.sentence_id)
                    .where(WordIndex.word.like(f"{word}%"))
                    .limit(limit)
                )
                results = session.execute(stmt).all()
                return [(jp, en) for jp, en in results]
            except Exception as e:
                logger.erorr(f"Error searching for word '{word}': {e}")
                return []

    def get_random_sentence(self) -> Tuple[str, str]:
        """
        get a random sentence from the database

        returns:
            a (jap, eng) sentence pair, or (None, None) if error
        """

        with Session(self.engine) as session:
            try:
                stmt = (
                    select(Sentence.japanese, Sentence.english)
                    .order_by(func.random())
                    .limit(1)
                )
                result = session.execute(stmt).first()

                if result:
                    return result
                return (None, None)
            except Exception as e:
                logger.error(f"Error getting random sentences: {e}")
                return (None, None)
            
    def get_sentence_count(self) -> int:
        """gets the total num of sentences in the database"""
        with Session(self.engine) as session:
            try:
                stmt = select(func.count()).select_from(Sentence)
                count = session.execute(stmt).scalar()
                return count if count else 0
            except Exception as e:
                logger.error(f"Error counting sentences: {e}")
                return 0
            
    def bulk_insert_sentences(self, sentences: List[Tuple[str, str]], batch_size: int = 10000):
        """
        attempt to efficiently insert many sentences at once

        args:
            sentences: list of (jap, eng) tuples
            batch_size: number of sentences to insert per step
        """

        with Session(self.engine) as session:
            try:
                for i in range (0, len(sentences), batch_size):
                    batch = sentences[i:i + batch_size]
                    sentence_objs = [
                        Sentence(japanese=jp, english=en)
                        for jp, en in batch
                    ]
                    session.bulk_save_objects(sentence_objs)
                    session.commit()
                    logger.info(f"Inserted sentences {i} to {i + len(batch)}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting sentences: {e}")
                raise

    def bulk_insert_word_index(self, word_entries: List[Tuple[str, int]], batch_size: int = 50000):
        """
        attempt to efficiently insert many word index entries at once

        args:
            word_entries: list of (word, sentence_id) tuples
            batch_size: number of entires to insert per transaction
        """
        with Session(self.engine) as session:
            try:
                for i in range(0, len(word_entries), batch_size):
                    batch = word_entries[i:i + batch_size]
                    index_objs = [
                        WordIndex(word=word, sentence_id=sid)
                        for word, sid in batch
                    ]
                    session.bulk_save_objects(index_objs)
                    session.commit()
                    logger.info(f"Inserted word index entries {i} to {i + len(batch)}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting word index: {e}")
                raise

db = Database()