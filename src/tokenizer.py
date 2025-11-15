"""
tokenization and processing for japanese text
"""

import fugashi
import jaconv
import logging
from typing import List, Set

logger = logging.getLogger(__name__)

class JapaneseTokenizer:

    def __init__(self):
        try:
            # init fugashi with unidic-lite
            self.tagger = fugashi.Tagger()
            logger.info("Japanese tokenizezr initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize tokenizer: {e}")
            raise

    def normalize(self, text: str) -> str:
        """
        normalize japanese text
        - convert full-width alphanumeric to half-width
        - convert half-width katakana to full-width

        args;
            text: input japanese text

        returns:
            normalized text
        """

        text = jaconv.z2h(text, kana=False, ascii=True, digit=True)
        text = jaconv.h2z(text, kana=True, ascii=False, digit=False)
        return text
    
    def tokenize(self, text:str) -> List[str]:
        """
        tokenize japanese text and return surface forms

        args:
            text: input japanese text

        returns: 
            list of surface form tokens
        """
        text = self.normalize(text)
        try:
            tokens = [word.surface for word in self.tagger(text)]
            return tokens
        except Exception as e:
            logger.error(f"Tokenization error: {e}")
            return []
        
    def get_lemmas(self, text: str) -> Set[str]:
        """
        extract unique dicionary forms form text
        used for indexing and searching

        args:
            text: input japanese text

        returns:
            set of unique forms
        """

        text = self.normalize(text)
        lemmas = set()

        try:
            for word in self.tagger(text):
                lemma = word.feature.lemma

                if lemma and lemma != '*':
                    lemmas.add(lemma)
                else:
                    lemmas.add(word.surface)
            
            return lemmas
        except Exception as e:
            logger.error(f"Lemma extraction error: {e}")
            return set()
        
    def get_lemmas_with_surface(self, text: str) -> Set[str]:
        """
        extract both lemmas and surface forms for better matching
        helps with conjugated forms users may search for

        args: 
            text: input japanese text

        returns:
            set of unique words (both lemmas and surface words)
        """

        text = self.normalize(text)
        words = set()

        try:
            for word in self.tagger(text):
                words.add(word.surface)
                lemma = word.feature.lemma

                if lemma and lemma != '*':
                    words.add(lemma)

            return words
        except Exception as e:
            logger.error(f"Word extraction error: {e}")
            return set()
        
    def analyze_word(self, word: str) -> dict:
        """
        get detailed structural info about a word
        useful for debugging or future features

        args:
            word: japanese word to analyze

        returns:
            dictinoary with info
        """

        try:
            result = self.tagger(word)
            if result:
                first_word = result[0]
                return {
                    'surface': first_word.surface,
                    'lemma': first_word.feature.lemma if first_word.feature.lemma != '*' else None,
                    'pos': first_word.feature.pos1,
                    'pos_detail': first_word.feature.pos2,
                    'reading': first_word.feature.kana if hasattr(first_word.feature, 'kana') else None
                }
        except Exception as e:
            logger.error(f"Word analysis error: {e}")

        return {}
    
# singleton instance
tokenizer = JapaneseTokenizer()

# convenience functions for use
def get_lemmas(text: str) -> Set[str]:
    return tokenizer.get_lemmas(text)

def get_lemmas_with_surface(text: str) -> Set[str]:
    return tokenizer.get_lemmas_with_surface(text)

def normalize_text(text: str) -> str:
    return tokenizer.normalize(text)