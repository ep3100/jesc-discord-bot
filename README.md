# JESC Discord Bot

A Discord bot that searches through 2.6+ million real Japanese example sentences from movies and TV shows. Perfect for Japanese learners who want to see how words are used in natural, authentic contexts.

---

## Features

- Smart search for Japanese words and phrases
- Automatic conjugation matching
- Random sentence generation
- Bot and database statistics
- Filtered and cleaned subtitle corpus
- Fast searches using n-gram indexing

---

## Commands

| Command | Description | Example |
|---|---|---|
| `/sentence [word]` | Search for example sentences | `/sentence 食べる` |
| `/random` | Get a random sentence | `/random` |
| `/stats` | View bot statistics | `/stats` |
| `/help` | Show help information | `/help` |

---

## Dataset
Japanese-English Subtitle Corpus (JESC)
2.6+ million parallel sentence pairs
Source: movie and TV subtitle data
Natural Japanese usage examples

Dataset source:

https://nlp.stanford.edu/projects/jesc/

## Tech Stack and Resources
- Python 3.11+
- discord.py 2.0+
- PostgreSQL 14+
- fugashi + unidic-lite
- SQLAlchemy 2.0+
- jaconv
