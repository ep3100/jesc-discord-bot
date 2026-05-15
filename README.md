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

## Tech Stack
![image]({BadgeURLHere})
![image]({https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue})
![image]({https://img.shields.io/badge/Discord-5865F2?style=for-the-badge&logo=discord&logoColor=white})
![image]({https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white})
- fugashi + unidic-lite
- SQLAlchemy 2.0+
- jaconv
