"""
JESC Discord Bot
A Discord bot for seraching Japanese example sentences from the JESC corpus
"""
import discord
from discord import app_commands
from discord.ext import commands
import logging
import sys

from database import db
from tokenizer import tokenizer, normalize_text
from config import config

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JESCBot(commands.Bot):
    """Main bot class"""

    def __init__(self):
        # intents
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(command_prefix=config.COMMAND_PREFIX, intents=intents)

    async def setup_hook(self):
        logger.info("Setting up bot...")

        # try to connect to database
        try:
            db.connect()
            sentence_count = db.get_sentence_count()
            logger.info(f"Connected to database with {sentence_count:,}, sentences")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error("Make sure PostgreSQL is running and you've run setup_db.py")
            sys.exit(1)

        # sync slash commands
        await self.tree.sync()
        logger.info("Slash command synced")

    async def on_ready(self):
        """called when bot is fully ready"""
        logger.info(f"✅ Bot is ready! Logged in as {self.user} (ID: {self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} server(s)")

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="Japanese sentences | /sentence"
            )
        )

bot = JESCBot()

@bot.tree.command(name="sentence", description="Search for Japanese example sentences")
@app_commands.describe(
    word="Japanese word to search for(e.g., 食べる, 面白い)",
    limit="Number of examples to show (1-10, default: 5)"
)
async def sentence_command(
    interaction: discord.Interaction,
    word: str,
    limit: int = 5
):
    """
    Search for example sentences containing a Japanese word
    """
    await interaction.response.defer()
    limit = max(1, min(limit, 10))

    normalized_word = normalize_text(word)

    logger.info(f"Search request: '{word}' (normalized: '{normalized_word}') by {interaction.user}")
    
    # try exact match first
    results = db.search_by_word(normalized_word, limit=limit)

    # if no match, try partial
    if not results:
        logger.info(f"No exact match for '{normalized_word}', trying partial match...")
        results = db.search_by_partial_word(normalized_word, limit=limit)

    # if still no results, try getting lemma
    if not results:
        lemmas = tokenizer.get_lemmas(normalized_word)
        if lemmas:
            lemma = list(lemmas)[0]
            logger.info(f"Trying lemma '{lemma}'...")
            results = db.search_by_word(lemma, limit=limit)

    # create response embed
    if results:
        embed = discord.Embed(
            title=f"Example sentence for: {word}",
            description=f"Fuond {len(results)} example(s) from Japanese subtitles",
            color=discord.Color.blue()
        )

        for idx, (ja_text, en_text) in enumerate(results, 1):
            # truncate if too long
            ja_display = ja_text if len(ja_text) <= 200 else ja_text[:197] + "..."
            en_display = en_text if len(en_text) <= 200 else en_text[:197] + "..."

            embed.add_field(
                name=f"Example {idx}",
                value=f"jp: {ja_display}\n en: {en_display}",
                inline=False
            )

        embed.set_footer(text="Data from JESC (Japanese-English Subtitle Corpus)")

    else:
        embed = discord.Embed(
            title="No results found",
            description=f"Couldn't find any sentences containing **{word}**",
            color=discord.Color.red()
        )
        embed.add_field(
            name="💡 Tips",
            values=(
                "- Try the dictionary form (e.g. 食べる instead of 食べた)\n"
                "- Check for typos\n"
                "- Try a more common word or synonym\n"
                "- Make sure you're using Japanese characters"
            ),
            inline=False 
        )

    await interaction.followup.send(embed=embed)

@bot.tree.command(name="random", description="Get a random Japanese sentence")
async def random_command(interaction: discord.Interaction):
    """
    get a random sentence from the database
    """
    await interaction.response.defer()

    logger.info(f"Random sentence request by {interaction.user}")

    ja_text, en_text = db.get_random_sentence()

    if ja_text and en_text:
        embed = discord.Embed(
            title="Random Sentence",
            description="Here's a random sentence from the Japanese media:",
            color=discord.Color.green()
        )
        embed.add_field(
            name="🇯🇵 Japanese",
            value=ja_text,
            inline=False
        )
        embed.add_field(
            name="🇬🇧 Enligh",
            value=en_text,
            inline=False
        )
        embed.set_footer(text="Data from JESC (Japanese-English Subtitle Corpus)")
    else:
        embed = discord.Embed(
            title="Error",
            description="Failed to retrieve a random sentence",
            color=discord.Color.red()
        )

    await interaction.followup.send(embed=embed)

@bot.tree.command(name="stats", description="Show bot statistics")
async def stats_command(interaction: discord.Interaction):
    """
    display statistics about the bot and the database
    """
    await interaction.response.defer()

    sentence_count = db.get_sentence_count()

    embed = discord.Embed(
        title="Bot Statistics",
        color=discord.Color.purple()
    )
    embed.add_field(
        name="Total Sentences",
        value=f"{sentence_count:,}",
        inline=True
    )
    embed.add_field(
        name="Servers",
        value=f"{len(bot.guilds)}",
        inline=True
    )
    embed.add_field(
        name="Bot version",
        value="1.0.0",
        inline=True
    )
    embed.add_field(
        name="Data Source",
        value="[JESC Corpus](https://nlp.stanford.edu/projects/jesc/)",
        inline=False
    )
    embed.set_footer(text="Japanese-English Subtitle Corpus")

    await interaction.followup.send(embed=embed)

@bot.tree.command(name="help", description="Show help information")
async def help_command(interaction: discord.Interaction):
    """
    display help information about bot commands
    """
    embed = discord.Embed(
        title="❓ JESC Bot Help",
        description="Search for natural Japanese example sentences from movies and TV shows!",
        color=discord.Color.gold()
    )
    
    embed.add_field(
        name="📝 /sentence [word]",
        value="Search for example sentences containing a Japanese word\nExample: `/sentence 食べる`",
        inline=False
    )
    embed.add_field(
        name="🎲 /random",
        value="Get a random Japanese sentence with translation",
        inline=False
    )
    embed.add_field(
        name="📊 /stats",
        value="View bot statistics and information",
        inline=False
    )
    embed.add_field(
        name="❓ /help",
        value="Show this help message",
        inline=False
    )
    
    embed.add_field(
        name="💡 Search Tips",
        value=(
            "• Use dictionary form of words (食べる not 食べた)\n"
            "• Bot understands various conjugations\n"
            "• All examples are from real Japanese media\n"
            "• Sentences are from the JESC corpus (2.8M+ examples)"
        ),
        inline=False
    )
    
    embed.set_footer(text="Data from JESC (Japanese-English Subtitle Corpus)")
    
    await interaction.response.send_message(embed=embed)


@bot.event
async def on_command_error(ctx, error):
    """Handle command errors"""
    if isinstance(error, commands.CommandNotFound):
        return
    logger.error(f"Command error: {error}")


def main():
    """Run the bot"""
    try:
        # Validate configuration
        config.validate()
        logger.info("Starting JESC Discord Bot...")
        
        # Run the bot
        bot.run(config.DISCORD_BOT_TOKEN)
        
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        logger.error("Please check your .env file")
        sys.exit(1)
    except discord.LoginFailure:
        logger.error("❌ Failed to login - invalid bot token")
        logger.error("Please check your DISCORD_BOT_TOKEN in .env file")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Failed to start bot: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    