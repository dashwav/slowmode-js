"""
Actually runs the code
"""
from asyncio import get_event_loop
from bot import Nanochan
from cogs import Spoils, Filter, Janitor, Stats, Owner, Reactions


def run():
    loop = get_event_loop()
    bot = loop.run_until_complete(Nanochan.get_instance())
    cogs = [
      Owner(bot),
      Spoils(bot),
      Filter(bot),
      Reactions(bot),
      Stats(bot),
    ]
    bot.start_bot(cogs)


if __name__ == '__main__':
    run()
