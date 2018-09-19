"""
Database utility functions.
"""
from datetime import datetime, timedelta
from typing import Optional
import random
try:
    from asyncpg import Record, InterfaceError, UniqueViolationError, create_pool
    from asyncpg.pool import Pool
except ImportError:
    Record = None
    Pool = None
    print('asyncpg not installed, PostgresSQL function not available.')


def parse_record(record: Record) -> Optional[tuple]:
    """
    Parse a asyncpg Record object to a tuple of values
    :param record: the asyncpg Record object
    :return: the tuple of values if it's not None, else None
    """
    try:
        return tuple(record.values())
    except AttributeError:
        return None


async def make_tables(pool: Pool, schema: str):
    """
    Make tables used for caching if they don't exist.
    :param pool: the connection pool.
    :param schema: the schema name.
    """
    await pool.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(schema))

    reacts = """
    CREATE TABLE IF NOT EXISTS {}.reacts (
        id SERIAL,
        serverid BIGINT,
        trigger TEXT UNIQUE,
        reaction TEXT,
        created_at TIMESTAMP DEFAULT current_timestamp,
        PRIMARY KEY (id, serverid, trigger)
    );
    """.format(schema)

    servers = """
    CREATE TABLE IF NOT EXISTS {}.servers (
      serverid BIGINT,
      assignableroles varchar ARRAY,
      filterwordswhite varchar ARRAY,
      filterwordsblack varchar ARRAY,
      blacklistchannels integer ARRAY,
      r9kchannels integer ARRAY,
      addtime TIMESTAMP DEFAULT current_timestamp,
      PRIMARY KEY (serverid)
    );""".format(schema)

    filters = """
    CREATE TABLE IF NOT EXISTS {}.filters (
      serverid BIGINT,
      channelid BIGINT,
      whitelist varchar ARRAY,
      PRIMARY KEY (channelid)
    );
    """.format(schema)

    spoils = """
    CREATE TABLE IF NOT EXISTS {}.spoils (
      serverid BIGINT,
      channelid BIGINT,
      interval INT,
      PRIMARY KEY (channelid)
    );
    """.format(schema)

    await pool.execute(reacts)
    await pool.execute(servers)
    await pool.execute(filters)
    await pool.execute(spoils)


class PostgresController():
    """
    We will use the schema 'nanochan' for the db
    """
    __slots__ = ('pool', 'schema', 'logger')

    def __init__(self, pool: Pool, logger, schema: str = 'nanochan'):
        self.pool = pool
        self.schema = schema
        self.logger = logger

    @classmethod
    async def get_instance(cls, logger=None, connect_kwargs: dict = None,
                           pool: Pool = None, schema: str = 'nanochan'):
        """
        Get a new instance of `PostgresController`
        This method will create the appropriate tables needed.
        :param logger: the logger object.
        :param connect_kwargs:
            Keyword arguments for the
            :func:`asyncpg.connection.connect` function.
        :param pool: an existing connection pool.
        One of `pool` or `connect_kwargs` must not be None.
        :param schema: the schema name used. Defaults to `minoshiro`
        :return: a new instance of `PostgresController`
        """
        assert logger, (
            'Please provide a logger to the data_controller'
        )
        assert connect_kwargs or pool, (
            'Please either provide a connection pool or '
            'a dict of connection data for creating a new '
            'connection pool.'
        )
        if not pool:
            try:
                pool = await create_pool(**connect_kwargs)
                logger.info('Connection pool made.')
            except InterfaceError as e:
                logger.error(str(e))
                raise e
        logger.info('Creating tables...')
        await make_tables(pool, schema)
        logger.info('Tables created.')
        return cls(pool, logger, schema)


    async def add_server(self, server_id: int):
        """
        Inserts into the server table a new server
        :param server_id: the id of the server added
        """
        sql = """
        INSERT INTO {}.servers VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (serverid)
        DO nothing;
        """.format(self.schema)

        await self.pool.execute(sql, server_id, [], [], [], [], [])

    """
    Custom Reactions below
    """

    async def get_all_triggers(self):
        """
        Returns list of triggers
        """
        sql = """
        SELECT trigger FROM {}.reacts;
        """.format(self.schema)
        trigger_list = []
        records = await self.pool.fetch(sql)
        for rec in records:
            trigger_list.append(rec['trigger'])
        return trigger_list

    async def rem_reaction(self, trigger):
        """
        REmoves a value from the reacts DB
        """
        sql = """
        DELETE FROM {}.reacts WHERE trigger = $1;
        """.format(self.schema)

        await self.pool.execute(sql, trigger)

    async def add_reaction(self, server_id, trigger, reaction):
        """
        sets or updates a reaction
        """
        sql = """
        INSERT INTO {}.reacts (serverid, trigger, reaction) VALUES ($1, $2, $3)
        ON CONFLICT (trigger)
        DO UPDATE SET
        reaction = $3 WHERE {}.reacts.trigger = $4 AND {}.reacts.serverid = $5;
        """.format(self.schema, self.schema, self.schema)

        await self.pool.execute(sql, server_id, trigger, reaction, reaction, trigger, server_id)

    async def get_reaction(self, trigger):
        """
        returns a reaction TEXT
        """
        sql = """
        SELECT reaction FROM {}.reacts
        WHERE trigger = $1;
        """.format(self.schema)
        return await self.pool.fetchval(sql, trigger)

    """
    Filter Stuff
    """

    async def add_filter_channel(self, logger, server_id, channel_id, whitelist):
        """
        TODO
        Adds a channel to the filter list
        """
        return

    async def rem_filter_channel(self, logger, server_id, channel_id):
        """
        TODO
        Removes a channel from filter list
        """
        return
    
    async def add_filter_word(self, logger, channel_id, word):
        """
        TODO
        Adds a word to the filter list
        """
        return
    
    async def get_filter_channels(self, logger):
        """
        TODO
        Returns list of channel filters
        """
        return
    
    """
    Spoiler Stuff
    """

    async def add_spoiler_channel(self, logger, server_id, channel_id, interval):
        """
        Adds a channel to the spoiler list
        """
        sql = """
        INSERT INTO {}.spoils VALUES ($1, $2, $3);
        """.format(self.schema)
        try:
            await self.pool.execute(sql, server_id, channel_id, interval)
            return True
        except Exception as e:
            logger.warning(f'Error adding spoiler channel to database: {e}')
            return False

    async def rem_spoiler_channel(self, logger, server_id, channel_id):
        """
        Removes a channel from the spoiler db
        """
        sql = """
        DELETE FROM {}.spoils
        WHERE serverid = $1 AND channelid = $2;
        """.format(self.schema)
        try:
            await self.pool.execute(sql, server_id, channel_id)
            return True
        except Exception as e:
            logger.warning(f'Error removing spoiler channel to database: {e}')
            return False

    async def get_spoiler_channels(self, logger):
        """
        Returns all spoiler channels
        """
        sql = """
        SELECT * FROM {}.spoils;
        """.format(self.schema)
        try:
            ret_channels = {}
            channels = await self.pool.fetch(sql)
            for channel in channels:
                ret_channels[channel['channelid']] = channel['interval']
            return ret_channels
        except Exception as e:
            logger.warning(f'Error retrieving spoiler channels {e}')
            return False
