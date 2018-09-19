"""
A cog that handles posting a large embed to block out spoils.
"""
import discord
import asyncio
from datetime import datetime, timedelta
from discord.ext import commands
from .utils import checks


class Spoils():
    """
    Class that creates a task to run every minute and check for
    time since last post
    """
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.wait_time = bot.wait_time
        # create the background task and run it in the background
        try:
            self.bg_task = self.bot.loop.create_task(self.my_background_task())
        except Exception as e:
            self.bot.logger.warning(f"Error starting task {e}")

    @commands.command(aliases=['tenfeettaller'])
    @checks.has_permissions(manage_roles=True)
    async def wall(self, ctx, *, reason=None):
        local_embed = self.create_wall_embed(reason)
        try:
            await ctx.send(embed=local_embed)
            await ctx.message.delete()
        except Exception as e:
            self.bot.logger.warning(f'Issue building wall: {e}')

    async def my_background_task(self):
        local_embed = self.create_wall_embed()
        await self.bot.wait_until_ready()
        self.bot.logger.info("Starting spoiler task")
        while True:
            if self.bot.is_closed():
                await asyncio.sleep(60)
                self.bot.logger.warning("Socket is closed")
            else:
                for channel_id, wait_time in self.bot.spoiler_channels.items():
                    try:
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            async for message in channel.history(limit=1):
                                if not message.author.bot:
                                    last_post = datetime.utcnow() - message.created_at
                                    if last_post > timedelta(minutes=wait_time):
                                        try:
                                            await channel.send(embed=local_embed)
                                        except Exception as e:
                                            self.bot.logger.warning(
                                                f'Error posting to channel'
                                                f' {channel_id}: {e}')
                    except Exception as e:
                        self.bot.logger.warning(
                            f'Error getting channel {channel_id}: {e}')
            await asyncio.sleep(60)

    @commands.group(aliases=['spoilers'])
    @commands.guild_only()
    @checks.has_permissions(manage_messages=True)
    async def spoils(self, ctx):
        """
        Adds or removes a channel to spoils list
        """
        if ctx.invoked_subcommand is None:
            return

    @spoils.command(aliases=['add'], name='add_channel')
    async def _add_channel(self, ctx, minutes=60):
        """
        Adds channel to spoils list
        """
        added_channels = []
        desc = ''
        try:
            success = await \
                self.bot.postgres_controller.add_spoiler_channel(
                    self.bot.logger,
                    ctx.guild.id,
                    ctx.message.channel.id,
                    minutes
                )
            if success:
                added_channels.append(ctx.message.channel.name)
            if added_channels:
                for channel in added_channels:
                    desc += f'{channel} timeout: {minutes} minutes.\n'
                local_embed = discord.Embed(
                    title=f'Channel added to spoils list:',
                    description=desc,
                    color=0x419400
                )
                self.bot.spoiler_channels[ctx.message.channel.id] = minutes
            else:
                local_embed = discord.Embed(
                    title=f'Something went terribly bad',
                    description='oh no',
                    color=0xFF0752
                )
            await ctx.send(embed=local_embed, delete_after=2)
        except Exception as e:
            self.bot.logger.info(f'Error adding channels {e}')
            local_embed = discord.Embed(
                    title=f'Something went terribly bad',
                    description='oh no',
                    color=0xFF0752
                )
            await ctx.message.delete()
            await ctx.send(embed=local_embed, delete_after=2)

    @spoils.command(aliases=['rem', 'remove', 'end'], name='rem_channel')
    async def __remove_channel(self, ctx):
        """
        Removes a channel from the spoils list
        """
        removed_channels = []
        absent_channels = []
        desc = ''
        try:
            try:
                success = False
                success = await \
                    self.bot.postgres_controller.rem_spoiler_channel(
                        self.bot.logger, ctx.guild.id, ctx.message.channel.id, 
                    )
            except ValueError:
                absent_channels.append(ctx.message.channel.name)
            if success:
                removed_channels.append(ctx.message.channel.name)
            if removed_channels:
                for channel in removed_channels:
                    desc += f'{channel} \n'
                local_embed = discord.Embed(
                    title=f'Channels removed from spoils list:',
                    description=desc,
                    color=0x419400
                )
                spoiler_channels_n = \
                    await self.bot.postgres_controller.get_spoiler_channels(self.bot.logger)
                self.bot.spoiler_channels = spoiler_channels_n
            elif absent_channels:
                desc = ''
                for channel in absent_channels:
                    desc += f'{channel}\n'
                local_embed = discord.Embed(
                    title=f'Channels not in spoils list: ',
                    description=desc,
                    color=0x651111
                )
            else:
                local_embed = discord.Embed(
                    title=f'Something went terribly bad',
                    description='oh no',
                    color=0xFF0752
                )
            await ctx.send(embed=local_embed, delete_after=2)
        except Exception as e:
            self.bot.logger.warning(f'Issue: {e}')
            local_embed = discord.Embed(
                    title=f'Something went terribly bad',
                    description='oh no',
                    color=0xFF0752
                )
            await ctx.message.delete()
            await ctx.send(embed=local_embed, delete_after=2)

    def create_wall_embed(self, reason=None):
        local_embed = discord.Embed(title='Clear Spoilers', type='rich')
        temp_description = '\_\_\_'
        for i in range(60):
            temp_description += ' \n'
        if reason:
            temp_description += f'\_\_\_\nSpoilers for {reason} above'
        else:
            temp_description += '\_\_\_\nSpoilers above'
        local_embed.description = temp_description
        return local_embed
