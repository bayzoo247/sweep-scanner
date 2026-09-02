#!/usr/bin/env python3
"""
Sweep & Reclaim Scanner — Discord Bot

Scans top 100 Binance USDT pairs for liquidity sweep-and-reclaim setups.
Posts alerts to a Discord channel with the reclaim price after candle close.

Setup:
  1. pip install discord.py aiohttp
  2. Create a Discord bot at https://discord.com/developers/applications
  3. Set your bot token and channel ID below (or use environment variables)
  4. python bot.py
"""

import os
import time
import asyncio
import aiohttp
import discord
from discord.ext import commands, tasks
from datetime import datetime

# =====================================================================
#  CONFIG — Set these or use environment variables
# =====================================================================
BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHANNEL_ID = int(os.environ.get("DISCORD_CHANNEL_ID", "1519274070439493642"))

SCAN_INTERVAL_SECONDS = 60
TIMEFRAME = "1h"
SWING_LOOKBACK = 20
KLINE_LIMIT = 50  # candles to fetch (lookback + buffer)
SR_TOLERANCE = 0.008  # 0.8% — swept level must be within this % of daily S/R to fire
SR_REFRESH_HOURS = 4  # refresh daily S/R levels every N hours

STABLECOIN_FILTER = {
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "BUSDUSDT", "DAIUSDT",
    "USD1USDT", "RLUSDUSDT", "EURUSDT", "GBPUSDT", "TRYUSDT",
}

BINANCE_BASE = "https://api.binance.com"

# =====================================================================
#  STATE
# =====================================================================
last_alert_key: dict[str, str] = {}  # symbol -> last alert key for dedup
daily_sr: dict[str, dict] = {}      # symbol -> {"supports": [...], "resistances": [...]}
sr_loaded_at: float = 0             # timestamp of last daily S/R refresh
watchlist: list[str] = []
scanning = False
session: aiohttp.ClientSession | None = None


# =====================================================================
#  BINANCE API
# =====================================================================
async def fetch_json(url: str) -> dict | list:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_klines(symbol: str, interval: str, limit: int = 100) -> list:
    url = f"{BINANCE_BASE}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    return await fetch_json(url)


async def fetch_top100() -> list[str]:
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"
    data = await fetch_json(url)
    usdt = [t for t in data
            if t["symbol"].endswith("USDT")
            and t["symbol"] not in STABLECOIN_FILTER]
    usdt.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)
    return [t["symbol"] for t in usdt[:100]]


# =====================================================================
#  SWEEP & RECLAIM DETECTION
# =====================================================================
def find_swing_levels(klines: list, swing_strength: int = 3):
    """Find swing highs/lows — these are the liquidity levels."""
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    swing_highs = []
    swing_lows = []

    for i in range(swing_strength, len(klines) - swing_strength):
        is_high = all(highs[i] > highs[i - j] and highs[i] > highs[i + j]
                      for j in range(1, swing_strength + 1))
        is_low = all(lows[i] < lows[i - j] and lows[i] < lows[i + j]
                     for j in range(1, swing_strength + 1))
        if is_high:
            swing_highs.append(highs[i])
        if is_low:
            swing_lows.append(lows[i])

    return swing_highs, swing_lows


def detect_sweep_reclaim(klines: list) -> dict | None:
    """
    Check the last CLOSED candle for a sweep-and-reclaim.

    Bullish: wick below swing low + close above it
    Bearish: wick above swing high + close below it
    """
    if len(klines) < SWING_LOOKBACK + 5:
        return None

    # Last closed candle (second to last — last one is still open)
    candle = klines[-2]
    c_high = float(candle[2])
    c_low = float(candle[3])
    c_close = float(candle[4])
    c_vol = float(candle[5])
    c_range = c_high - c_low if c_high != c_low else 0.0001

    # 20-period average volume (excluding the open candle)
    vol_candles = klines[max(0, len(klines) - 22):-2]
    avg_vol = sum(float(k[5]) for k in vol_candles) / max(len(vol_candles), 1)
    vol_ratio = c_vol / avg_vol if avg_vol > 0 else 0

    # Swing levels from candles BEFORE the one we're checking
    historical = klines[:-2]
    swing_highs, swing_lows = find_swing_levels(historical)

    # Check last 8 swing levels (most recent = most relevant)
    recent_lows = swing_lows[-8:]
    recent_highs = swing_highs[-8:]

    # --- Bullish: sweep below swing low, close above ---
    for level in recent_lows:
        if c_low < level and c_close > level:
            body_ratio = (c_close - c_low) / c_range
            # Volume must be at or above average
            if body_ratio > 0.4 and vol_ratio >= 1.0:
                return {
                    "direction": "BULLISH",
                    "level": level,
                    "swept": c_low,
                    "close": c_close,
                    "volume": c_vol,
                    "avg_volume": avg_vol,
                    "vol_ratio": vol_ratio,
                }

    # --- Bearish: sweep above swing high, close below ---
    for level in recent_highs:
        if c_high > level and c_close < level:
            body_ratio = (c_high - c_close) / c_range
            if body_ratio > 0.4 and vol_ratio >= 1.0:
                return {
                    "direction": "BEARISH",
                    "level": level,
                    "swept": c_high,
                    "close": c_close,
                    "volume": c_vol,
                    "avg_volume": avg_vol,
                    "vol_ratio": vol_ratio,
                }

    return None


# =====================================================================
#  DAILY S/R LEVELS — Only fire alerts near proven daily zones
# =====================================================================
async def fetch_daily_sr(symbol: str) -> dict | None:
    """Fetch 90 daily candles, find swing highs (resistance) and lows (support)."""
    try:
        klines = await fetch_klines(symbol, "1d", 90)
        if len(klines) < 15:
            return None
        swing_highs, swing_lows = find_swing_levels(klines, swing_strength=2)
        return {
            "supports": swing_lows,
            "resistances": swing_highs,
        }
    except Exception:
        return None


def near_daily_sr(level: float, sr: dict | None) -> dict | None:
    """Check if a swept level is within SR_TOLERANCE of any daily S/R zone."""
    if not sr:
        return None
    for s in sr["supports"]:
        if abs(level - s) / s <= SR_TOLERANCE:
            return {"type": "Daily Support", "daily_level": s}
    for r in sr["resistances"]:
        if abs(level - r) / r <= SR_TOLERANCE:
            return {"type": "Daily Resistance", "daily_level": r}
    return None


async def load_all_daily_sr(symbols: list[str], channel=None):
    """Load daily S/R for all symbols. Called on startup and every SR_REFRESH_HOURS."""
    global sr_loaded_at
    loaded = 0
    for symbol in symbols:
        sr = await fetch_daily_sr(symbol)
        if sr:
            daily_sr[symbol] = sr
            loaded += 1
        await asyncio.sleep(0.1)
    sr_loaded_at = time.time()
    print(f"Loaded daily S/R for {loaded}/{len(symbols)} coins")
    if channel:
        await channel.send(f"Loaded daily S/R for **{loaded}** coins (refreshes every {SR_REFRESH_HOURS}h)")


# =====================================================================
#  FORMAT HELPERS
# =====================================================================
def fmt_price(n: float) -> str:
    if n >= 1000:
        return f"{n:,.2f}"
    if n >= 1:
        return f"{n:.4f}"
    if n >= 0.001:
        return f"{n:.6f}"
    return f"{n:.4g}"


def fmt_vol(v: float) -> str:
    if v >= 1e9:
        return f"{v / 1e9:.1f}B"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:.0f}"


def build_embed(symbol: str, result: dict) -> discord.Embed:
    """Build a Discord embed for a sweep & reclaim alert."""
    is_bull = result["direction"] == "BULLISH"
    color = 0x3fb950 if is_bull else 0xf85149
    emoji = "\U0001f7e2" if is_bull else "\U0001f534"
    arrow = "▲" if is_bull else "▼"

    level = result["level"]
    reclaim_pct = abs(result["close"] - level) / level * 100

    embed = discord.Embed(
        title=f"{emoji} {symbol} — {result['direction']} Sweep & Reclaim",
        color=color,
        timestamp=datetime.utcnow(),
    )
    embed.add_field(
        name=f"{arrow} Liquidity Level",
        value=f"**{fmt_price(level)}**",
        inline=True,
    )
    embed.add_field(
        name="Swept To",
        value=fmt_price(result["swept"]),
        inline=True,
    )
    embed.add_field(
        name="​",  # spacer
        value="​",
        inline=True,
    )
    embed.add_field(
        name="Reclaim Price (Candle Close)",
        value=f"**{fmt_price(result['close'])}** ({reclaim_pct:.2f}% {'above' if is_bull else 'below'} level)",
        inline=False,
    )
    # Show which daily S/R zone it matched
    if result.get("daily_type"):
        embed.add_field(
            name=f"Proven {result['daily_type']}",
            value=f"**{fmt_price(result['daily_level'])}**",
            inline=True,
        )
    # Volume confirmation
    if result.get("vol_ratio"):
        vol_emoji = "🔥" if result["vol_ratio"] >= 1.5 else "📊"
        embed.add_field(
            name=f"{vol_emoji} Volume",
            value=f"**{result['vol_ratio']:.1f}x** avg ({fmt_vol(result['volume'])} vs {fmt_vol(result['avg_volume'])})",
            inline=True,
        )
    embed.add_field(
        name="Timeframe",
        value=TIMEFRAME,
        inline=True,
    )
    embed.set_footer(text="Sweep & Reclaim Scanner • Daily S/R + Volume Filtered")
    return embed


# =====================================================================
#  BOT
# =====================================================================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    global session, watchlist, scanning
    session = aiohttp.ClientSession()
    print(f"Bot connected as {bot.user}")

    # Auto-load top 100 and start scanning
    try:
        watchlist = await fetch_top100()
        print(f"Loaded {len(watchlist)} coins")
    except Exception as e:
        print(f"Failed to load top 100: {e}")
        watchlist = [
            "BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "BNBUSDT",
            "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
        ]

    if CHANNEL_ID and CHANNEL_ID != 0:
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            # Pre-load daily S/R levels for all watched coins
            await load_all_daily_sr(watchlist, channel)
            await channel.send(
                f"**Sweep & Reclaim Scanner started** (Daily S/R Filtered)\n"
                f"Watching **{len(watchlist)}** coins on **{TIMEFRAME}** — "
                f"scanning every **{SCAN_INTERVAL_SECONDS}s**\n"
                f"Only alerting sweeps near proven daily support/resistance"
            )
        scan_loop.start()
    else:
        print("WARNING: DISCORD_CHANNEL_ID not set! Use !setchannel in your server.")


@tasks.loop(seconds=SCAN_INTERVAL_SECONDS)
async def scan_loop():
    """Main scan loop — runs every SCAN_INTERVAL_SECONDS."""
    if not watchlist:
        return

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return

    # Refresh daily S/R levels periodically
    if time.time() - sr_loaded_at > SR_REFRESH_HOURS * 3600:
        await load_all_daily_sr(watchlist, channel)

    found = 0
    for symbol in watchlist:
        try:
            klines = await fetch_klines(symbol, TIMEFRAME, KLINE_LIMIT)
            result = detect_sweep_reclaim(klines)

            if result:
                # --- Daily S/R filter ---
                sr = daily_sr.get(symbol)
                if not sr:
                    continue  # no daily levels loaded, skip

                match = near_daily_sr(result["level"], sr)
                if not match:
                    continue  # swept level not near proven daily S/R

                # Enrich result with the daily S/R match
                result["daily_type"] = match["type"]
                result["daily_level"] = match["daily_level"]

                # Dedup: only alert once per symbol+direction+level
                alert_key = f"{symbol}_{result['direction']}_{result['level']:.6f}"
                if last_alert_key.get(symbol) != alert_key:
                    last_alert_key[symbol] = alert_key
                    found += 1
                    embed = build_embed(symbol, result)
                    await channel.send(embed=embed)

        except Exception as e:
            pass  # silently skip failed symbols

        # Rate limit: small delay between requests
        await asyncio.sleep(0.15)

    if found:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Scan complete — {found} alert(s)")


@scan_loop.before_loop
async def before_scan():
    await bot.wait_until_ready()


# =====================================================================
#  COMMANDS
# =====================================================================
@bot.command(name="setchannel")
async def set_channel(ctx):
    """Set the current channel as the alert channel."""
    global CHANNEL_ID
    CHANNEL_ID = ctx.channel.id
    await ctx.send(f"Alert channel set to **#{ctx.channel.name}** (`{CHANNEL_ID}`)")
    if not scan_loop.is_running():
        scan_loop.start()
        await ctx.send(f"Scanner started — watching **{len(watchlist)}** coins on **{TIMEFRAME}**")


@bot.command(name="top100")
async def reload_top100(ctx):
    """Reload the top 100 coins by volume."""
    global watchlist
    try:
        watchlist = await fetch_top100()
        last_alert_key.clear()
        await ctx.send(f"Reloaded **{len(watchlist)}** top coins by volume — loading daily S/R levels...")
        await load_all_daily_sr(watchlist, ctx.channel)
    except Exception as e:
        await ctx.send(f"Failed to reload: {e}")


@bot.command(name="watchlist")
async def show_watchlist(ctx):
    """Show current watchlist."""
    if not watchlist:
        await ctx.send("Watchlist is empty. Use `!top100` to load.")
        return
    # Show in chunks to avoid message limit
    chunks = [watchlist[i:i+20] for i in range(0, len(watchlist), 20)]
    for i, chunk in enumerate(chunks):
        await ctx.send(f"**Watchlist ({i*20+1}-{i*20+len(chunk)}/{len(watchlist)}):**\n`{', '.join(chunk)}`")


@bot.command(name="addcoin")
async def add_coin(ctx, *, coins: str):
    """Add coins to watchlist. Usage: !addcoin BTCUSDT, XRPUSDT"""
    added = []
    for c in coins.replace(",", " ").split():
        c = c.strip().upper()
        if c and c not in watchlist:
            watchlist.append(c)
            added.append(c)
    if added:
        await ctx.send(f"Added: **{', '.join(added)}** — watchlist now has {len(watchlist)} coins")
    else:
        await ctx.send("Those coins are already in the watchlist")


@bot.command(name="removecoin")
async def remove_coin(ctx, *, coins: str):
    """Remove coins from watchlist. Usage: !removecoin DOGEUSDT"""
    removed = []
    for c in coins.replace(",", " ").split():
        c = c.strip().upper()
        if c in watchlist:
            watchlist.remove(c)
            removed.append(c)
    if removed:
        await ctx.send(f"Removed: **{', '.join(removed)}** — watchlist now has {len(watchlist)} coins")
    else:
        await ctx.send("Those coins weren't in the watchlist")


@bot.command(name="tf")
async def set_timeframe(ctx, new_tf: str):
    """Change timeframe. Usage: !tf 1h"""
    global TIMEFRAME
    valid = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"]
    new_tf = new_tf.lower()
    if new_tf not in valid:
        await ctx.send(f"Invalid timeframe. Use one of: `{', '.join(valid)}`")
        return
    TIMEFRAME = new_tf
    last_alert_key.clear()
    await ctx.send(f"Timeframe changed to **{TIMEFRAME}** — alerts reset")


@bot.command(name="status")
async def show_status(ctx):
    """Show scanner status."""
    running = scan_loop.is_running()
    status = "Running" if running else "Stopped"
    await ctx.send(
        f"**Scanner Status:** {status}\n"
        f"**Coins:** {len(watchlist)}\n"
        f"**Timeframe:** {TIMEFRAME}\n"
        f"**Scan interval:** {SCAN_INTERVAL_SECONDS}s\n"
        f"**Channel:** <#{CHANNEL_ID}>"
    )


@bot.command(name="pause")
async def pause_scanner(ctx):
    """Pause the scanner."""
    if scan_loop.is_running():
        scan_loop.stop()
        await ctx.send("Scanner paused. Use `!resume` to restart.")
    else:
        await ctx.send("Scanner is already paused.")


@bot.command(name="resume")
async def resume_scanner(ctx):
    """Resume the scanner."""
    if not scan_loop.is_running():
        last_alert_key.clear()
        scan_loop.start()
        await ctx.send(f"Scanner resumed — watching **{len(watchlist)}** coins on **{TIMEFRAME}**")
    else:
        await ctx.send("Scanner is already running.")


@bot.command(name="clear")
async def clear_alerts(ctx):
    """Reset alert dedup so all signals fire fresh."""
    last_alert_key.clear()
    await ctx.send("Alert history cleared — all signals will fire fresh on next scan")


@bot.command(name="commands")
async def show_commands(ctx):
    """Show all bot commands."""
    cmds = (
        "**Sweep & Reclaim Scanner Commands:**\n\n"
        "`!setchannel` — Set this channel for alerts\n"
        "`!status` — Show scanner status\n"
        "`!top100` — Reload top 100 coins by volume\n"
        "`!watchlist` — Show current watchlist\n"
        "`!addcoin BTCUSDT, XRPUSDT` — Add coins\n"
        "`!removecoin DOGEUSDT` — Remove coins\n"
        "`!tf 15m` — Change timeframe (1m/5m/15m/1h/4h/1d)\n"
        "`!pause` — Pause scanner\n"
        "`!resume` — Resume scanner\n"
        "`!clear` — Reset alerts (fire all fresh)\n"
        "`!commands` — Show this list"
    )
    await ctx.send(cmds)


# =====================================================================
#  RUN
# =====================================================================
if __name__ == "__main__":
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("""
╔══════════════════════════════════════════════════════════╗
║  SETUP REQUIRED — Follow these steps:                    ║
║                                                          ║
║  1. Go to https://discord.com/developers/applications    ║
║  2. Click "New Application" → name it "Sweep Scanner"    ║
║  3. Go to "Bot" tab → click "Reset Token" → copy token   ║
║  4. Under "Privileged Gateway Intents":                   ║
║     Turn ON "Message Content Intent"                      ║
║  5. Go to "OAuth2" → "URL Generator":                     ║
║     - Scopes: bot                                         ║
║     - Bot Permissions: Send Messages, Embed Links,        ║
║       Read Message History                                ║
║     - Copy the generated URL → open it → add to server    ║
║  6. In Discord: right-click the alerts channel →           ║
║     "Copy Channel ID" (enable Developer Mode in Settings   ║
║     → Advanced if you don't see it)                        ║
║  7. Set your token and channel ID:                         ║
║                                                            ║
║     Option A — Edit this file:                             ║
║       BOT_TOKEN = "your-token-here"                        ║
║       CHANNEL_ID = 123456789                               ║
║                                                            ║
║     Option B — Environment variables:                      ║
║       export DISCORD_BOT_TOKEN="your-token-here"           ║
║       export DISCORD_CHANNEL_ID="123456789"                ║
║                                                            ║
║  8. python bot.py                                          ║
╚══════════════════════════════════════════════════════════╝
""")
    else:
        bot.run(BOT_TOKEN)
