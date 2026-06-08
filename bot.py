import discord
from discord.ext import commands, tasks
import aiohttp
import asyncio
from datetime import datetime, timezone
from config import (
    DISCORD_TOKEN, RIOT_API_KEY, DISCORD_CHANNEL_ID,
    TRACKED_PLAYERS, CHECK_INTERVAL_SECONDS, REGION, PLATFORM
)

HEADERS = {"X-Riot-Token": RIOT_API_KEY}

RANK_ORDER = {
    "IRON": 0, "BRONZE": 1, "SILVER": 2, "GOLD": 3,
    "PLATINUM": 4, "EMERALD": 5, "DIAMOND": 6,
    "MASTER": 7, "GRANDMASTER": 8, "CHALLENGER": 9
}
DIVISION_ORDER = {"IV": 0, "III": 1, "II": 2, "I": 3}

RANK_EMOJIS = {
    "IRON": "⬛", "BRONZE": "🟫", "SILVER": "⬜", "GOLD": "🟨",
    "PLATINUM": "🟦", "EMERALD": "🟩", "DIAMOND": "💎",
    "MASTER": "🔮", "GRANDMASTER": "🏆", "CHALLENGER": "👑",
    "UNRANKED": "❓"
}

POSITION_EMOJIS = {
    "TOP": "🛡️", "JUNGLE": "🌲", "MIDDLE": "⚡",
    "BOTTOM": "🏹", "UTILITY": "💊"
}

# ─────────────────────────────────────────────
#  Stats tracking
# ─────────────────────────────────────────────

stats = {
    "requests_total": 0,
    "rate_limit_hits": 0,
    "errors": 0,
    "last_check": None,
    "checks_total": 0,
    "games_detected": 0,
    "bot_start": datetime.now(timezone.utc),
}

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ─────────────────────────────────────────────
#  Riot API helpers
# ─────────────────────────────────────────────

async def riot_get(session, url: str, label: str = "") -> tuple[int, dict | list | None]:
    stats["requests_total"] += 1
    try:
        async with session.get(url, headers=HEADERS) as r:
            status = r.status
            if status == 200:
                return status, await r.json()
            if status == 429:
                retry_after = int(r.headers.get("Retry-After", 10))
                stats["rate_limit_hits"] += 1
                log(f"⚠️  RATE LIMIT HIT ({label}) – čekám {retry_after}s")
                channel = bot.get_channel(DISCORD_CHANNEL_ID)
                if channel:
                    await channel.send(
                        f"⚠️ **Riot API rate limit!** Příliš mnoho requestů.\n"
                        f"Čekám **{retry_after}s** než zkusím znovu. "
                        f"(Celkem hitů: {stats['rate_limit_hits']})"
                    )
                await asyncio.sleep(retry_after)
                return status, None
            if status == 403:
                log(f"❌ 403 Forbidden ({label}) – neplatný nebo expirovaný API klíč!")
                stats["errors"] += 1
                return status, None
            if status == 404:
                return status, None
            log(f"⚠️  HTTP {status} pro {label}")
            stats["errors"] += 1
            return status, None
    except aiohttp.ClientError as e:
        log(f"❌ Síťová chyba ({label}): {e}")
        stats["errors"] += 1
        return 0, None


async def get_account_by_riot_id(session, game_name, tag_line):
    url = f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    _, data = await riot_get(session, url, f"account/{game_name}#{tag_line}")
    return data


async def get_summoner_by_puuid(session, puuid):
    url = f"https://{PLATFORM}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    _, data = await riot_get(session, url, f"summoner/{puuid[:8]}")
    return data


async def get_live_game(session, puuid):
    url = f"https://{PLATFORM}.api.riotgames.com/lol/spectator/v5/active-games/by-summoner/{puuid}"
    _, data = await riot_get(session, url, f"spectator/{puuid[:8]}")
    return data


async def get_ranked_stats(session, puuid: str) -> dict:
    url = f"https://{PLATFORM}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    status, raw = await riot_get(session, url, f"ranked/{puuid[:8]}")
    if not raw:
        return {}
    result = {}
    for entry in raw:
        wins = entry.get("wins", 0)
        losses = entry.get("losses", 0)
        total = wins + losses
        result[entry["queueType"]] = {
            "tier": entry.get("tier", "UNRANKED"),
            "rank": entry.get("rank", ""),
            "lp": entry.get("leaguePoints", 0),
            "wins": wins, "losses": losses, "total": total,
            "winrate": round((wins / total * 100) if total else 0, 1),
        }
    return result


async def get_match_history(session, puuid: str, count: int = 1) -> list:
    """Vrátí seznam posledních match ID z match-v5."""
    url = (
        f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        f"?start=0&count={count}"
    )
    _, data = await riot_get(session, url, f"matchlist/{puuid[:8]}")
    return data or []


async def get_match_detail(session, match_id: str) -> dict | None:
    """Vrátí detail zápasu z match-v5."""
    url = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}"
    _, data = await riot_get(session, url, f"match/{match_id}")
    return data


# ─────────────────────────────────────────────
#  Champion cache
# ─────────────────────────────────────────────

champion_cache: dict[int, str] = {}

async def load_champion_cache(session):
    try:
        async with session.get("https://ddragon.leagueoflegends.com/api/versions.json") as r:
            versions = await r.json()
            version = versions[0]
        async with session.get(
            f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
        ) as r:
            data = await r.json()
            for name, info in data["data"].items():
                champion_cache[int(info["key"])] = name
        log(f"✅ Champion cache načten ({len(champion_cache)} championů, patch {version})")
    except Exception as e:
        log(f"⚠️  Nepodařilo se načíst champion cache: {e}")


def champion_name(champion_id: int) -> str:
    return champion_cache.get(champion_id, str(champion_id))


# ─────────────────────────────────────────────
#  Rank helpers
# ─────────────────────────────────────────────

def rank_score(tier: str, division: str, lp: int) -> int:
    """Převede rank na číselné skóre pro průměrování. 1 LP = 1 bod."""
    t = RANK_ORDER.get(tier.upper(), -1)
    if t == -1:
        return -1
    d = DIVISION_ORDER.get(division.upper(), 0) if division else 0
    # Každý tier = 400 bodů (4 divize × 100 LP), každá divize = 100 bodů
    return t * 400 + d * 100 + lp


def score_to_rank_label(score: int) -> str:
    """Převede číselné skóre zpět na čitelný rank."""
    if score < 0:
        return "Unranked"
    tiers = list(RANK_ORDER.keys())
    tier_idx = min(score // 400, len(tiers) - 1)
    tier = tiers[tier_idx]
    if tier in ("MASTER", "GRANDMASTER", "CHALLENGER"):
        return tier.capitalize()
    remainder = score % 400
    div_idx = min(remainder // 100, 3)
    divisions = ["IV", "III", "II", "I"]
    return f"{tier.capitalize()} {divisions[div_idx]}"


def average_rank_label(scores: list[int]) -> str:
    valid = [s for s in scores if s >= 0]
    if not valid:
        return "Unranked"
    return score_to_rank_label(round(sum(valid) / len(valid)))


def format_rank(solo: dict) -> str:
    tier = solo.get("tier", "UNRANKED")
    if not tier or tier == "UNRANKED":
        return "Unranked"
    div = solo.get("rank", "")
    lp = solo.get("lp", 0)
    if tier.upper() in ("MASTER", "GRANDMASTER", "CHALLENGER"):
        return f"{tier.capitalize()} {lp} LP"
    return f"{tier.capitalize()} {div} – {lp} LP"


# ─────────────────────────────────────────────
#  Bot state
# ─────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# puuid -> game_id aktuálně probíhající hry
active_games: dict[str, str] = {}

# game_id -> True  (již odeslané lobby zprávy, brání duplicitám)
notified_games: set[str] = set()

# puuid -> poslední známé match_id (pro detekci dokončených her)
last_known_match: dict[str, str] = {}

summoner_cache: dict[str, dict] = {}


@bot.event
async def on_ready():
    log(f"✅ Bot přihlášen jako {bot.user}")
    async with aiohttp.ClientSession() as session:
        await load_champion_cache(session)
        await resolve_players(session)
        # Načti aktuální match history jako baseline (nechceme posílat staré hry)
        await init_match_baselines(session)
    log(f"🔁 Spouštím polling každých {CHECK_INTERVAL_SECONDS}s")
    check_live_games.start()


async def resolve_players(session):
    log(f"🔍 Resolvuji {len(TRACKED_PLAYERS)} hráčů...")
    for player in TRACKED_PLAYERS:
        game_name, tag = player["riot_id"].split("#", 1)
        account = await get_account_by_riot_id(session, game_name, tag)
        if not account:
            log(f"⚠️  Účet nenalezen: {player['riot_id']}")
            continue
        puuid = account["puuid"]
        player["puuid"] = puuid

        summoner = await get_summoner_by_puuid(session, puuid)
        if summoner:
            summoner_cache[puuid] = summoner
            log(f"   ✅ {player['riot_id']} → PUUID {puuid[:12]}...")
        await asyncio.sleep(1.2)


async def init_match_baselines(session):
    """Při startu zapamatuj poslední match každého hráče, abychom neposílali staré hry."""
    log("📋 Načítám match baselines...")
    for player in TRACKED_PLAYERS:
        puuid = player.get("puuid")
        if not puuid:
            continue
        matches = await get_match_history(session, puuid, count=1)
        if matches:
            last_known_match[puuid] = matches[0]
            log(f"   {player['riot_id']} baseline: {matches[0]}")
        await asyncio.sleep(1.0)


# ─────────────────────────────────────────────
#  Main loop
# ─────────────────────────────────────────────

@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def check_live_games():
    stats["checks_total"] += 1
    stats["last_check"] = datetime.now(timezone.utc)
    log(f"🔎 Check #{stats['checks_total']} | reqs: {stats['requests_total']} | RL hity: {stats['rate_limit_hits']}")

    async with aiohttp.ClientSession() as session:
        for player in TRACKED_PLAYERS:
            puuid = player.get("puuid")

            # ── Bod 1: skrytý profil ──────────────────────────
            if not puuid:
                log(f"   🔒 {player['riot_id']} – skrytý nebo nedostupný profil, přeskakuji")
                continue

            game = await get_live_game(session, puuid)

            if game is None:
                # Hráč není ve hře – zkontroluj jestli právě dohrál
                if puuid in active_games:
                    log(f"   💤 {player['riot_id']} opustil hru {active_games[puuid]}")
                    del active_games[puuid]
                    # Počkej chvíli než Riot API zpracuje výsledek
                    asyncio.create_task(check_match_result(player, puuid))
                continue

            game_id = str(game.get("gameId"))

            if active_games.get(puuid) == game_id:
                log(f"   🎮 {player['riot_id']} stále ve hře {game_id}")
                continue

            # Hráč vstoupil do nové hry
            active_games[puuid] = game_id

            # ── Bod 2: deduplicita lobby ──────────────────────
            if game_id in notified_games:
                log(f"   ℹ️  {player['riot_id']} – game {game_id} již notifikována, přeskakuji")
                continue

            notified_games.add(game_id)
            stats["games_detected"] += 1

            # Zjisti které sledované hráče obsahuje tato lobby
            tracked_in_game = [
                p["riot_id"] for p in TRACKED_PLAYERS
                if p.get("puuid") in [part.get("puuid") for part in game.get("participants", [])]
            ]
            log(f"   🆕 NOVÁ HRA: game_id={game_id} | sledovaní v lobby: {tracked_in_game}")

            try:
                embed = await build_game_embed(session, game, player, tracked_in_game)
                channel = bot.get_channel(DISCORD_CHANNEL_ID)
                if channel:
                    await channel.send(embed=embed)
                    log(f"   ✅ Embed odeslán")
                else:
                    log(f"   ❌ Kanál {DISCORD_CHANNEL_ID} nenalezen!")
            except Exception as e:
                log(f"   ❌ Chyba při stavbě embedu: {e}")
                stats["errors"] += 1

            await asyncio.sleep(1.2)


async def check_match_result(player: dict, puuid: str):
    """Po skončení hry počkej a pak zkontroluj výsledek posledního zápasu."""
    await asyncio.sleep(30)  # Riot API potřebuje čas na zpracování výsledku

    async with aiohttp.ClientSession() as session:
        matches = await get_match_history(session, puuid, count=1)
        if not matches:
            return

        latest_match_id = matches[0]
        known = last_known_match.get(puuid)

        if latest_match_id == known:
            # Výsledek ještě není k dispozici, zkus znovu za chvíli
            await asyncio.sleep(60)
            matches = await get_match_history(session, puuid, count=1)
            if not matches or matches[0] == known:
                log(f"   ⚠️  Match result pro {player['riot_id']} stále nedostupný")
                return
            latest_match_id = matches[0]

        last_known_match[puuid] = latest_match_id
        log(f"   📊 Nový match výsledek: {player['riot_id']} → {latest_match_id}")

        detail = await get_match_detail(session, latest_match_id)
        if not detail:
            return

        embed = build_match_result_embed(detail, puuid, player["riot_id"])
        if embed:
            channel = bot.get_channel(DISCORD_CHANNEL_ID)
            if channel:
                await channel.send(embed=embed)


# ─────────────────────────────────────────────
#  Match result embed
# ─────────────────────────────────────────────

def build_match_result_embed(match: dict, puuid: str, riot_id: str) -> discord.Embed | None:
    QUEUE_NAMES = {
        420: "Ranked Solo/Duo", 440: "Ranked Flex",
        400: "Normal Draft",    430: "Normal Blind",
        450: "ARAM",            700: "Clash",
    }

    info = match.get("info", {})
    queue_id = info.get("queueId", 0)
    queue_name = QUEUE_NAMES.get(queue_id, f"Mód {queue_id}")

    # Najdi tohoto hráče v participantech
    participant = next(
        (p for p in info.get("participants", []) if p.get("puuid") == puuid),
        None
    )
    if not participant:
        return None

    win = participant.get("win", False)
    champ = champion_name(participant.get("championId", 0))
    kills = participant.get("kills", 0)
    deaths = participant.get("deaths", 0)
    assists = participant.get("assists", 0)
    kda_str = f"{kills}/{deaths}/{assists}"

    # LP změna – dostupná jen u ranked, Riot vrací přímo v participantovi
    lp_change = None
    lp_before = participant.get("lpBefore")
    lp_after  = participant.get("lpAfter")
    if lp_before is not None and lp_after is not None:
        lp_change = lp_after - lp_before
    else:
        # Fallback: interpretace z ratingEarnedForPlacement (není vždy dostupné)
        lp_change = participant.get("lpChange") or participant.get("ratingChange")

    result_str = "✅ Výhra" if win else "❌ Prohra"
    color = 0x57F287 if win else 0xED4245

    embed = discord.Embed(
        title=f"{result_str} – {queue_name}",
        description=f"**{riot_id}** dokončil hru",
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Champion", value=champ, inline=True)
    embed.add_field(name="K/D/A", value=kda_str, inline=True)

    if lp_change is not None:
        lp_str = f"+{lp_change} LP" if lp_change >= 0 else f"{lp_change} LP"
        embed.add_field(name="LP změna", value=lp_str, inline=True)
    elif queue_id in (420, 440):
        embed.add_field(name="LP změna", value="N/A", inline=True)

    embed.set_footer(text=f"LoL Live Tracker • {match.get('metadata', {}).get('matchId', '')}")
    return embed


# ─────────────────────────────────────────────
#  Live game embed
# ─────────────────────────────────────────────

async def build_game_embed(session, game: dict, trigger_player: dict, tracked_in_game: list[str]) -> discord.Embed:
    QUEUE_NAMES = {
        420: "Ranked Solo/Duo", 440: "Ranked Flex",
        400: "Normal Draft",    430: "Normal Blind",
        450: "ARAM",            700: "Clash",
    }
    queue_id = game.get("gameQueueConfigId", 0)
    queue_name = QUEUE_NAMES.get(queue_id, f"Mód {queue_id}")

    team1, team2, rank_scores = [], [], []

    for i, p in enumerate(game.get("participants", []), 1):
        puuid_p  = p.get("puuid")
        name     = p.get("summonerName") or p.get("riotId", "Unknown")
        champ    = champion_name(p.get("championId", 0))
        team_id  = p.get("teamId", 100)
        position = p.get("teamPosition") or ""

        # ── Bod 1: skrytý profil v lobby ──────────────────────
        if not puuid_p:
            log(f"      [{i}/10] {name} – chybí PUUID (skrytý profil)")
            entry = dict(
                name=name, champion=champ, position=position,
                tier="HIDDEN", div="", lp=0,
                wins=0, losses=0, total=0, winrate=0,
                is_tracked=False, is_trigger=False, score=-1,
            )
            (team1 if team_id == 100 else team2).append(entry)
            continue

        log(f"      [{i}/10] {name} ({champ}) puuid={puuid_p[:12]}")
        ranked = await get_ranked_stats(session, puuid_p)
        await asyncio.sleep(0.3)

        solo    = ranked.get("RANKED_SOLO_5x5", {})
        tier    = solo.get("tier", "UNRANKED")
        div     = solo.get("rank", "")
        lp      = solo.get("lp", 0)
        wins    = solo.get("wins", 0)
        losses  = solo.get("losses", 0)
        total   = solo.get("total", 0)
        winrate = solo.get("winrate", 0)

        score = rank_score(tier, div, lp)
        rank_scores.append(score)
        log(f"         → {format_rank({'tier': tier, 'rank': div, 'lp': lp})} | {winrate}% WR ({total} her)")

        entry = dict(
            name=name, champion=champ, position=position,
            tier=tier, div=div, lp=lp,
            wins=wins, losses=losses, total=total, winrate=winrate,
            is_tracked=any(pl.get("puuid") == puuid_p for pl in TRACKED_PLAYERS),
            is_trigger=(puuid_p == trigger_player.get("puuid")),
            score=score,
        )
        (team1 if team_id == 100 else team2).append(entry)

    avg_rank = average_rank_label(rank_scores)
    log(f"   📊 Průměrný rank lobby: {avg_rank}")

    color = 0x0095FF if queue_id in (420, 440) else 0x00C49A

    # ── Bod 2: pokud více sledovaných v lobby, uveď je všechny ──
    if len(tracked_in_game) > 1:
        tracked_str = ", ".join(f"**{r}**" for r in tracked_in_game)
        description = (
            f"👁️ Sledovaní hráči v lobby: {tracked_str}\n"
            f"📊 Průměrný rank lobby: **{avg_rank}**"
        )
    else:
        description = (
            f"**{trigger_player['riot_id']}** vstoupil do hry!\n"
            f"📊 Průměrný rank lobby: **{avg_rank}**"
        )

    embed = discord.Embed(
        title=f"🎮 {queue_name} – Live Game",
        description=description,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"LoL Live Tracker • Game ID: {game.get('gameId', '?')}")

    def fmt(p: dict) -> str:
        # ── Bod 1: zobrazení skrytého profilu ──
        if p["tier"] == "HIDDEN":
            pos_emoji = POSITION_EMOJIS.get(p["position"].upper(), "")
            marks = " ⭐" if p["is_trigger"] else ""
            marks += " 👁️" if p["is_tracked"] else ""
            return f"{pos_emoji} **{p['champion']}** – {p['name']}{marks}\n  🔒 Skrytý profil"

        rank_emoji = RANK_EMOJIS.get(p["tier"].upper(), "❓")
        pos_emoji  = POSITION_EMOJIS.get(p["position"].upper(), "")
        rank_str   = format_rank({"tier": p["tier"], "rank": p["div"], "lp": p["lp"]})
        wr_str     = f"{p['winrate']}% WR ({p['total']} her)" if p["total"] > 0 else "Unranked"
        marks      = (" ⭐" if p["is_trigger"] else "") + (" 👁️" if p["is_tracked"] else "")
        return (
            f"{pos_emoji} **{p['champion']}** – {p['name']}{marks}\n"
            f"  {rank_emoji} {rank_str} | {wr_str}"
        )

    embed.add_field(name="🔵 Tým 1 (Blue)", value="\n\n".join(fmt(p) for p in team1) or "—", inline=False)
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # vizuální oddělovač
    embed.add_field(name="🔴 Tým 2 (Red)",  value="\n\n".join(fmt(p) for p in team2) or "—", inline=False)
    return embed


# ─────────────────────────────────────────────
#  Commands
# ─────────────────────────────────────────────

@bot.command(name="tracked")
async def cmd_tracked(ctx):
    lines = [
        f"• **{p['riot_id']}** – {'🎮 Ve hře' if p.get('puuid') in active_games else '💤 Offline/Lobby'}"
        for p in TRACKED_PLAYERS
    ]
    await ctx.send(embed=discord.Embed(
        title="👁️ Sledovaní hráči",
        description="\n".join(lines),
        color=0x5865F2,
    ))


@bot.command(name="status")
async def cmd_status(ctx):
    uptime = datetime.now(timezone.utc) - stats["bot_start"]
    h, m = divmod(int(uptime.total_seconds()), 3600)
    m, s = divmod(m, 60)
    e = discord.Embed(title="✅ Bot je online", color=0x57F287)
    e.add_field(name="Uptime",           value=f"{h}h {m}m {s}s",           inline=True)
    e.add_field(name="Sledovaní hráči",  value=str(len(TRACKED_PLAYERS)),    inline=True)
    e.add_field(name="Aktivní hry",      value=str(len(active_games)),       inline=True)
    e.add_field(name="Check interval",   value=f"{CHECK_INTERVAL_SECONDS}s", inline=True)
    e.add_field(name="Celkem checků",    value=str(stats["checks_total"]),   inline=True)
    e.add_field(name="Celkem requestů",  value=str(stats["requests_total"]), inline=True)
    e.add_field(name="Rate limit hity",  value=str(stats["rate_limit_hits"]),inline=True)
    e.add_field(name="Chyby",            value=str(stats["errors"]),         inline=True)
    e.add_field(name="Detekované hry",   value=str(stats["games_detected"]), inline=True)
    if stats["last_check"]:
        e.add_field(name="Poslední check", value=f"<t:{int(stats['last_check'].timestamp())}:R>", inline=True)
    await ctx.send(embed=e)


@bot.command(name="debug")
async def cmd_debug(ctx, riot_id: str = None):
    player = None
    if riot_id:
        player = next((p for p in TRACKED_PLAYERS if p["riot_id"].lower() == riot_id.lower()), None)
        if not player:
            await ctx.send(f"❌ Hráč `{riot_id}` není v seznamu sledovaných.")
            return
    else:
        player = next((p for p in TRACKED_PLAYERS if p.get("puuid")), None)
        if not player:
            await ctx.send("❌ Žádný hráč ještě nemá resolvnuté PUUID.")
            return

    await ctx.send(f"🔍 Hledám živou hru pro **{player['riot_id']}**...")

    async with aiohttp.ClientSession() as session:
        puuid = player.get("puuid")
        if not puuid:
            await ctx.send("❌ Hráč nemá PUUID.")
            return

        game = await get_live_game(session, puuid)
        if game is None:
            await ctx.send(
                f"💤 **{player['riot_id']}** momentálně není ve hře.\n"
                f"Posílám **ukázkový embed** se statickými daty:"
            )
            await ctx.send(embed=_build_fake_embed(player))
            return

        await ctx.send(f"✅ Živá hra nalezena (ID: `{game.get('gameId')}`) – stavím embed...")
        try:
            tracked_in_game = [
                p["riot_id"] for p in TRACKED_PLAYERS
                if p.get("puuid") in [part.get("puuid") for part in game.get("participants", [])]
            ]
            embed = await build_game_embed(session, game, player, tracked_in_game)
            await ctx.send(embed=embed)
        except Exception as e:
            await ctx.send(f"❌ Chyba při stavbě embedu: `{e}`")


def _build_fake_embed(trigger_player: dict) -> discord.Embed:
    embed = discord.Embed(
        title="🎮 Ranked Solo/Duo – Live Game (UKÁZKA)",
        description=(
            f"**{trigger_player['riot_id']}** vstoupil do hry!\n"
            f"📊 Průměrný rank lobby: **Diamond III**\n"
            f"⚠️ *Toto jsou fiktivní data – hráč momentálně není ve hře.*"
        ),
        color=0xFFA500,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="LoL Live Tracker • DEBUG MODE")
    team1 = (
        "⚡ **Orianna** – FakePlayer1 ⭐\n  💎 Diamond II – 75 LP | 58.3% WR (120 her)\n\n"
        "🛡️ **Garen** – FakePlayer2\n  🟩 Emerald I – 45 LP | 52.1% WR (87 her)\n\n"
        "🌲 **Hecarim** – FakePlayer3\n  🟨 Gold II – 12 LP | 49.0% WR (203 her)\n\n"
        "🏹 **Jinx** – FakePlayer4\n  💎 Diamond IV – 3 LP | 55.7% WR (310 her)\n\n"
        "💊 **Thresh** – FakePlayer5\n  🟦 Platinum III – 88 LP | 61.2% WR (54 her)"
    )
    team2 = (
        "⚡ **Syndra** – EnemyPlayer1\n  💎 Diamond I – 22 LP | 53.8% WR (175 her)\n\n"
        "🛡️ **Darius** – EnemyPlayer2\n  🟩 Emerald II – 60 LP | 50.0% WR (98 her)\n\n"
        "🌲 **Vi** – EnemyPlayer3\n  💎 Diamond III – 40 LP | 56.4% WR (141 her)\n\n"
        "🏹 **Caitlyn** – EnemyPlayer4\n  🟩 Emerald IV – 5 LP | 47.9% WR (67 her)\n\n"
        "💊 **Lulu** – EnemyPlayer5\n  🟦 Platinum II – 99 LP | 59.1% WR (44 her)"
    )
    embed.add_field(name="🔵 Tým 1 (Blue)", value=team1, inline=False)
    embed.add_field(name="🔴 Tým 2 (Red)",  value=team2, inline=False)
    return embed


@bot.command(name="track")
async def cmd_track(ctx, riot_id: str = None):
    if not riot_id or "#" not in riot_id:
        await ctx.send("❌ Použití: `!track Jméno#TAG`")
        return

    game_name, tag = riot_id.split("#", 1)
    msg = await ctx.send(f"🔍 Hledám účet **{riot_id}**...")

    async with aiohttp.ClientSession() as session:
        account = await get_account_by_riot_id(session, game_name, tag)
        if not account:
            await msg.edit(content=f"❌ Účet **{riot_id}** nenalezen.")
            return

        puuid = account["puuid"]
        summoner = await get_summoner_by_puuid(session, puuid)
        if not summoner:
            await msg.edit(content=f"❌ Summoner pro **{riot_id}** nenalezen.")
            return

        await msg.edit(content=f"✅ Účet nalezen. Hledám živou hru pro **{riot_id}**...")

        game = await get_live_game(session, puuid)
        if game is None:
            await msg.edit(content=f"💤 **{riot_id}** momentálně není ve hře.")
            return

        queue_map = {420: "Ranked Solo/Duo", 440: "Ranked Flex", 400: "Normal Draft",
                     430: "Normal Blind", 450: "ARAM", 700: "Clash"}
        queue_id = game.get("gameQueueConfigId", 0)
        queue_name = queue_map.get(queue_id, f"Mód {queue_id}")
        game_id = game.get("gameId", "?")

        await msg.edit(content=f"🎮 Živá hra nalezena! ({queue_name}, ID: `{game_id}`) Načítám statistiky...")
        log(f"!track {riot_id} → živá hra {game_id} ({queue_name})")

        pseudo_player = {"riot_id": riot_id, "puuid": puuid}
        tracked_in_game = [
            p["riot_id"] for p in TRACKED_PLAYERS
            if p.get("puuid") in [part.get("puuid") for part in game.get("participants", [])]
        ]

        try:
            embed = await build_game_embed(session, game, pseudo_player, tracked_in_game)
            await msg.edit(content="", embed=embed)
        except Exception as e:
            import traceback
            traceback.print_exc()
            await msg.edit(content=f"❌ Chyba při načítání statistik: `{e}`")
            log(f"!track chyba: {e}")
            stats["errors"] += 1



@bot.command(name="trackg")
async def cmd_trackg(ctx, riot_id: str = None):
    """
    !trackg Jméno#TAG  – ukáže výsledkový embed z poslední dohrané hry daného hráče
    """
    if not riot_id or "#" not in riot_id:
        await ctx.send("❌ Použití: `!trackg Jméno#TAG`")
        return

    game_name, tag = riot_id.split("#", 1)
    msg = await ctx.send(f"🔍 Hledám účet **{riot_id}**...")

    async with aiohttp.ClientSession() as session:
        account = await get_account_by_riot_id(session, game_name, tag)
        if not account:
            await msg.edit(content=f"❌ Účet **{riot_id}** nenalezen.")
            return
        puuid = account["puuid"]

        await msg.edit(content=f"✅ Účet nalezen. Stahuji poslední hru...")

        matches = await get_match_history(session, puuid, count=1)
        if not matches:
            await msg.edit(content=f"❌ Žádné dohrané hry nenalezeny pro **{riot_id}**.")
            return

        match_id = matches[0]
        await msg.edit(content=f"📋 Načítám detail hry `{match_id}`...")

        detail = await get_match_detail(session, match_id)
        if not detail:
            await msg.edit(content=f"❌ Nepodařilo se načíst detail hry `{match_id}`.")
            return

        embed = build_match_result_embed(detail, puuid, riot_id)
        if not embed:
            await msg.edit(content=f"❌ Hráč **{riot_id}** nebyl nalezen v zápase `{match_id}`.")
            return

        await msg.edit(content="", embed=embed)
        log(f"!trackg {riot_id} → {match_id}")

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)