#!/usr/bin/env python3
"""
Dota2 Telegram Tracker Bot - full version (single file)
Features:
- Steam binding (steam32, steam64, profiles/<steam64>)
- Auto MMR (approx. from rank_tier) + manual MMR override via "mmr 4321"
- Last match (any mode), Last 10 ranked, heroes analytics (top15), activity & MMR trend charts (PNG)
- Daily report at 23:59 MSK (always sent, even if 0 games)
- Polling OpenDota for new matches every POLL_INTERVAL seconds (default 60)
- Notifications: new match, streak alerts, rank up/down
- Caching OpenDota results for short TTL to reduce latency (default 90s)
- SQLite for persistence (users + matches)
- Designed to run on Railway / Heroku-like platforms
"""

import os
import re
import json
import math
import time
import sqlite3
import logging
import aiohttp
import asyncio
import tempfile
from contextlib import closing
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone

# aiogram v3 style
from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import F

# matplotlib for charts
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN env variable with your Telegram bot token.")

OPEN_DOTA = "https://api.opendota.com/api"
DB_PATH = os.getenv("DB_PATH", "dota_bot.db")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))     # seconds
CACHE_TTL = int(os.getenv("CACHE_TTL", "90"))             # seconds for OpenDota caching
MSK_OFFSET = int(os.getenv("MSK_OFFSET", "3"))            # MSK offset from UTC
ASSUMED_MMR_DELTA = int(os.getenv("ASSUMED_MMR_DELTA", "30"))
DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "23"))
DAILY_REPORT_MINUTE = int(os.getenv("DAILY_REPORT_MINUTE", "59"))

STREAK_NOTIFY_WIN = int(os.getenv("STREAK_NOTIFY_WIN", "5"))
STREAK_NOTIFY_LOSE = int(os.getenv("STREAK_NOTIFY_LOSE", "5"))

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dota_bot")

# ---------------- BOT / DISPATCHER ----------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---------------- SQLITE DB ----------------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            steam32 TEXT,
            exact_mmr INTEGER,      -- user-specified exact mmr
            current_mmr INTEGER,    -- auto/estimated mmr
            max_mmr INTEGER,
            last_any_match INTEGER,
            last_ranked_match INTEGER,
            last_rank_tier INTEGER,
            created_ts INTEGER DEFAULT (strftime('%s','now'))
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            steam32 TEXT,
            match_id INTEGER,
            start_time INTEGER,
            duration INTEGER,
            hero_id INTEGER,
            kills INTEGER, deaths INTEGER, assists INTEGER,
            lobby_type INTEGER,
            game_mode INTEGER,
            radiant_win INTEGER,
            player_slot INTEGER,
            net_worth INTEGER,
            gpm INTEGER,
            delta_mmr INTEGER,
            mmr_after INTEGER,
            PRIMARY KEY (steam32, match_id)
        )
        """)
        con.commit()

def db_get_user(tg: int) -> Optional[Dict[str, Any]]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        r = con.execute("SELECT * FROM users WHERE telegram_id=?", (tg,)).fetchone()
        return dict(r) if r else None

def db_set_user_steam(tg: int, steam32: int):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("""
        INSERT INTO users (telegram_id, steam32) VALUES (?,?)
        ON CONFLICT(telegram_id) DO UPDATE SET steam32=excluded.steam32
        """, (tg, str(steam32)))
        con.commit()

def db_update_exact_mmr(tg: int, mmr: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("UPDATE users SET exact_mmr=? WHERE telegram_id=?", (mmr, tg))
        con.commit()

def db_update_auto_mmr(tg: int, mmr: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        if mmr is None:
            con.execute("UPDATE users SET current_mmr=NULL WHERE telegram_id=?", (tg,))
        else:
            con.execute("""
            UPDATE users
            SET current_mmr=?, max_mmr=MAX(COALESCE(max_mmr,0),?)
            WHERE telegram_id=?
            """, (mmr, mmr, tg))
        con.commit()

def db_set_last_ids(tg:int, any_id: Optional[int]=None, ranked_id: Optional[int]=None):
    with closing(sqlite3.connect(DB_PATH)) as con:
        if any_id is not None:
            con.execute("UPDATE users SET last_any_match=? WHERE telegram_id=?", (any_id, tg))
        if ranked_id is not None:
            con.execute("UPDATE users SET last_ranked_match=? WHERE telegram_id=?", (ranked_id, tg))
        con.commit()

def db_set_last_rank_tier(tg:int, tier: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("UPDATE users SET last_rank_tier=? WHERE telegram_id=?", (tier, tg))
        con.commit()

def db_upsert_match(steam32:str, m:dict, nw:Optional[int], gpm:Optional[int], delta:Optional[int], mmr_after:Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("""
        INSERT INTO matches (steam32, match_id, start_time, duration, hero_id, kills, deaths, assists,
                             lobby_type, game_mode, radiant_win, player_slot, net_worth, gpm, delta_mmr, mmr_after)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(steam32, match_id) DO UPDATE SET
            start_time=excluded.start_time,
            duration=excluded.duration,
            hero_id=excluded.hero_id,
            kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists,
            lobby_type=excluded.lobby_type, game_mode=excluded.game_mode,
            radiant_win=excluded.radiant_win, player_slot=excluded.player_slot,
            net_worth=excluded.net_worth, gpm=excluded.gpm,
            delta_mmr=excluded.delta_mmr, mmr_after=excluded.mmr_after
        """, (
            steam32,
            m.get("match_id"), m.get("start_time"), m.get("duration"), m.get("hero_id"),
            m.get("kills",0), m.get("deaths",0), m.get("assists",0),
            m.get("lobby_type"), m.get("game_mode"), int(bool(m.get("radiant_win"))),
            m.get("player_slot"), nw, gpm, delta, mmr_after
        ))
        con.commit()

def db_last_matches(steam32:str, limit:int=10) -> List[Dict[str,Any]]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT * FROM matches WHERE steam32=? ORDER BY start_time DESC LIMIT ?", (steam32, limit)).fetchall()
        return [dict(r) for r in rs]

def db_get_all_users_with_steam() -> List[Dict[str,Any]]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT * FROM users WHERE steam32 IS NOT NULL").fetchall()
        return [dict(r) for r in rs]

def db_sum_delta_mmr_today(steam32:str, start_ts:int, end_ts:int) -> int:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        r = con.execute("""
            SELECT SUM(COALESCE(delta_mmr,0)) s FROM matches
            WHERE steam32=? AND lobby_type=7 AND start_time BETWEEN ? AND ?
        """, (steam32, start_ts, end_ts)).fetchone()
        return int(r["s"]) if r and r["s"] is not None else 0

def db_role_wr(steam32:str) -> Dict[str,Dict[str,int]]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT role, radiant_win, player_slot FROM matches WHERE steam32=? AND role IS NOT NULL", (steam32,)).fetchall()
    stat = {"core":{"g":0,"w":0}, "support":{"g":0,"w":0}}
    for r in rs:
        role = r["role"]
        if role not in stat: continue
        win = ((r["player_slot"]<128) and (r["radiant_win"]==1)) or ((r["player_slot"]>=128) and (r["radiant_win"]==0))
        stat[role]["g"] += 1
        if win: stat[role]["w"] += 1
    return stat

def db_hero_aggregates(steam32:str) -> List[Dict[str,Any]]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("""
        SELECT hero_id, COUNT(*) games,
               SUM(CASE WHEN ((player_slot<128 AND radiant_win=1) OR (player_slot>=128 AND radiant_win=0)) THEN 1 ELSE 0 END) wins,
               AVG(COALESCE(net_worth,0)) avg_nw
        FROM matches WHERE steam32=? GROUP BY hero_id ORDER BY games DESC
        """, (steam32,)).fetchall()
        return [dict(r) for r in rs]

# ---------------- CACHING (in-memory) ----------------
_open_dota_cache: Dict[str, Tuple[float, Any]] = {}  # key -> (ts, data)

async def od_get(path:str, params:dict=None, use_cache:bool=True):
    """
    GET helper with simple TTL caching
    """
    key = path + (f"?{json.dumps(params, sort_keys=True)}" if params else "")
    now = time.time()
    if use_cache and key in _open_dota_cache:
        ts, data = _open_dota_cache[key]
        if now - ts < CACHE_TTL:
            return data
    url = OPEN_DOTA + path
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params, timeout=25) as r:
                if r.status == 404:
                    data = None
                else:
                    r.raise_for_status()
                    data = await r.json()
    except Exception as e:
        logger.warning("OpenDota request failed: %s %s", url, e)
        data = None
    _open_dota_cache[key] = (now, data)
    return data

# convenience wrappers
async def od_player(steam32:int): return await od_get(f"/players/{steam32}")
async def od_matches(steam32:int, limit:int=10, params:dict=None): return await od_get(f"/players/{steam32}/matches", params={**({"limit":limit} if limit else {}), **(params or {})})
async def od_recent(steam32:int): return await od_get(f"/players/{steam32}/recentMatches")
async def od_heroes_map(): return await od_get("/heroes")
async def od_player_heroes(steam32:int): return await od_get(f"/players/{steam32}/heroes")
async def od_wl(steam32:int): return await od_get(f"/players/{steam32}/wl")
async def od_match_detail(match_id:int): return await od_get(f"/matches/{match_id}", use_cache=False)

# ---------------- HELPERS ----------------
STEAM_PROFILE_RE = re.compile(r"(?:https?://)?steamcommunity\.com/(?:id|profiles)/([^/\s]+)", re.I)
STEAM64_OFFSET = 76561197960265728

def parse_steam_any(text:str) -> Optional[int]:
    text = (text or "").strip()
    m = STEAM_PROFILE_RE.search(text)
    if m:
        part = m.group(1)
        # if numeric it's steam64
        if part.isdigit() and len(part) >= 16:
            return int(part) - STEAM64_OFFSET
        # vanity names can't be resolved without Steam Web API key; ask user to send profiles/<steam64>
        return None
    if text.isdigit():
        if len(text) >= 16:
            return int(text) - STEAM64_OFFSET
        return int(text)
    return None

def fmt_duration(sec:int) -> str:
    sec = int(max(0, sec or 0))
    mm, ss = divmod(sec, 60)
    hh, mm = divmod(mm, 60)
    return f"{hh}:{mm:02d}:{ss:02d}" if hh else f"{mm}:{ss:02d}"

def ts_msk(ts:int) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=MSK_OFFSET)
    return dt.strftime("%d.%m.%Y %H:%M МСК")

def is_player_win(player_slot:int, radiant_win:bool) -> bool:
    rad = player_slot < 128
    return (rad and radiant_win) or ((not rad) and (not radiant_win))

def safe_kda(k,d,a) -> float:
    return round(( (k or 0) + (a or 0) ) / max(1, (d or 0)), 2)

def hero_name_from_map(hero_id:int, heroes_map:List[dict]) -> str:
    for h in heroes_map:
        if h.get("id")==hero_id:
            return h.get("localized_name") or f"Hero {hero_id}"
    return f"Hero {hero_id}"

def lobby_name(lobby:int) -> str:
    table = {0:"Unranked",1:"Practice",2:"Tournament",3:"Tutorial",4:"Co-op Bots",5:"Ranked Team",6:"Ranked Solo",7:"Ranked",8:"1v1 Mid",9:"Battle Cup"}
    return table.get(lobby, "Custom/Unknown")

def game_mode_name(mode:int, gm_map:dict) -> str:
    # gm_map from OPEN_DOTA /constants/game_mode might be present; fallback to integer map
    fallback = {1:"All Pick",2:"Captains Mode",3:"Random Draft",4:"Single Draft",5:"All Random",12:"Least Played",13:"Limited Heroes",14:"Compendium",15:"Custom",16:"Captains Draft",17:"Balanced Draft",18:"Ability Draft",19:"Event",20:"ARDM",21:"1v1 Mid",22:"All Draft",23:"Turbo"}
    # gm_map keys are like '1': {'id':1,'name':'game_mode_all_pick'...}
    if gm_map:
        for k,v in gm_map.items():
            try:
                if int(v.get("id",-1)) == mode:
                    return v.get("name","").replace("game_mode_","").replace("_"," ").title()
            except Exception:
                pass
    return fallback.get(mode, f"Mode {mode}")

def approx_mmr_from_rank_tier(rank_tier:Optional[int]) -> Optional[int]:
    if not isinstance(rank_tier, int): return None
    base = {1:0,2:600,3:1200,4:1800,5:2600,6:3400,7:4400,8:5400}
    major = rank_tier // 10
    minor = rank_tier % 10
    if major not in base: return None
    if major == 8:
        return base[major]
    return base[major] + (minor-1)*200

def mmr_progress_text(rank_tier:Optional[int], exact_mmr:Optional[int]) -> Optional[str]:
    if exact_mmr is None:
        return None
    if not isinstance(rank_tier, int):
        return None
    curr_est = approx_mmr_from_rank_tier(rank_tier)
    if curr_est is None:
        return None
    next_border = curr_est + 200
    need = max(0, next_border - exact_mmr)
    return f"до следующей звезды ≈ {need} MMR"

# role guess
def guess_role_from_purchase_and_gpm(purchase_keys:List[str], gpm:int) -> str:
    core_items = {"bkb","manta","daedalus","skadi","desolator","battle_fury","butterfly","radiance","satanic"}
    support_items = {"mekansm","glimmer_cape","force_staff","guardian_greaves","lotus_orb","pipe","urn_of_shadows","spirit_vessel"}
    s = set(purchase_keys or [])
    if gpm and gpm >= 420: return "core"
    if any(x in s for x in core_items): return "core"
    if any(x in s for x in support_items): return "support"
    # default based on gpm:
    if gpm and gpm < 350: return "support"
    return "core"

# ---------------- UI (keyboards) ----------------
def build_main_kb(bound:bool):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(text="🏆 Статус", callback_data="status"),
        InlineKeyboardButton(text="🎮 Последние матчи", callback_data="last_games")
    )
    kb.add(
        InlineKeyboardButton(text="🧙 Герои", callback_data="heroes_menu"),
        InlineKeyboardButton(text="📈 Активность", callback_data="activity")
    )
    kb.add(
        InlineKeyboardButton(text="📉 Тренд MMR", callback_data="mmr_trend"),
        InlineKeyboardButton(text="⚙ Привязать / Сменить Steam", callback_data="bind")
    )
    kb.add(InlineKeyboardButton(text=("🔁 Указать точный MMR" if bound else "🔗 Указать точный MMR"), callback_data="set_mmr"))
    return kb

def heroes_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔝 По играм", callback_data="heroes_games"),
        InlineKeyboardButton("✅ По WR", callback_data="heroes_wr")
    )
    kb.add(
        InlineKeyboardButton("⚔ По KDA", callback_data="heroes_kda"),
        InlineKeyboardButton("🧠 Аналитика героев", callback_data="heroes_analytics")
    )
    kb.add(InlineKeyboardButton("⬅ Назад", callback_data="back_main"))
    return kb

def charts_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 Активность (7д)", callback_data="activity"),
        InlineKeyboardButton("📉 Тренд MMR", callback_data="mmr_trend")
    )
    kb.add(
        InlineKeyboardButton("🎭 Винрейт по ролям", callback_data="role_wr"),
        InlineKeyboardButton("⬅ Назад", callback_data="back_main")
    )
    return kb

# ---------------- Handlers ----------------

# start / main menu
@dp.message(Command("start"))
async def cmd_start(msg: Message):
    init_db()
    user = db_get_user(msg.from_user.id)
    bound = bool(user and user.get("steam32"))
    await msg.answer("Привет! Я Dota 2 трекер — выбери действие:", reply_markup=build_main_kb(bound))

# bind steam
@dp.callback_query(lambda c: c.data == "bind")
async def cb_bind(cq: CallbackQuery):
    await cq.message.answer("Пришли Steam ID (steam32/steam64) или ссылку вида https://steamcommunity.com/profiles/7656...")
    await cq.answer()

@dp.message()
async def msg_handle(m: Message):
    """
    This message handler covers:
    - binding Steam if message looks like Steam id or url (and user clicked bind)
    - 'mmr 4321' to set exact mmr
    Otherwise ignored (we don't spam)
    """
    txt = (m.text or "").strip()
    if not txt:
        return

    # mmr input
    mmr_match = re.match(r"^\s*mmr\s*[:=]?\s*(\d{2,5})\s*$", txt, re.I) or re.match(r"^\s*mmr(\d{2,5})\s*$", txt, re.I)
    if mmr_match:
        mmr_val = int(mmr_match.group(1))
        if mmr_val <= 0 or mmr_val > 30000:
            await m.reply("Неправильное значение MMR.")
            return
        init_db()
        db_update_exact_mmr(m.from_user.id, mmr_val)
        # also update max_mmr if needed
        u = db_get_user(m.from_user.id)
        if u:
            max_mm = u.get("max_mmr") or 0
            if mmr_val > max_mm:
                with closing(sqlite3.connect(DB_PATH)) as con:
                    con.execute("UPDATE users SET max_mmr=? WHERE telegram_id=?", (mmr_val, m.from_user.id))
                    con.commit()
        await m.reply(f"✅ Точный MMR сохранён: {mmr_val}")
        return

    # steam binding detection
    if "steamcommunity.com" in txt or txt.isdigit():
        steam32 = parse_steam_any(txt)
        if steam32 is None:
            await m.reply("Не удалось распознать Steam ID. Отправь ссылку вида /profiles/7656... или числовой steam64/steam32.")
            return
        init_db()
        # verify with OpenDota
        pl = await od_player(steam32)
        if not pl or not pl.get("profile"):
            await m.reply("Профиль не найден в OpenDota. Убедись, что профиль доступен и ты входил в OpenDota ранее.")
            return
        db_set_user_steam(m.from_user.id, steam32)
        # set initial auto mmr
        rank_tier = pl.get("rank_tier")
        est = approx_mmr_from_rank_tier(rank_tier)
        if est:
            db_update_auto_mmr(m.from_user.id, est)
            db_set_last_rank_tier(m.from_user.id, rank_tier)
        await m.reply(f"✅ Привязан Steam32: {steam32}. Используй меню.", reply_markup=build_main_kb(True))
        return

    # otherwise ignore (or respond help)
    # await m.reply("Не понимаю. Для привязки Steam нажми кнопку 'Привязать Steam' или пришли 'mmr 4321' чтобы указать MMR.")

# back to main
@dp.callback_query(lambda c: c.data == "back_main")
async def cb_back_main(cq: CallbackQuery):
    user = db_get_user(cq.from_user.id)
    bound = bool(user and user.get("steam32"))
    await cq.message.edit_text("Главное меню:", reply_markup=build_main_kb(bound))
    await cq.answer()

# set mmr via button
@dp.callback_query(lambda c: c.data == "set_mmr")
async def cb_set_mmr(cq: CallbackQuery):
    await cq.message.answer("Пришли точный MMR как сообщение: <code>mmr 4321</code>", disable_web_page_preview=True)
    await cq.answer()

# STATUS
@dp.callback_query(lambda c: c.data == "status")
async def cb_status(cq: CallbackQuery):
    init_db()
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Сначала привяжи Steam (кнопка Привязать / Сменить Steam).")
        await cq.answer()
        return
    loading = await cq.message.answer("⏳ Загружаю статус...")
    steam32 = int(u["steam32"])
    # parallel fetch
    heroes_map_task = od_heroes_map()
    gm_task = od_get("/constants/game_mode")
    player_task = od_player(steam32)
    recent_task = od_recent(steam32)
    heroes_map, gm_map, player, recent = await asyncio.gather(heroes_map_task, gm_task, player_task, recent_task)
    rank_tier = player.get("rank_tier") if player else None
    rank_str = (lambda x: "—" if not x else ( "Immortal" if x//10==8 else f"{['Herald','Guardian','Crusader','Archon','Legend','Ancient','Divine','Immortal'][x//10 -1]} {x%10}"))(rank_tier)
    approx_mmr = approx_mmr_from_rank_tier(rank_tier)
    exact_mmr = u.get("exact_mmr") or None
    auto_mmr = u.get("current_mmr") or approx_mmr

    mmr_text = f"{exact_mmr} (точный)" if exact_mmr else (f"~{auto_mmr} (оценка)" if auto_mmr else "—")
    prog = mmr_progress_text(rank_tier, exact_mmr)

    last_info = "—"
    if recent and isinstance(recent, list) and len(recent)>0:
        r = recent[0]
        gm_name = game_mode_name(r.get("game_mode",-1), gm_map or {})
        # compute win properly
        ps = r.get("player_slot",0)
        radiant_win = bool(r.get("radiant_win"))
        win = "✅ Победа" if is_player_win(ps, radiant_win) else "❌ Поражение"
        last_info = f"{ts_msk(r.get('start_time'))}\n{gm_name} | {win} | {r.get('kills',0)}/{r.get('deaths',0)}/{r.get('assists',0)}\n<a href='{OPEN_DOTA}/matches/{r.get('match_id')}'>OpenDota</a>"

    text_lines = [
        "<b>🏆 Статус аккаунта</b>",
        f"👤 Ник: <b>{(player.get('profile') or {}).get('personaname','—') if player else '—'}</b>",
        f"🆔 Steam32: <b>{steam32}</b>",
        f"🏅 Ранг: <b>{rank_str}</b>",
        f"📈 MMR: <b>{mmr_text}</b>" + (f"\n🧭 {prog}" if prog else ""),
        f"🔝 Макс. MMR: <b>{u.get('max_mmr') or '—'}</b>",
        f"🕓 Последний матч:\n{last_info}"
    ]
    await loading.edit_text("\n".join(text_lines), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

# LAST ANY (single)
@dp.callback_query(lambda c: c.data == "last_games")
async def cb_last_any(cq: CallbackQuery):
    init_db()
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Привяжи Steam сначала.")
        await cq.answer()
        return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Получаю последние матчи...")
    recent = await od_recent(steam32) or []
    if not recent:
        await loading.edit_text("Нет доступных последних матчей.", reply_markup=build_main_kb(True))
        await cq.answer(); return

    # show last 10 recent (all modes) summarised
    heroes_map = await od_heroes_map() or []
    gm_map = await od_get("/constants/game_mode") or {}
    lines = ["<b>🎮 Последние 10 матчей (все режимы)</b>"]
    for i,m in enumerate(recent[:10],1):
        gm = game_mode_name(m.get("game_mode",-1), gm_map)
        hero = hero_name_from_map(m.get("hero_id"), heroes_map)
        ps = m.get("player_slot",0)
        win = "✅" if is_player_win(ps, bool(m.get("radiant_win"))) else "❌"
        kda = f"{m.get('kills',0)}/{m.get('deaths',0)}/{m.get('assists',0)} (KDA {safe_kda(m.get('kills',0),m.get('deaths',0),m.get('assists',0)):.2f})"
        # if ranked, attempt to show delta mmr if in DB
        ranked_str = ""
        if m.get("lobby_type") == 7:
            # search DB for this match
            with closing(sqlite3.connect(DB_PATH)) as con:
                con.row_factory = sqlite3.Row
                r = con.execute("SELECT delta_mmr, mmr_after FROM matches WHERE steam32=? AND match_id=?", (str(steam32), m.get("match_id"))).fetchone()
                if r and r["delta_mmr"] is not None:
                    arrow = "▲" if r["delta_mmr"]>0 else ("▼" if r["delta_mmr"]<0 else "•")
                    ranked_str = f" | {arrow} {r['delta_mmr']:+d} (MMR {r['mmr_after']})"
        lines.append(f"{i}) {ts_msk(m.get('start_time'))} — {hero} — {gm} — {win} — {kda}{ranked_str} — <a href='{OPEN_DOTA}/matches/{m.get('match_id')}'>match</a>")
    await loading.edit_text("\n".join(lines), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

# LAST 10 RANKED
@dp.callback_query(lambda c: c.data == "last_ranked")
async def cb_last_ranked(cq: CallbackQuery):
    init_db()
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Привяжи Steam сначала.")
        await cq.answer()
        return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Получаю последние рейтинговые матчи...")
    matches = await od_matches(steam32, limit=40) or []
    ranked = [m for m in matches if m.get("lobby_type")==7]
    ranked = ranked[:10]
    if not ranked:
        await loading.edit_text("Не найдено рейтинговых матчей.", reply_markup=build_main_kb(True))
        await cq.answer(); return
    lines = ["<b>🏆 Последние 10 рейтинговых матчей</b>"]
    for i,m in enumerate(ranked,1):
        gm = f"{lobby_name(m.get('lobby_type'))} | game_mode:{m.get('game_mode')}"
        ps = m.get("player_slot",0)
        win = is_player_win(ps, bool(m.get("radiant_win")))
        res = "✅ Победа" if win else "❌ Поражение"
        lines.append(f"{i}) {ts_msk(m.get('start_time'))} — {gm} — {res} — <a href='{OPEN_DOTA}/matches/{m.get('match_id')}'>match</a>")
    await loading.edit_text("\n".join(lines), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

# HEROES menu
@dp.callback_query(lambda c: c.data == "heroes_menu")
async def cb_heroes_menu(cq: CallbackQuery):
    await cq.message.answer("Выбери сортировку героев:", reply_markup=heroes_kb())
    await cq.answer()

async def render_heroes_sorted(steam32:int, sort_by:str):
    heroes_map = await od_heroes_map() or []
    stats = await od_player_heroes(steam32) or []
    rows = []
    for s in stats:
        games = s.get("games",0)
        if games <= 0: continue
        hid = s.get("hero_id")
        k = s.get("k",0); d = s.get("d",0); a = s.get("a",0)
        rows.append({
            "hero": hero_name_from_map(hid, heroes_map),
            "games": games,
            "wr": (s.get("win",0)/games*100) if games else 0.0,
            "kda": safe_kda(k,d,a)
        })
    if sort_by=="games":
        rows.sort(key=lambda x:x["games"], reverse=True)
    elif sort_by=="wr":
        rows = [r for r in rows if r["games"]>=10]
        rows.sort(key=lambda x:(x["wr"], x["games"]), reverse=True)
    else:
        rows = [r for r in rows if r["games"]>=10]
        rows.sort(key=lambda x:(x["kda"], x["games"]), reverse=True)
    return rows[:15]

@dp.callback_query(lambda c: c.data == "heroes_games")
async def cb_heroes_games(cq: CallbackQuery):
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Сначала привяжи Steam.")
        await cq.answer(); return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Загружаю статистику героев...")
    top = await render_heroes_sorted(steam32, "games")
    lines = ["🧙 <b>Топ 15 по играм</b>"]
    for i,h in enumerate(top,1):
        lines.append(f"{i}) {h['hero']} — игр: {h['games']}, WR: {h['wr']:.0f}%, KDA: {h['kda']:.2f}")
    await loading.edit_text("\n".join(lines), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

@dp.callback_query(lambda c: c.data == "heroes_wr")
async def cb_heroes_wr(cq: CallbackQuery):
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Сначала привяжи Steam.")
        await cq.answer(); return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Загружаю статистику героев...")
    top = await render_heroes_sorted(steam32, "wr")
    lines = ["🧙 <b>Топ 15 по винрейту (≥10 игр)</b>"]
    for i,h in enumerate(top,1):
        lines.append(f"{i}) {h['hero']} — игр: {h['games']}, WR: {h['wr']:.0f}%, KDA: {h['kda']:.2f}")
    await loading.edit_text("\n".join(lines), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

@dp.callback_query(lambda c: c.data == "heroes_kda")
async def cb_heroes_kda(cq: CallbackQuery):
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Сначала привяжи Steam.")
        await cq.answer(); return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Загружаю статистику героев...")
    top = await render_heroes_sorted(steam32, "kda")
    lines = ["🧙 <b>Топ 15 по KDA (≥10 игр)</b>"]
    for i,h in enumerate(top,1):
        lines.append(f"{i}) {h['hero']} — игр: {h['games']}, KDA: {h['kda']:.2f}, WR: {h['wr']:.0f}%")
    await loading.edit_text("\n".join(lines), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

@dp.callback_query(lambda c: c.data == "heroes_analytics")
async def cb_heroes_analytics(cq: CallbackQuery):
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Сначала привяжи Steam.")
        await cq.answer(); return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Формирую аналитику героев...")
    agg = db_hero_aggregates(str(steam32))
    heroes_map = await od_heroes_map() or []
    # WR top (>=10)
    wrs = []
    for a in agg:
        g = a["games"]; w = a["wins"]
        if g >= 10:
            wrs.append((a["hero_id"], g, w, (w/g)*100, a["avg_nw"]))
    wrs.sort(key=lambda x:(x[3], x[1]), reverse=True)
    text = ["🏅 <b>Топ по винрейту (≥10 игр)</b>"]
    for i, item in enumerate(wrs[:10],1):
        hid, g, w, wrp, nw = item
        text.append(f"{i}) {hero_name_from_map(hid, heroes_map)} — WR {wrp:.0f}% ({g} игр)")
    # Networth top (>=5)
    nwlist = [ (a["hero_id"], a["games"], a["avg_nw"]) for a in agg if a["games"]>=5 ]
    nwlist.sort(key=lambda x:x[2], reverse=True)
    text += ["", "💰 <b>Топ по среднему Net Worth (≥5 игр)</b>"]
    for i, item in enumerate(nwlist[:10],1):
        hid,g,nw = item
        text.append(f"{i}) {hero_name_from_map(hid, heroes_map)} — NW {nw:.0f} (игр: {g})")
    await loading.edit_text("\n".join(text), disable_web_page_preview=True, reply_markup=build_main_kb(True))
    await cq.answer()

# ACTIVITY chart (7 days)
@dp.callback_query(lambda c: c.data == "activity")
async def cb_activity(cq: CallbackQuery):
    init_db()
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Привяжи Steam сначала.")
        await cq.answer(); return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Строю активность (7 дней)...")
    recent = await od_recent(steam32) or []
    # build 7 days list (MSK)
    today_msk = (datetime.utcnow() + timedelta(hours=MSK_OFFSET)).date()
    days = [(today_msk - timedelta(days=i)) for i in range(6,-1,-1)]
    counts = {d:0 for d in days}
    for m in recent:
        ts = datetime.utcfromtimestamp(m.get("start_time",0)) + timedelta(hours=MSK_OFFSET)
        d = ts.date()
        if d in counts: counts[d] += 1
    xs = [d.strftime("%d.%m") for d in days]
    ys = [counts[d] for d in days]
    fig, ax = plt.subplots(figsize=(7,3))
    ax.bar(xs, ys)
    ax.set_title("Активность: игр в день (последние 7 дн.)")
    ax.set_xlabel("День"); ax.set_ylabel("Игры")
    ax.grid(axis='y', alpha=0.3)
    tmpf = f"/tmp/activity_{cq.from_user.id}.png"
    fig.savefig(tmpf, bbox_inches='tight'); plt.close(fig)
    total = sum(ys); avg = total/7.0
    cap = f"📈 Активность за 7 дн.\n• Всего игр: {total}\n• В среднем/день: {avg:.1f}"
    await bot.send_photo(cq.from_user.id, FSInputFile(tmpf), caption=cap)
    try: os.remove(tmpf)
    except: pass
    await loading.delete()
    await cq.answer()

# MMR TREND chart
@dp.callback_query(lambda c: c.data == "mmr_trend")
async def cb_mmr_trend(cq: CallbackQuery):
    init_db()
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Привяжи Steam сначала.")
        await cq.answer(); return
    steam32 = int(u["steam32"])
    loading = await cq.message.answer("⏳ Строю тренд MMR по последним ranked...")
    matches = await od_matches(steam32, limit=60) or []
    ranked = [m for m in matches if m.get("lobby_type")==7]
    ranked = ranked[::-1]  # old -> new for plotting
    if not ranked:
        await loading.edit_text("Недостаточно ранк-матчей для тренда.", reply_markup=build_main_kb(True))
        await cq.answer(); return
    # starting point: exact mmr if present else approx from rank tier or 0
    exact = u.get("exact_mmr")
    if exact is not None:
        cur = exact
    else:
        # try current_mmr or approx
        cur = u.get("current_mmr") or approx_mmr_from_rank_tier((await od_player(int(u["steam32"]))).get("rank_tier") if await od_player(int(u["steam32"])) else None) or 0
    xs = []; ys = []
    tmpcur = cur
    for i,m in enumerate(ranked, start=1):
        win = is_player_win(m.get("player_slot",0), bool(m.get("radiant_win")))
        delta = ASSUMED_MMR_DELTA if win else -ASSUMED_MMR_DELTA
        tmpcur = tmpcur + delta
        xs.append(i); ys.append(tmpcur)
    fig, ax = plt.subplots(figsize=(7,3))
    ax.plot(xs, ys, marker='o')
    ax.set_title("Тренд условного MMR (последние ранк)")
    ax.set_xlabel("Матч"); ax.set_ylabel("MMR")
    ax.grid(alpha=0.3)
    tmpf = f"/tmp/mmr_{cq.from_user.id}.png"
    fig.savefig(tmpf, bbox_inches='tight'); plt.close(fig)
    cap = f"📉 Тренд MMR (условный). Точка старта: {cur}"
    await bot.send_photo(cq.from_user.id, FSInputFile(tmpf), caption=cap)
    try: os.remove(tmpf)
    except: pass
    await loading.delete()
    await cq.answer()

# ROLE WR
@dp.callback_query(lambda c: c.data == "role_wr")
async def cb_role_wr(cq: CallbackQuery):
    init_db()
    u = db_get_user(cq.from_user.id)
    if not u or not u.get("steam32"):
        await cq.message.answer("Привяжи Steam сначала.")
        await cq.answer(); return
    stats = db_role_wr(str(u["steam32"]))
    core = stats["core"]; sup = stats["support"]
    core_wr = round(100*core["w"]/core["g"]) if core["g"] else 0
    sup_wr  = round(100*sup["w"]/sup["g"]) if sup["g"] else 0
    text = f"🎭 <b>Винрейт по ролям</b>\n• Core: игр {core['g']}, побед {core['w']}, WR {core_wr}%\n• Support: игр {sup['g']}, побед {sup['w']}, WR {sup_wr}%"
    await cq.message.answer(text, parse_mode="HTML")
    await cq.answer()

# ---------------- Background: Polling + Daily Summary ----------------
async def send_match_card(to_tg:int, heroes_map:List[dict], m:dict, mmr_after:Optional[int], delta:Optional[int]):
    hero = hero_name_from_map(m.get("hero_id"), heroes_map)
    win = is_player_win(m.get("player_slot",0), bool(m.get("radiant_win")))
    res = "✅ Победа" if win else "❌ Поражение"
    kdastr = f"{m.get('kills',0)}/{m.get('deaths',0)}/{m.get('assists',0)} (KDA {safe_kda(m.get('kills',0),m.get('deaths',0),m.get('assists',0)):.2f})"
    when = ts_msk(m.get("start_time",0))
    dur = fmt_duration(m.get("duration",0))
    mode_text = f"{lobby_name(m.get('lobby_type'))} | game_mode:{m.get('game_mode')}"
    mmr_line = ""
    if m.get("lobby_type")==7 and mmr_after is not None and delta is not None:
        arrow = "▲" if delta>0 else ("▼" if delta<0 else "•")
        mmr_line = f"\n📈 ΔMMR: {arrow} {delta:+d}\n📊 Текущий: <b>{mmr_after}</b>"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Открыть на OpenDota", url=f"{OPEN_DOTA}/matches/{m.get('match_id')}")]
    ])
    text = (
        f"🎮 <b>Новая игра</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {when}\n"
        f"🧩 {mode_text}\n"
        f"🧙 Герой: <b>{hero}</b>\n"
        f"⚔️ {kdastr} • ⏱ {dur}\n"
        f"🏆 Итог: {res}"
        f"{mmr_line}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    try:
        await bot.send_message(to_tg, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.exception("Failed to send match card to %s: %s", to_tg, e)

async def poll_worker():
    init_db()
    await asyncio.sleep(3)
    while True:
        try:
            users = db_get_all_users_with_steam()
            if not users:
                await asyncio.sleep(POLL_INTERVAL); continue
            async with aiohttp.ClientSession() as sess:
                heroes_map = await od_heroes_map()
                for u in users:
                    try:
                        tg = u["telegram_id"]
                        steam32 = int(u["steam32"])
                        # fetch last match any
                        matches = await od_matches(steam32, limit=1, params={})
                        if not matches:
                            continue
                        m = matches[0]
                        last_any_db = u.get("last_any_match")
                        if last_any_db != m.get("match_id"):
                            # new match for this user
                            # get details to determine networth/gpm/purchases
                            detail = await od_match_detail(m.get("match_id"))
                            nw = None; gpm=None; role="core"
                            if detail and "players" in detail:
                                for p in detail["players"]:
                                    if p.get("account_id") == steam32:
                                        nw = p.get("net_worth"); gpm = p.get("gold_per_min")
                                        purchases = [it.get("key","") for it in p.get("purchase_log", [])]
                                        role = guess_role_from_purchase_and_gpm(purchases, gpm or 0)
                                        break
                            # mmr delta only for ranked
                            delta=None; mmr_after=None
                            if m.get("lobby_type")==7:
                                # determine base current mmr (exact preferred)
                                dbu = db_get_user(tg)
                                effective = dbu.get("exact_mmr") if dbu.get("exact_mmr") is not None else dbu.get("current_mmr")
                                if isinstance(effective, int):
                                    win = is_player_win(m.get("player_slot",0), bool(m.get("radiant_win")))
                                    delta = ASSUMED_MMR_DELTA if win else -ASSUMED_MMR_DELTA
                                    mmr_after = effective + delta
                                    # update in DB: if user has exact_mmr set, modify it (we assume it's 'current state'), else update auto current_mmr
                                    if dbu.get("exact_mmr") is not None:
                                        db_update_exact_mmr(tg, mmr_after)
                                    else:
                                        db_update_auto_mmr(tg, mmr_after)
                            # store match
                            db_upsert_match(str(steam32), m, nw, gpm, delta, mmr_after)
                            # update last ids
                            db_set_last_ids(tg, any_id=m.get("match_id"))
                            # send notification
                            await send_match_card(tg, heroes_map or [], m, mmr_after, delta)
                            # streak notifications (calculate)
                            streak = calc_streak_for_user(str(steam32))
                            if streak >= STREAK_NOTIFY_WIN:
                                await bot.send_message(tg, f"🔥 Винстрик: {streak} побед подряд!")
                            if streak <= -STREAK_NOTIFY_LOSE:
                                await bot.send_message(tg, f"💀 Лузстрик: {-streak} поражений подряд.")
                        # handle ranked separately for last_ranked id
                        ranked_matches = await od_matches(steam32, limit=1, params={"lobby_type":7})
                        if ranked_matches:
                            rid = ranked_matches[0].get("match_id")
                            if u.get("last_ranked_match") != rid:
                                db_set_last_ids(tg, ranked_id=rid)
                    except Exception as e:
                        logger.exception("Error handling user %s in poll_worker: %s", u, e)
            await asyncio.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.exception("poll_worker crashed: %s", e)
            await asyncio.sleep(10)

# streak calc helper
def calc_streak_for_user(steam32:str) -> int:
    # positive for winning streak, negative for losing streak
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT radiant_win, player_slot FROM matches WHERE steam32=? ORDER BY start_time DESC LIMIT 50", (steam32,)).fetchall()
    if not rs: return 0
    streak = 0; last_win=None
    for r in rs:
        win = ((r["player_slot"]<128) and (r["radiant_win"]==1)) or ((r["player_slot"]>=128) and (r["radiant_win"]==0))
        if last_win is None:
            last_win = win; streak = 1
        elif win == last_win:
            streak += 1
        else:
            break
    return streak if last_win else -streak if streak else 0

# daily summary worker (sends report to all users at 23:59 MSK)
def seconds_until_daily():
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc + timedelta(hours=MSK_OFFSET)
    target = now_msk.replace(hour=DAILY_REPORT_HOUR, minute=DAILY_REPORT_MINUTE, second=0, microsecond=0)
    if target <= now_msk:
        target += timedelta(days=1)
    return int((target - now_msk).total_seconds())

async def daily_worker():
    await asyncio.sleep(5)
    while True:
        try:
            wait = seconds_until_daily()
            logger.info("Daily worker sleeping %s seconds", wait)
            await asyncio.sleep(wait)
            users = db_get_all_users_with_steam()
            if not users:
                continue
            # determine today's start/end in UTC timestamps
            now_utc = datetime.now(timezone.utc)
            now_msk = now_utc + timedelta(hours=MSK_OFFSET)
            start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
            start_utc = start_msk - timedelta(hours=MSK_OFFSET)
            start_ts = int(start_utc.timestamp())
            end_ts = int(now_utc.timestamp())
            for u in users:
                try:
                    steam32 = u["steam32"]
                    tg = u["telegram_id"]
                    # get matches today (from OpenDota)
                    arr = await od_matches(int(steam32), limit=200) or []
                    today = [m for m in arr if start_ts <= m.get("start_time",0) <= end_ts]
                    games = len(today)
                    wins = sum(1 for m in today if is_player_win(m.get("player_slot",0), bool(m.get("radiant_win"))))
                    loses = games - wins
                    wr = round(100*wins/games) if games else 0
                    dm = db_sum_delta_mmr_today(str(steam32), start_ts, end_ts)
                    eff = (u.get("exact_mmr") if u.get("exact_mmr") is not None else u.get("current_mmr"))
                    text = (
                        "📊 <b>Итоги дня</b>\n"
                        f"• Игр: <b>{games}</b>\n"
                        f"• Победы/Поражения: <b>{wins}</b>/<b>{loses}</b> (WR <b>{wr}%</b>)\n"
                        f"• Δ MMR (ranked): <b>{dm:+d}</b>\n"
                        f"• Текущий рейтинг: <b>{eff if eff is not None else '—'}</b>\n"
                    )
                    if games == 0:
                        text += "\n• Сегодня ты не играл — удачи завтра! ✨"
                    await bot.send_message(tg, text, parse_mode="HTML")
                except Exception as e:
                    logger.exception("daily_worker user send failed: %s", e)
        except Exception as e:
            logger.exception("daily_worker crashed: %s", e)
            await asyncio.sleep(30)

# ---------------- Startup ----------------
async def on_startup(dispatcher: Dispatcher):
    logger.info("Bot started, initializing DB and tasks")
    init_db()
    # start background tasks
    asyncio.create_task(poll_worker())
    asyncio.create_task(daily_worker())

if __name__ == "__main__":
    try:
        from aiogram import executor
        executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
    except KeyboardInterrupt:
        logger.info("Stopping bot by KeyboardInterrupt")
