import os
import re
import math
import asyncio
import logging
import sqlite3
from contextlib import closing
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ========= ENV / CONFIG =========
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or "PASTE_YOUR_TOKEN"
OPEN_DOTA = "https://api.opendota.com/api"
DB_PATH = os.getenv("DB_PATH", "data.db")

POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL", "60"))
MSK_OFFSET_HOURS = 3  # UTC+3
ASSUMED_MMR_DELTA = 30  # эвристика дельты MMR за ranked (если нет точных данных)

# нотификации
STREAK_NOTIFY_WIN = 5     # винстрик N+
STREAK_NOTIFY_LOSE = 5    # лузстрик N+

if not BOT_TOKEN or BOT_TOKEN == "PASTE_YOUR_TOKEN":
    raise SystemExit("Set BOT_TOKEN env variable with your Telegram bot token.")

logging.basicConfig(level=logging.INFO)
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# ========= DB =========
def db_init():
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            steam32 TEXT,
            current_mmr INTEGER,       -- авто/MMR-оценка (rank_tier + дельты)
            user_set_mmr INTEGER,      -- точный MMR, если пользователь указал вручную
            max_mmr INTEGER,
            last_any_match_id INTEGER,
            last_ranked_match_id INTEGER,
            last_rank_tier INTEGER,    -- для детекта rank up/down
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            steam32 TEXT,
            match_id INTEGER,
            start_time INTEGER,
            duration INTEGER,
            hero_id INTEGER,
            k INTEGER, d INTEGER, a INTEGER,
            lobby_type INTEGER,
            game_mode INTEGER,
            radiant_win INTEGER,
            player_slot INTEGER,
            net_worth INTEGER,
            gpm INTEGER,
            role TEXT,                 -- 'core'/'support' (эвристика)
            delta_mmr INTEGER,
            mmr_after INTEGER,
            PRIMARY KEY (steam32, match_id)
        )
        """)
        con.commit()

def db_get_user(tg_id: int) -> Optional[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        r = con.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,)).fetchone()
        return dict(r) if r else None

def db_set_user_steam(tg_id: int, steam32: str):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("""
        INSERT INTO users (telegram_id, steam32)
        VALUES (?,?)
        ON CONFLICT(telegram_id) DO UPDATE SET steam32=excluded.steam32
        """, (tg_id, steam32))
        con.commit()

def db_update_auto_mmr(tg_id: int, new_mmr: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        if new_mmr is None:
            con.execute("UPDATE users SET current_mmr=NULL WHERE telegram_id=?", (tg_id,))
        else:
            con.execute("""
            UPDATE users
            SET current_mmr=?, max_mmr=MAX(COALESCE(max_mmr,0), ?)
            WHERE telegram_id=?
            """, (new_mmr, new_mmr, tg_id))
        con.commit()

def db_set_user_mmr(tg_id: int, mmr: int):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("""
        UPDATE users
        SET user_set_mmr=?, max_mmr=MAX(COALESCE(max_mmr,0), ?)
        WHERE telegram_id=?
        """, (mmr, mmr, tg_id))
        con.commit()

def db_clear_user_mmr(tg_id: int):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("UPDATE users SET user_set_mmr=NULL WHERE telegram_id=?", (tg_id,))
        con.commit()

def db_set_last_match_ids(tg_id: int, any_id: Optional[int]=None, ranked_id: Optional[int]=None):
    with closing(sqlite3.connect(DB_PATH)) as con:
        if any_id is not None:
            con.execute("UPDATE users SET last_any_match_id=? WHERE telegram_id=?", (any_id, tg_id))
        if ranked_id is not None:
            con.execute("UPDATE users SET last_ranked_match_id=? WHERE telegram_id=?", (ranked_id, tg_id))
        con.commit()

def db_set_last_rank_tier(tg_id: int, tier: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("UPDATE users SET last_rank_tier=? WHERE telegram_id=?", (tier, tg_id))
        con.commit()

def effective_mmr(u: dict) -> Optional[int]:
    # приоритет ручного MMR, затем авто
    return u.get("user_set_mmr") if u.get("user_set_mmr") is not None else u.get("current_mmr")

def db_upsert_match(steam32: str, m: dict, net_worth: Optional[int],
                    gpm: Optional[int], role: str,
                    delta_mmr: Optional[int], mmr_after: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("""
        INSERT INTO matches (steam32, match_id, start_time, duration, hero_id, k, d, a,
                             lobby_type, game_mode, radiant_win, player_slot, net_worth,
                             gpm, role, delta_mmr, mmr_after)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(steam32, match_id) DO UPDATE SET
            start_time=excluded.start_time,
            duration=excluded.duration,
            hero_id=excluded.hero_id,
            k=excluded.k, d=excluded.d, a=excluded.a,
            lobby_type=excluded.lobby_type,
            game_mode=excluded.game_mode,
            radiant_win=excluded.radiant_win,
            player_slot=excluded.player_slot,
            net_worth=excluded.net_worth,
            gpm=excluded.gpm,
            role=excluded.role,
            delta_mmr=excluded.delta_mmr,
            mmr_after=excluded.mmr_after
        """, (
            steam32,
            m.get("match_id"), m.get("start_time"), m.get("duration"), m.get("hero_id"),
            m.get("kills",0), m.get("deaths",0), m.get("assists",0),
            m.get("lobby_type"), m.get("game_mode"), int(bool(m.get("radiant_win"))),
            m.get("player_slot"), net_worth, gpm, role, delta_mmr, mmr_after
        ))
        con.commit()

def db_last_matches(steam32: str, limit: int=10) -> List[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("""
        SELECT * FROM matches WHERE steam32=?
        ORDER BY start_time DESC LIMIT ?
        """, (steam32, limit)).fetchall()
        return [dict(r) for r in rs]

def db_role_wr(steam32: str) -> Dict[str, Dict[str,int]]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("""
        SELECT role, radiant_win, player_slot FROM matches
        WHERE steam32=? AND role IS NOT NULL
        """, (steam32,)).fetchall()
    stat = {"core":{"g":0,"w":0}, "support":{"g":0,"w":0}}
    for r in rs:
        role = r["role"]
        if role not in stat: continue
        win = ((r["player_slot"]<128) and (r["radiant_win"]==1)) or \
              ((r["player_slot"]>=128) and (r["radiant_win"]==0))
        stat[role]["g"] += 1
        if win: stat[role]["w"] += 1
    return stat

def db_hero_agg(steam32: str) -> List[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("""
        SELECT hero_id,
               COUNT(*) games,
               SUM(CASE WHEN ((player_slot<128 AND radiant_win=1) OR (player_slot>=128 AND radiant_win=0)) THEN 1 ELSE 0 END) wins,
               AVG(COALESCE(net_worth,0)) avg_nw
        FROM matches
        WHERE steam32=?
        GROUP BY hero_id
        """, (steam32,)).fetchall()
        return [dict(r) for r in rs]

def db_calc_streak_dir(steam32: str) -> int:
    """>0 — винстрик длиной N, <0 — лузстрик длиной N"""
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("""
        SELECT radiant_win, player_slot FROM matches
        WHERE steam32=? ORDER BY start_time DESC LIMIT 50
        """, (steam32,)).fetchall()
    cnt = 0
    last_win = None
    for r in rs:
        win = ((r["player_slot"] < 128) and (r["radiant_win"] == 1)) or \
              ((r["player_slot"] >= 128) and (r["radiant_win"] == 0))
        if last_win is None:
            last_win = win
            cnt = 1
        elif win == last_win:
            cnt += 1
        else:
            break
    return cnt if (last_win is True) else (-cnt if cnt else 0)

# ========= HELPERS =========
STEAM_PROFILE_RE = re.compile(r"(?:https?://)?steamcommunity\.com/(?:id|profiles)/([^/\s]+)", re.I)
STEAM64_OFFSET = 76561197960265728

def steam_any_to_steam32(value: str) -> Optional[str]:
    s = (value or "").strip()
    m = STEAM_PROFILE_RE.search(s)
    if m:
        part = m.group(1)
        if part.isdigit() and len(part) >= 16:
            return str(int(part) - STEAM64_OFFSET)
        return None  # vanity /id/<name> без Steam Web API не резолвим
    if not s.isdigit(): return None
    if len(s) >= 16:
        return str(int(s) - STEAM64_OFFSET)
    return s

def fmt_duration(sec: int) -> str:
    sec = int(max(0, sec or 0))
    mm, ss = divmod(sec, 60)
    hh, mm = divmod(mm, 60)
    return f"{hh}:{mm:02d}:{ss:02d}" if hh else f"{mm}:{ss:02d}"

def ts_msk_str(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=MSK_OFFSET_HOURS)
    return dt.strftime("%d.%m.%Y %H:%M МСК")

def is_win(player_slot: int, radiant_win: bool) -> bool:
    rad = player_slot < 128
    return (rad and radiant_win) or ((not rad) and (not radiant_win))

def kda_str(k: int, d: int, a: int) -> str:
    ratio = (k + a) / max(1, d)
    return f"{k}/{d}/{a} (KDA {ratio:.2f})"

def rank_name(rank_tier: Optional[int]) -> str:
    if not isinstance(rank_tier, int): return "—"
    names = {1:"Herald",2:"Guardian",3:"Crusader",4:"Archon",5:"Legend",6:"Ancient",7:"Divine",8:"Immortal"}
    return f"{names.get(rank_tier//10,'?')} {rank_tier%10}"

def mmr_from_rank_tier(rank_tier: Optional[int]) -> Optional[int]:
    if not isinstance(rank_tier, int): return None
    base = {1:0,2:560,3:1260,4:1960,5:2660,6:3360,7:4060,8:5200}
    major = rank_tier // 10
    minor = rank_tier % 10
    if major not in base: return None
    return base[major] + (minor-1)*140 if minor>=1 else base[major]

def next_star_need(rank_tier: Optional[int]) -> Optional[int]:
    cur = mmr_from_rank_tier(rank_tier)
    return 140 if cur is not None else None

def lobby_name(lobby_type: Optional[int]) -> str:
    table = {0:"Unranked",1:"Practice",2:"Tournament",3:"Tutorial",4:"Co-op Bots",
             5:"Ranked Team",6:"Ranked Solo",7:"Ranked",8:"1v1 Mid",9:"Battle Cup"}
    return table.get(lobby_type, "Custom/Unknown")

def game_mode_name(game_mode: Optional[int]) -> str:
    table = {1:"All Pick",2:"Captains Mode",3:"Random Draft",4:"Single Draft",5:"All Random",
             12:"Least Played",13:"Limited Heroes",14:"Compendium",15:"Custom",16:"Captains Draft",
             17:"Balanced Draft",18:"Ability Draft",19:"Event",20:"ARDM",21:"1v1 Mid",22:"All Draft",
             23:"Turbo",24:"Mutation",25:"Coaches Challenge"}
    return table.get(game_mode, "Unknown")

def guess_role(purchases: List[str], gpm: int) -> str:
    core_items = {"bkb","manta","daedalus","skadi","desolator","battle_fury","butterfly","radiance","satanic"}
    sup_items  = {"mekansm","glimmer_cape","force_staff","guardian_greaves","solar_crest","lotus_orb","pipe","urn_of_shadows","spirit_vessel"}
    s = set(purchases or [])
    if any(x in s for x in core_items) or gpm >= 450:
        return "core"
    if any(x in s for x in sup_items) or gpm <= 350:
        return "support"
    return "core"  # дефолт

# ========= OPEN DOTA =========
async def od_get(session: aiohttp.ClientSession, path: str, params: dict=None) -> Any:
    async with session.get(f"{OPEN_DOTA}{path}", params=params, timeout=25) as r:
        if r.status == 404:
            return None
        r.raise_for_status()
        return await r.json()

async def fetch_player(session, steam32: str) -> Optional[dict]:
    return await od_get(session, f"/players/{steam32}")

async def fetch_player_wl(session, steam32: str) -> Optional[dict]:
    return await od_get(session, f"/players/{steam32}/wl")

async def fetch_player_heroes(session, steam32: str) -> Optional[List[dict]]:
    return await od_get(session, f"/players/{steam32}/heroes")

async def fetch_heroes_map(session) -> Dict[int, str]:
    arr = await od_get(session, "/heroes") or []
    return {h["id"]: h["localized_name"] for h in arr}

async def fetch_last_matches(session, steam32: str, limit: int=10, ranked_only: bool=False) -> List[dict]:
    params = {"limit": limit}
    if ranked_only:
        params["lobby_type"] = 7
    return await od_get(session, f"/players/{steam32}/matches", params=params) or []

async def fetch_match_detail(session, match_id: int) -> Optional[dict]:
    return await od_get(session, f"/matches/{match_id}")

# ========= KEYBOARDS =========
def main_menu(bound: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🏆 Статус", callback_data="status"),
         InlineKeyboardButton(text="🎮 Последние 10 матчей", callback_data="last10")],
        [InlineKeyboardButton(text="🧙 Герои", callback_data="heroes_menu"),
         InlineKeyboardButton(text="📈 Графики", callback_data="charts_menu")],
        [InlineKeyboardButton(text=("🔁 Сменить аккаунт" if bound else "🔗 Привязать аккаунт"),
                              callback_data="bind")],
        [InlineKeyboardButton(text="🤖 Совет по сборке (последний матч)", callback_data="ai_last")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def heroes_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔝 По играм", callback_data="heroes_sort_games"),
         InlineKeyboardButton(text="✅ По винрейту (≥10 игр)", callback_data="heroes_sort_wr")],
        [InlineKeyboardButton(text="⚔ По KDA (≥10 игр)", callback_data="heroes_sort_kda")],
        [InlineKeyboardButton(text="🧠 Топ героев по WR/NetWorth", callback_data="heroes_analytics")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back_main")]
    ])

def charts_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📈 Активность (7 дней)", callback_data="activity")],
        [InlineKeyboardButton(text="📉 Тренд MMR", callback_data="mmr_trend")],
        [InlineKeyboardButton(text="🎭 Винрейт по ролям", callback_data="role_wr")],
        [InlineKeyboardButton(text="⚙ Указать точный MMR", callback_data="set_mmr")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back_main")]
    ])

# ========= BOT =========
@dp.message(Command("start"))
async def on_start(m: Message):
    db_init()
    u = db_get_user(m.from_user.id)
    await m.answer(
        "👋 Привет! Я трекер Dota 2: статус, матчи, герои, графики, подсказки и уведомления.\n"
        "Начни с привязки аккаунта.",
        reply_markup=main_menu(bool(u and u.get("steam32")))
    )

@dp.callback_query(F.data == "back_main")
async def back_main(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    await cb.message.edit_text("Главное меню:", reply_markup=main_menu(bool(u and u.get("steam32"))))
    await cb.answer()

# ----- Привязка -----
@dp.callback_query(F.data == "bind")
async def on_bind(cb: CallbackQuery):
    await cb.message.answer(
        "🔗 Пришли свой Steam:\n"
        "• Steam32 / Steam64\n"
        "• или ссылку на профиль вида https://steamcommunity.com/profiles/XXXXXXXXXXXXXXX\n"
        "(vanity /id/<name> не поддерживается)"
    )
    await cb.answer()

@dp.message()
async def bind_or_setmmr(m: Message):
    text = (m.text or "").strip()
    if not text:
        return

    # обработка ввода MMR (режим ожидания помечаем простым флагом user state в SQLite? проще через метку файла)
    # Упростим: командный формат "mmr 4321"
    if text.lower().startswith("mmr "):
        try:
            val = int(text.split()[1])
            if val <= 0 or val > 15000:
                raise ValueError
            db_set_user_mmr(m.from_user.id, val)
            await m.answer(f"✅ Точный MMR сохранён: {val}")
            return
        except Exception:
            await m.answer("❌ Формат: <code>mmr 4321</code>", parse_mode="HTML")
            return

    # привязка аккаунта
    if (text.isdigit() or "steamcommunity.com" in text):
        steam32 = steam_any_to_steam32(text)
        if not steam32:
            await m.answer("❌ Не удалось определить Steam ID. Пришли Steam64 или ссылку с /profiles/.")
            return

        async with aiohttp.ClientSession() as sess:
            player = await fetch_player(sess, steam32)
        if not player or not player.get("profile"):
            await m.answer("❌ Профиль не найден в OpenDota. Авторизуйся на opendota.com через Steam и открой игровой профиль в Dota 2.")
            return

        db_set_user_steam(m.from_user.id, steam32)
        # первичное авто-MMR по рангу
        rank_tier = player.get("rank_tier")
        est = mmr_from_rank_tier(rank_tier)
        db_update_auto_mmr(m.from_user.id, est)
        db_set_last_rank_tier(m.from_user.id, rank_tier)

        await m.answer("✅ Аккаунт привязан! Советы: можешь прислать «<code>mmr 4321</code>» для точности.", parse_mode="HTML",
                       reply_markup=main_menu(True))

# ----- Меню графиков -----
@dp.callback_query(F.data == "charts_menu")
async def charts_menu(cb: CallbackQuery):
    await cb.message.answer("Выбери действие:", reply_markup=charts_keyboard()); await cb.answer()

# ----- Указать точный MMR -----
@dp.callback_query(F.data == "set_mmr")
async def on_set_mmr(cb: CallbackQuery):
    await cb.message.answer("✍ Пришли сообщением в чат: <code>mmr 4321</code>", parse_mode="HTML")
    await cb.answer()

# ----- Статус -----
@dp.callback_query(F.data == "status")
async def on_status(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]

    async with aiohttp.ClientSession() as sess:
        player = await fetch_player(sess, steam32)
        wl = await fetch_player_wl(sess, steam32)

    prof = (player or {}).get("profile") or {}
    rank_tier = player.get("rank_tier")
    rank = rank_name(rank_tier)
    auto_mmr = mmr_from_rank_tier(rank_tier)
    if auto_mmr is not None:
        db_update_auto_mmr(cb.from_user.id, auto_mmr)  # обновим авто-оценку
    need_star = next_star_need(rank_tier)
    plus = bool(prof.get("plus"))
    streak_dir = db_calc_streak_dir(steam32)
    streak_text = f"{abs(streak_dir)} {'побед' if streak_dir>0 else 'поражений'} подряд" if streak_dir else "—"
    max_mmr = u.get("max_mmr") or auto_mmr
    last_any = u.get("last_any_match_id")
    eff_mmr = effective_mmr(db_get_user(cb.from_user.id))

    lines = [
        "🏆 <b>Статус аккаунта</b>",
        f"👤 Ник: <b>{prof.get('personaname','—')}</b>",
        f"🆔 Steam32: <b>{steam32}</b>",
        f"🏅 Ранг: <b>{rank}</b>",
        f"📈 MMR: <b>{eff_mmr if eff_mmr is not None else '—'}</b>" + (f" | до след.★: <b>{need_star} MMR</b>" if need_star else ""),
        f"💛 Dota Plus: <b>{'Да' if plus else 'Нет'}</b>",
        f"🔥 Серия: <b>{streak_text}</b>",
        f"🔝 Макс. MMR: <b>{max_mmr if max_mmr is not None else '—'}</b>",
        f"🔗 OpenDota: <a href='https://www.opendota.com/players/{steam32}'>профиль</a>"
    ]
    if last_any:
        lines.append(f"🕓 Последний матч: <a href='https://www.opendota.com/matches/{last_any}'>#{last_any}</a>")

    await cb.message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()

# ----- Последние 10 матчей -----
@dp.callback_query(F.data == "last10")
async def on_last10(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]

    rows = db_last_matches(steam32, 10)
    if not rows:
        # если пусто — подтянем 10 матчей сейчас
        async with aiohttp.ClientSession() as sess:
            arr = await fetch_last_matches(sess, steam32, 10, ranked_only=False)
            heroes_map = await fetch_heroes_map(sess)
            for m in arr:
                detail = await fetch_match_detail(sess, m["match_id"])
                nw = gpm = None
                role = "core"
                if detail and "players" in detail:
                    you = next((p for p in detail["players"] if p.get("account_id")==int(steam32)), None)
                    if you:
                        nw = you.get("net_worth")
                        gpm = you.get("gold_per_min")
                        purchases = [log.get("key","") for log in you.get("purchase_log", [])]
                        role = guess_role(purchases, gpm or 0)
                db_upsert_match(steam32, m, nw, gpm, role, None, None)
        rows = db_last_matches(steam32, 10)

    lines = ["🎮 <b>Последние 10 матчей</b> (все режимы)"]
    for i, r in enumerate(rows, 1):
        mode = f"{lobby_name(r['lobby_type'])} | {game_mode_name(r['game_mode'])}"
        win = is_win(r["player_slot"], bool(r["radiant_win"]))
        flag = "✅" if win else "❌"
        kdastr = kda_str(r["k"], r["d"], r["a"])
        mmr_part = ""
        if r["lobby_type"] == 7 and r["delta_mmr"] is not None and r["mmr_after"] is not None:
            arrow = "▲" if r["delta_mmr"] > 0 else "▼" if r["delta_mmr"] < 0 else "•"
            mmr_part = f" | {arrow} {r['delta_mmr']:+d} (MMR: {r['mmr_after']})"
        lines.append(
            f"{i}) {flag} {mode} | {kdastr} | <a href='https://www.opendota.com/matches/{r['match_id']}'>match</a>{mmr_part}"
        )
    await cb.message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()

# ----- Герои (сортировки) -----
def kda_calc(k,d,a) -> float:
    return round((k+a)/max(1,d), 2)

async def render_heroes(cb: CallbackQuery, sort_by: str):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]

    async with aiohttp.ClientSession() as sess:
        heroes_map = await fetch_heroes_map(sess)
        data = await fetch_player_heroes(sess, steam32)

    if not data:
        await cb.message.answer("Нет данных по героям (возможен приватный профиль)."); await cb.answer(); return

    rows = []
    for it in data:
        games = it.get("games",0) or 0
        wins = it.get("win",0) or 0
        k = it.get("k",0) or 0
        d = it.get("d",0) or 0
        a = it.get("a",0) or 0
        wr = (wins/games*100) if games else 0.0
        rows.append({
            "hero": heroes_map.get(it.get("hero_id"), f"Hero {it.get('hero_id')}"),
            "games": games, "wins": wins, "wr": wr, "kda": kda_calc(k,d,a)
        })

    if sort_by == "games":
        rows.sort(key=lambda x: x["games"], reverse=True)
        header = "играм"
    elif sort_by == "wr":
        rows = [r for r in rows if r["games"] >= 10]
        rows.sort(key=lambda x: (x["wr"], x["games"]), reverse=True)
        header = "винрейту"
    else:  # kda
        rows = [r for r in rows if r["games"] >= 10]
        rows.sort(key=lambda x: (x["kda"], x["games"]), reverse=True)
        header = "KDA"

    top = rows[:15]
    lines = [f"🧙 <b>Герои — топ 15 по {header}</b>"]
    for i, r in enumerate(top, 1):
        lines.append(f"{i}) {r['hero']} — игр: {r['games']}, WR: {r['wr']:.0f}%, KDA: {r['kda']:.2f}")
    await cb.message.answer("\n".join(lines), parse_mode="HTML")

@dp.callback_query(F.data == "heroes_menu")
async def heroes_menu(cb: CallbackQuery):
    await cb.message.answer("Выбери:", reply_markup=heroes_keyboard()); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_games")
async def heroes_games(cb: CallbackQuery):
    await render_heroes(cb, "games"); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_wr")
async def heroes_wr(cb: CallbackQuery):
    await render_heroes(cb, "wr"); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_kda")
async def heroes_kda(cb: CallbackQuery):
    await render_heroes(cb, "kda"); await cb.answer()

# ----- Герой-аналитика (WR и NetWorth) -----
@dp.callback_query(F.data == "heroes_analytics")
async def heroes_analytics(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]

    async with aiohttp.ClientSession() as sess:
        heroes_map = await fetch_heroes_map(sess)

    agg = db_hero_agg(steam32)
    if not agg:
        await cb.message.answer("Недостаточно данных (сыграй пару матчей, чтобы собрать Net Worth)."); await cb.answer(); return

    # топ WR (>=10 игр)
    wr_list = []
    for row in agg:
        g = row["games"]; w = row["wins"]
        if g >= 10:
            wr = (w/g)*100
            wr_list.append((heroes_map.get(row["hero_id"], f"Hero {row['hero_id']}"), g, wr))
    wr_list.sort(key=lambda x: (x[2], x[1]), reverse=True)
    wr_text = ["🏅 <b>Топ героев по винрейту (≥10 игр)</b>"] + [
        f"{i+1}) {h} — WR: {wr:.0f}% (игр: {g})" for i,(h,g,wr) in enumerate(wr_list[:10])
    ]

    # топ по среднему Net Worth
    nw_list = []
    for row in agg:
        if row["games"] >= 5:
            nw = row["avg_nw"] or 0
            nw_list.append((heroes_map.get(row["hero_id"], f"Hero {row['hero_id']}"), row["games"], nw))
    nw_list.sort(key=lambda x: (x[2], x[1]), reverse=True)
    nw_text = ["💰 <b>Топ героев по среднему Net Worth (≥5 игр)</b>"] + [
        f"{i+1}) {h} — NW: {nw:.0f} (игр: {g})" for i,(h,g,nw) in enumerate(nw_list[:10])
    ]

    await cb.message.answer("\n".join(wr_text + [""] + nw_text), parse_mode="HTML")
    await cb.answer()

# ----- График активности -----
@dp.callback_query(F.data == "activity")
async def on_activity(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]

    async with aiohttp.ClientSession() as sess:
        matches = await fetch_last_matches(sess, steam32, limit=200, ranked_only=False)

    if not matches:
        await cb.message.answer("Пока нет матчей."); await cb.answer(); return

    by_day: Dict[str, int] = {}
    for m in matches:
        dt = datetime.fromtimestamp(m["start_time"], tz=timezone.utc) + timedelta(hours=MSK_OFFSET_HOURS)
        day = dt.strftime("%Y-%m-%d")
        by_day[day] = by_day.get(day, 0) + 1

    days_sorted = sorted(by_day.items())
    days_tail = days_sorted[-7:]
    labels = [d for d,_ in days_tail]
    values = [c for _,c in days_tail]

    fig, ax = plt.subplots(figsize=(7,4))
    ax.plot(labels, values, marker="o")
    ax.set_title("Активность: игр в день (последние 7)")
    ax.set_xlabel("Дата")
    ax.set_ylabel("Игры")
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.xticks(rotation=45)
    plt.tight_layout()
    img_path = f"activity_{steam32}.png"
    fig.savefig(img_path); plt.close(fig)

    total = sum(values)
    avg = total/len(values) if values else 0
    top_day = max(values) if values else 0
    caption = (
        f"📈 <b>Активность за {len(values)} дн.</b>\n"
        f"• Всего игр: <b>{total}</b>\n"
        f"• В среднем в день: <b>{avg:.1f}</b>\n"
        f"• Пик за день: <b>{top_day}</b>\n"
    )
    await bot.send_photo(cb.message.chat.id, FSInputFile(img_path), caption=caption, parse_mode="HTML",
                         reply_markup=charts_keyboard())
    try: os.remove(img_path)
    except Exception: pass
    await cb.answer()

# ----- График тренда MMR -----
@dp.callback_query(F.data == "mmr_trend")
async def on_mmr_trend(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]
    rows = db_last_matches(steam32, 60)  # до 60 последних
    pts = [(r["start_time"], r["mmr_after"]) for r in rows if r["lobby_type"]==7 and r["mmr_after"] is not None]
    pts = sorted(set(pts))
    if not pts:
        await cb.message.answer("Недостаточно данных для графика (сыграй ranked-матчи)."); await cb.answer(); return

    xs = [datetime.fromtimestamp(t, tz=timezone.utc) + timedelta(hours=MSK_OFFSET_HOURS) for t,_ in pts]
    ys = [y for _,y in pts]
    fig, ax = plt.subplots(figsize=(7,4))
    ax.plot(xs, ys, marker="o")
    ax.set_title("Тренд MMR (ranked)")
    ax.set_xlabel("Дата")
    ax.set_ylabel("MMR")
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.autofmt_xdate()
    plt.tight_layout()
    img_path = f"mmr_{steam32}.png"
    fig.savefig(img_path); plt.close(fig)

    caption = f"📉 <b>Тренд MMR</b>\nТочек: <b>{len(ys)}</b> | Текущий: <b>{ys[-1]}</b>"
    await bot.send_photo(cb.message.chat.id, FSInputFile(img_path), caption=caption, parse_mode="HTML",
                         reply_markup=charts_keyboard())
    try: os.remove(img_path)
    except Exception: pass
    await cb.answer()

# ----- Винрейт по ролям -----
@dp.callback_query(F.data == "role_wr")
async def on_role_wr(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]
    stat = db_role_wr(steam32)
    core = stat["core"]; sup = stat["support"]
    core_wr = round(100*core["w"]/core["g"]) if core["g"] else 0
    sup_wr = round(100*sup["w"]/sup["g"]) if sup["g"] else 0
    txt = (
        "🎭 <b>Винрейт по ролям</b>\n"
        f"• Core — игр: <b>{core['g']}</b>, побед: <b>{core['w']}</b>, WR: <b>{core_wr}%</b>\n"
        f"• Support — игр: <b>{sup['g']}</b>, побед: <b>{sup['w']}</b>, WR: <b>{sup_wr}%</b>\n"
        "Примечание: роль определяется эвристически по GPM и ключевым предметам."
    )
    await cb.message.answer(txt, parse_mode="HTML"); await cb.answer()

# ----- AI совет -----
SILENCE_HEROES = {75, 43, 35, 22, 15}  # примеры
MAGIC_NUKERS = {25,31,74,62,66,101}
PHY_DPS = {8,99,114,95}
ILLUSION_CORES = {12,19,111}

def ai_suggest(items: List[str], enemies: List[int], role_hint: str) -> List[str]:
    s = set(items or [])
    tips = []
    def want(name, cond=True):
        if cond and (name not in s):
            tips.append(name)
    if SILENCE_HEROES & set(enemies):
        want("Black King Bar (BKB)")
        want("Manta Style", role_hint in ("carry","mid"))
        want("Lotus Orb", role_hint in ("offlane","support"))
    if MAGIC_NUKERS & set(enemies):
        want("Hood of Defiance / Pipe")
        want("BKB")
    if PHY_DPS & set(enemies):
        want("Force Staff")
        want("Ghost Scepter / E-Blade", role_hint in ("support","mid"))
        want("Shiva's Guard / Assault Cuirass", role_hint in ("offlane","carry"))
        want("Heaven's Halberd", role_hint in ("offlane","support"))
    if ILLUSION_CORES & set(enemies):
        want("Maelstrom / Battle Fury / Cleave")
        want("Crimson Guard / Radiance (ситуативно)")
    want("Wards / Dust", role_hint=="support")
    if not tips: tips.append("Сборка ок 👍 (по базовым правилам)")
    return tips[:8]

@dp.callback_query(F.data == "ai_last")
async def on_ai_last(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи аккаунт.", reply_markup=main_menu(False)); await cb.answer(); return
    steam32 = u["steam32"]
    last_match_id = u.get("last_any_match_id")
    if not last_match_id:
        await cb.message.answer("Пока нет последнего матча."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        detail = await fetch_match_detail(sess, last_match_id)
        heroes_map = await fetch_heroes_map(sess)

    if not detail or "players" not in detail:
        await cb.message.answer("Не удалось получить детали матча."); await cb.answer(); return

    you = next((p for p in detail["players"] if p.get("account_id")==int(steam32)), None)
    if not you:
        await cb.message.answer("В деталях матча нет твоих данных."); await cb.answer(); return

    your_slot = you.get("player_slot", 0)
    your_team_radiant = your_slot < 128
    enemies = [p.get("hero_id") for p in detail["players"] if ((p.get("player_slot",0)<128) != your_team_radiant)]
    purchases = [log.get("key","") for log in you.get("purchase_log", [])]
    role = guess_role(purchases, you.get("gold_per_min",0) or 0)
    tips = ai_suggest(purchases, enemies, role)
    enemy_list = ", ".join(heroes_map.get(h, f"Hero {h}") for h in enemies)
    txt = (
        "🤖 <b>Совет по сборке (последний матч)</b>\n"
        f"Роль: <b>{role}</b>\n"
        f"Противники: {enemy_list}\n"
        "Рекомендации:\n• " + "\n• ".join(tips) + "\n\n"
        f"<a href='https://www.opendota.com/matches/{last_match_id}'>Открыть матч</a>"
    )
    await cb.message.answer(txt, parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()

# ========= BACKGROUND: polling + daily summary =========
async def send_match_card(chat_id: int, heroes_map: Dict[int,str], m: dict,
                          mmr_after: Optional[int], delta_mmr: Optional[int]):
    hero = heroes_map.get(m.get("hero_id"), f"Hero {m.get('hero_id')}")
    win = is_win(m.get("player_slot",0), bool(m.get("radiant_win")))
    outcome = "✅ Победа" if win else "❌ Поражение"
    kdastr = kda_str(m.get("kills",0), m.get("deaths",0), m.get("assists",0))
    when = ts_msk_str(m.get("start_time",0))
    dur = fmt_duration(m.get("duration",0))
    mode = f"{lobby_name(m.get('lobby_type'))} | {game_mode_name(m.get('game_mode'))}"

    mmr_line = ""
    if m.get("lobby_type") == 7 and (mmr_after is not None) and (delta_mmr is not None):
        arrow = "▲" if delta_mmr > 0 else "▼" if delta_mmr < 0 else "•"
        mmr_line = f"\n📈 Изменение: {arrow} {delta_mmr:+d}\n📊 Текущий рейтинг: <b>{mmr_after}</b>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть матч в OpenDota", url=f"https://www.opendota.com/matches/{m.get('match_id')}")],
        [InlineKeyboardButton(text="🤖 Совет по сборке", callback_data="ai_last")]
    ])

    text = (
        "🎮 <b>Новая игра</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📅 {when}\n"
        f"🧩 Режим: <b>{mode}</b>\n"
        f"🧙 Герой: <b>{hero}</b>\n"
        f"⚔️ {kdastr}  ⏱ {dur}\n"
        f"🏆 Итог: {outcome}"
        f"{mmr_line}\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

async def poll_worker():
    await asyncio.sleep(3)
    while True:
        try:
            users = db_get_users_with_steam()
            if not users:
                await asyncio.sleep(POLL_INTERVAL_SEC); continue

            async with aiohttp.ClientSession() as sess:
                heroes_map = await fetch_heroes_map(sess)
                for u in users:
                    steam32 = u["steam32"]; tg = u["telegram_id"]

                    # трекаем rank_tier для rank up/down
                    player = await fetch_player(sess, steam32)
                    rank_tier = (player or {}).get("rank_tier")
                    if rank_tier is not None:
                        prev = u.get("last_rank_tier")
                        if prev is not None and prev != rank_tier:
                            direction = "⬆️ Ранг ап!" if rank_tier > prev else "⬇️ Ранг даун..."
                            await bot.send_message(tg, f"🏅 {direction} Теперь: <b>{rank_name(rank_tier)}</b>", parse_mode="HTML")
                        db_set_last_rank_tier(tg, rank_tier)
                        # обновим авто-MMR
                        auto_mmr = mmr_from_rank_tier(rank_tier)
                        if auto_mmr is not None:
                            db_update_auto_mmr(tg, auto_mmr)

                    # последняя игра любого режима
                    arr_any = await fetch_last_matches(sess, steam32, limit=1, ranked_only=False)
                    if arr_any:
                        m = arr_any[0]
                        mid = m["match_id"]
                        if u.get("last_any_match_id") != mid:
                            detail = await fetch_match_detail(sess, mid)
                            nw = gpm = None
                            role = "core"
                            if detail and "players" in detail:
                                you = next((p for p in detail["players"] if p.get("account_id")==int(steam32)), None)
                                if you:
                                    nw = you.get("net_worth")
                                    gpm = you.get("gold_per_min")
                                    purchases = [log.get("key","") for log in you.get("purchase_log", [])]
                                    role = guess_role(purchases, gpm or 0)

                            # дельта MMR — только для ranked; применяем к «эффективному» источнику
                            delta = None
                            mmr_after = None
                            if m.get("lobby_type") == 7:
                                cur_user = db_get_user(tg)
                                curr = effective_mmr(cur_user)
                                if isinstance(curr, int):
                                    win = is_win(m.get("player_slot",0), m.get("radiant_win",False))
                                    delta = ASSUMED_MMR_DELTA if win else -ASSUMED_MMR_DELTA
                                    mmr_after = curr + delta
                                    if cur_user.get("user_set_mmr") is not None:
                                        db_set_user_mmr(tg, mmr_after)
                                    else:
                                        db_update_auto_mmr(tg, mmr_after)

                            db_upsert_match(steam32, m, nw, gpm, role, delta, mmr_after)
                            db_set_last_match_ids(tg, any_id=mid)

                            # карточка матча
                            await send_match_card(tg, heroes_map, m, mmr_after, delta)

                            # streak notify
                            sdir = db_calc_streak_dir(steam32)
                            if sdir >= STREAK_NOTIFY_WIN:
                                await bot.send_message(tg, f"🔥 Винстрик: {sdir} подряд! Так держать!")
                            elif -sdir >= STREAK_NOTIFY_LOSE:
                                await bot.send_message(tg, f"💀 Лузстрик: {-sdir} подряд. Передохни или соберись 💪")

                    # последняя ranked
                    arr_ranked = await fetch_last_matches(sess, steam32, limit=1, ranked_only=True)
                    if arr_ranked:
                        mid_r = arr_ranked[0]["match_id"]
                        if u.get("last_ranked_match_id") != mid_r:
                            db_set_last_match_ids(tg, ranked_id=mid_r)

        except Exception as e:
            logging.warning(f"poll_worker error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SEC)

def seconds_until_2359_msk() -> int:
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc + timedelta(hours=MSK_OFFSET_HOURS)
    target = now_msk.replace(hour=23, minute=59, second=0, microsecond=0)
    if target <= now_msk:
        target += timedelta(days=1)
    return max(5, int((target - now_msk).total_seconds()))

def db_get_users_with_steam() -> List[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT * FROM users WHERE steam32 IS NOT NULL").fetchall()
        return [dict(r) for r in rs]

async def daily_summary_worker():
    await asyncio.sleep(5)
    while True:
        try:
            wait = seconds_until_2359_msk()
            await asyncio.sleep(wait)
            users = db_get_users_with_steam()
            if not users:
                continue

            # интервал на сегодня по МСК
            now_utc = datetime.now(timezone.utc)
            now_msk = now_utc + timedelta(hours=MSK_OFFSET_HOURS)
            start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
            start_ts = int((start_msk - timedelta(hours=MSK_OFFSET_HOURS)).timestamp())
            end_ts = int((now_msk - timedelta(hours=MSK_OFFSET_HOURS)).timestamp())

            async with aiohttp.ClientSession() as sess:
                for u in users:
                    steam32 = u["steam32"]; tg = u["telegram_id"]
                    arr = await fetch_last_matches(sess, steam32, limit=200, ranked_only=False)
                    today = [m for m in arr if start_ts <= m["start_time"] <= end_ts]

                    games = len(today)
                    wins = sum(1 for m in today if is_win(m.get("player_slot",0), m.get("radiant_win",False)))
                    loses = games - wins
                    wr = round(100*wins/games) if games else 0

                    # ΔMMR из БД по сегодняшним ranked
                    with closing(sqlite3.connect(DB_PATH)) as con:
                        con.row_factory = sqlite3.Row
                        rs = con.execute("""
                            SELECT SUM(COALESCE(delta_mmr,0)) s FROM matches
                            WHERE steam32=? AND lobby_type=7 AND start_time BETWEEN ? AND ?
                        """, (steam32, start_ts, end_ts)).fetchone()
                        dm = rs["s"] if rs and rs["s"] is not None else 0

                    curr = effective_mmr(db_get_user(tg))
                    txt = (
                        "📊 <b>Итоги дня</b>\n"
                        f"• Игр: <b>{games}</b>\n"
                        f"• Победы/Поражения: <b>{wins}</b>/<b>{loses}</b> (WR <b>{wr}%</b>)\n"
                        f"• Δ MMR (ranked): <b>{dm:+d}</b>\n"
                        f"• Текущий рейтинг: <b>{curr if curr is not None else '—'}</b>"
                    )
                    if games == 0:
                        txt += "\n• Сегодня ты не играл — удачи завтра! ✨"
                    await bot.send_message(tg, txt, parse_mode="HTML")

        except Exception as e:
            logging.warning(f"daily_summary_worker error: {e}")
            await asyncio.sleep(10)

# ========= STARTUP =========
async def main():
    db_init()
    asyncio.create_task(poll_worker())
    asyncio.create_task(daily_summary_worker())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
