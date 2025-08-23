import os
import re
import asyncio
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

# ===================== ENV =====================
TOKEN = os.getenv("TELEGRAM_TOKEN", "8475681655:AAE10f4jbdYZ0Q2fgTLvQ1HhXK8U6KQ9gD0")
DB_PATH = os.getenv("DB_PATH", "data.db")
OPEN_DOTA = "https://api.opendota.com/api"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))          # каждые 60с
ASSUMED_MMR_DELTA = int(os.getenv("ASSUMED_MMR_DELTA", "30"))  # оценка ±MMR за игру
TRACK_RANKED_ONLY = os.getenv("TRACK_RANKED_ONLY", "true").lower() in ("1", "true", "yes")
MSK_UTC_OFFSET = 3  # UTC+3

if not TOKEN:
    raise SystemExit("❌ TELEGRAM_TOKEN не задан")

# ===================== DB =====================
def db_init():
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            steam32 TEXT,
            current_mmr INTEGER,
            max_mmr INTEGER,
            last_match_id INTEGER,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            steam32 TEXT,
            match_id INTEGER,
            when_ts INTEGER,
            hero_id INTEGER,
            k INTEGER, d INTEGER, a INTEGER,
            duration INTEGER,
            radiant_win INTEGER,
            player_slot INTEGER,
            net_worth INTEGER,
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
        INSERT INTO users(telegram_id, steam32)
        VALUES(?,?)
        ON CONFLICT(telegram_id) DO UPDATE SET steam32=excluded.steam32
        """, (tg_id, steam32))
        con.commit()

def db_set_user_mmr(tg_id: int, mmr: Optional[int]):
    with closing(sqlite3.connect(DB_PATH)) as con:
        if mmr is None:
            con.execute("UPDATE users SET current_mmr=NULL WHERE telegram_id=?", (tg_id,))
        else:
            con.execute("UPDATE users SET current_mmr=?, max_mmr=MAX(COALESCE(max_mmr,0),?) WHERE telegram_id=?", (mmr, mmr, tg_id))
        con.commit()

def db_set_last_match(tg_id: int, match_id: int):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("UPDATE users SET last_match_id=? WHERE telegram_id=?", (match_id, tg_id))
        con.commit()

def db_upsert_match(rec: dict):
    keys = ["steam32","match_id","when_ts","hero_id","k","d","a","duration",
            "radiant_win","player_slot","net_worth","delta_mmr","mmr_after"]
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute(f"""
        INSERT INTO matches({",".join(keys)}) VALUES({",".join("?" for _ in keys)})
        ON CONFLICT(steam32, match_id) DO UPDATE SET
            when_ts=excluded.when_ts,
            hero_id=excluded.hero_id,
            k=excluded.k, d=excluded.d, a=excluded.a,
            duration=excluded.duration,
            radiant_win=excluded.radiant_win,
            player_slot=excluded.player_slot,
            net_worth=excluded.net_worth,
            delta_mmr=excluded.delta_mmr,
            mmr_after=excluded.mmr_after
        """, tuple(rec[k] for k in keys))
        con.commit()

def db_get_last_matches(steam32: str, limit: int = 10) -> List[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT * FROM matches WHERE steam32=? ORDER BY when_ts DESC LIMIT ?", (steam32, limit)).fetchall()
        return [dict(r) for r in rs]

def db_get_all_users_with_steam() -> List[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT * FROM users WHERE steam32 IS NOT NULL").fetchall()
        return [dict(r) for r in rs]

# streak из БД
def db_calc_streak(steam32: str) -> int:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT radiant_win, player_slot FROM matches WHERE steam32=? ORDER BY when_ts DESC LIMIT 50", (steam32,)).fetchall()
    streak = 0
    for r in rs:
        win = (r["player_slot"] < 128 and r["radiant_win"]==1) or (r["player_slot"]>=128 and r["radiant_win"]==0)
        if win: streak += 1
        else: break
    return streak

# ===================== FSM =====================
class BindSteam(StatesGroup):
    wait_steam = State()

# ===================== Utils =====================
_hero_cache: Dict[int, str] = {}
_item_names: Dict[str, dict] = {}
_last_api_error: Optional[str] = None

STEAM_PROFILE_RE = re.compile(r"(?:https?://)?steamcommunity\.com/(?:id|profiles)/([^/\s]+)", re.I)

def to_steam32(maybe_id_or_url: str) -> Optional[str]:
    s = (maybe_id_or_url or "").strip()
    # ссылка на профиль
    m = STEAM_PROFILE_RE.search(s)
    if m:
        part = m.group(1)
        # если profiles/<steam64>
        if part.isdigit() and len(part) >= 16:
            return str(int(part) - 76561197960265728)
        # vanity id (id/<name>) — OpenDota сам не резолвит; попросим дать steam64
        return None
    # просто числа
    if not s.isdigit():
        return None
    if len(s) >= 16:  # steam64
        try:
            return str(int(s) - 76561197960265728)
        except Exception:
            return None
    return s  # уже steam32

def fmt_duration(sec: int) -> str:
    m, s = divmod(max(0, sec), 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"

def ts_to_msk(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=MSK_UTC_OFFSET)
    return dt.strftime("%d.%m.%Y %H:%M МСК")

def is_win(player_slot: int, radiant_win: bool) -> bool:
    rad = player_slot < 128
    return (rad and radiant_win) or ((not rad) and (not radiant_win))

def bold(x: str) -> str:
    return f"<b>{x}</b>"

# «оценка» MMR по rank_tier (грубая, т.к. Valve не отдает точный MMR)
def mmr_from_rank_tier(rank_tier: Optional[int]) -> Optional[int]:
    if not isinstance(rank_tier, int): return None
    # эмпирически: 1 звезда ≈ +140 MMR, коридоры по медалям
    base = {1:0,2:560,3:1260,4:1960,5:2660,6:3360,7:4060,8:5200} # стартовые точки медалей
    major = rank_tier // 10
    minor = rank_tier % 10
    if major not in base: return None
    return base[major] + (minor-1)*140 if minor>=1 else base[major]

def next_rank_mmr(rank_tier: Optional[int]) -> Optional[int]:
    cur = mmr_from_rank_tier(rank_tier)
    if cur is None: return None
    return cur + 140  # до следующей «звезды»

def parse_rank_tier(rank_tier: Optional[int]) -> str:
    if not isinstance(rank_tier, int):
        return "—"
    names = {1:"Herald",2:"Guardian",3:"Crusader",4:"Archon",5:"Legend",6:"Ancient",7:"Divine",8:"Immortal"}
    return f"{names.get(rank_tier//10,'?')} {rank_tier%10}"

# ===================== OpenDota =====================
async def od_get(session: aiohttp.ClientSession, path: str, params: dict=None) -> Any:
    global _last_api_error
    try:
        async with session.get(f"{OPEN_DOTA}{path}", params=params, timeout=25) as r:
            if r.status == 404:
                _last_api_error = f"404: {path}"; return None
            r.raise_for_status()
            _last_api_error = None
            return await r.json()
    except Exception as e:
        _last_api_error = f"{type(e).__name__}: {e}"
        return None

async def fetch_heroes(session) -> Dict[int, str]:
    global _hero_cache
    if _hero_cache: return _hero_cache
    data = await od_get(session, "/heroes")
    _hero_cache = {h["id"]: h["localized_name"] for h in (data or [])}
    return _hero_cache

async def fetch_items(session) -> Dict[str, dict]:
    global _item_names
    if _item_names: return _item_names
    data = await od_get(session, "/constants/items") or {}
    _item_names = data
    return _item_names

async def fetch_player(session, steam32: str) -> Optional[dict]:
    return await od_get(session, f"/players/{steam32}")

async def fetch_player_wl(session, steam32: str) -> Optional[dict]:
    return await od_get(session, f"/players/{steam32}/wl")

async def fetch_player_totals(session, steam32: str) -> Optional[List[dict]]:
    return await od_get(session, f"/players/{steam32}/totals")

async def fetch_player_heroes(session, steam32: str) -> Optional[List[dict]]:
    return await od_get(session, f"/players/{steam32}/heroes")

async def fetch_latest_matches(session, steam32: str, limit: int=10) -> List[dict]:
    params = {"limit": limit}
    if TRACK_RANKED_ONLY: params["lobby_type"] = 7
    return await od_get(session, f"/players/{steam32}/matches", params=params) or []

async def fetch_match_detail(session, match_id: int) -> Optional[dict]:
    return await od_get(session, f"/matches/{match_id}")

# ===================== Keyboards =====================
def main_menu(bound: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🕓 Последние 10 матчей", callback_data="last10")],
        [InlineKeyboardButton(text="📊 Статус", callback_data="status")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="overall")],
        [InlineKeyboardButton(text="🧙 Герои", callback_data="heroes_menu")],
        [InlineKeyboardButton(text="🤖 AI-совет (последняя игра)", callback_data="ai_last")]
    ]
    rows.insert(0, [InlineKeyboardButton(text=("🔁 Сменить Steam" if bound else "➕ Привязать Steam"),
                                         callback_data="bind_steam")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def heroes_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Топ по играм", callback_data="heroes_sort_games")],
        [InlineKeyboardButton(text="🎯 Топ по винрейту", callback_data="heroes_sort_wr")],
        [InlineKeyboardButton(text="⚔️ Топ по KDA", callback_data="heroes_sort_kda")]
    ])

# ===================== Bot =====================
bot = Bot(TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def on_start(m: Message, state: FSMContext):
    u = db_get_user(m.from_user.id)
    await m.answer(
        f"👋 Привет, {bold(m.from_user.first_name)}!\n"
        "Я слежу за твоими играми в Dota 2: статус, матчи, герои, отчёты, и даже подскажу по сборке на основе драфта.\n"
        "Начни с привязки Steam.",
        reply_markup=main_menu(bool(u and u.get("steam32"))), parse_mode="HTML"
    )

@dp.message(Command("debug"))
async def on_debug(m: Message):
    u = db_get_user(m.from_user.id)
    await m.answer(f"steam32={u.get('steam32') if u else None}\nlast_api_error={_last_api_error}")

# -------- Привязка Steam (ID или ссылка) --------
class BindSteam(StatesGroup):
    wait_steam = State()

@dp.callback_query(F.data == "bind_steam")
async def on_bind(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Пришли свой Steam:\n• Steam64 / Steam32\n• или ссылку вида https://steamcommunity.com/profiles/XXXXXXXXXXXXXXX\n( vanity /id/<name> не поддерживается — пришли профиль со /profiles/ )")
    await state.set_state(BindSteam.wait_steam); await cb.answer()

@dp.message(BindSteam.wait_steam)
async def on_bind_value(m: Message, state: FSMContext):
    steam32 = to_steam32(m.text)
    if not steam32:
        await m.answer("Не смог определить Steam ID. Пришли Steam64/Steam32 или ссылку на профиль с /profiles/."); return

    async with aiohttp.ClientSession() as sess:
        player = await fetch_player(sess, steam32)

    if not player or not player.get("profile"):
        await m.answer("Профиль не найден в OpenDota. Открой игровой профиль в Dota 2 и авторизуйся на opendota.com через Steam.")
        return

    db_set_user_steam(m.from_user.id, steam32)

    # выставим текущий MMR по rank_tier (оценка)
    mmr_est = mmr_from_rank_tier(player.get("rank_tier"))
    db_set_user_mmr(m.from_user.id, mmr_est)

    await state.clear()
    await m.answer(f"✅ Привязал Steam32: {bold(steam32)}", reply_markup=main_menu(True), parse_mode="HTML")

# -------- Статус --------
@dp.callback_query(F.data == "status")
async def on_status(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        player = await fetch_player(sess, u["steam32"])
        hero_map = await fetch_heroes(sess)

    prof = (player or {}).get("profile") or {}
    rank_tier = player.get("rank_tier")
    dota_plus = bool(prof.get("plus"))  # есть ли Dota+
    mmr_est = mmr_from_rank_tier(rank_tier)
    next_m = next_rank_mmr(rank_tier)
    need = (next_m - mmr_est) if (next_m and mmr_est) else None
    last_mid = u.get("last_match_id")

    # streak и max mmr
    streak = db_calc_streak(u["steam32"])
    max_mmr = u.get("max_mmr")

    last_line = "—"
    kb = None
    if last_mid:
        last_line = f"<a href='https://www.opendota.com/matches/{last_mid}'>#{last_mid}</a>"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Открыть последний матч", url=f"https://www.opendota.com/matches/{last_mid}")]
        ])

    text = (
        "📊 " + bold("Статус аккаунта") + "\n"
        f"👤 Ник: {bold(prof.get('personaname') or '—')}\n"
        f"🆔 Steam32: {bold(u['steam32'])}\n"
        f"🏅 Ранг: {bold(parse_rank_tier(rank_tier))}\n"
        f"📈 Оценочный MMR: {bold(str(mmr_est)) if mmr_est else '—'}"
        f"{(' | до след. звезды: ' + bold(str(need)) + ' MMR') if need else ''}\n"
        f"💛 Dota Plus: {bold('Да' if dota_plus else 'Нет')}\n"
        f"🔥 Текущий винстрик: {bold(str(streak))}\n"
        f"🔝 Макс. MMR: {bold(str(max_mmr)) if max_mmr else '—'}\n"
        f"🕓 Последний матч: {last_line}\n"
        f"🔗 Профиль OpenDota: <a href='https://www.opendota.com/players/{u['steam32']}'>открыть</a>"
    )
    await cb.message.answer(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()

# -------- Общая статистика --------
@dp.callback_query(F.data == "overall")
async def on_overall(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        wl = await fetch_player_wl(sess, u["steam32"])
        totals = await fetch_player_totals(sess, u["steam32"])

    wins = wl.get("win", 0) if wl else 0
    loses = wl.get("lose", 0) if wl else 0
    games = wins + loses
    wr = round(100 * wins / games) if games else 0

    k = d = a = 0
    if totals:
        for t in totals:
            if t.get("field") == "kills": k = t.get("sum", 0)
            if t.get("field") == "deaths": d = t.get("sum", 0)
            if t.get("field") == "assists": a = t.get("sum", 0)
    avg_k = round(k / games, 1) if games else 0
    avg_d = round(d / games, 1) if games else 0
    avg_a = round(a / games, 1) if games else 0

    text = (
        "📈 " + bold("Общая статистика") + "\n"
        f"• Всего игр: {bold(str(games))}\n"
        f"• Победы/Поражения: {bold(str(wins))}/{bold(str(loses))} (WR {bold(str(wr))}%)\n"
        f"• Средняя K/D/A: {bold(f'{avg_k}/{avg_d}/{avg_a}')}\n"
        "• Примечание: точный MMR Valve не отдаёт через API; показывается оценка по рангу."
    )
    await cb.message.answer(text, parse_mode="HTML")
    await cb.answer()

# -------- Герои (фикс KDA) --------
def kda_calc(k, d, a) -> float:
    return round((k + a) / max(1, d), 2)

async def render_heroes_list(cb: CallbackQuery, sort_by: str):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        hero_map = await fetch_heroes(sess)
        data = await fetch_player_heroes(sess, u["steam32"])

    if not data:
        await cb.message.answer("Нет данных по героям (профиль может быть приватным)."); await cb.answer(); return

    rows = []
    for it in data:
        hid = it.get("hero_id", 0)
        games = it.get("games", 0) or 0
        wins = it.get("win", 0) or 0
        k = it.get("k", 0) or 0
        d = it.get("d", 0) or 0
        a = it.get("a", 0) or 0
        wr = (wins / games * 100) if games else 0
        rows.append({
            "hero": hero_map.get(hid, f"Hero {hid}"),
            "games": games,
            "wins": wins,
            "wr": wr,
            "kda": kda_calc(k, d, a)
        })

    if sort_by == "games":
        rows.sort(key=lambda x: x["games"], reverse=True)
    elif sort_by == "wr":
        rows = [r for r in rows if r["games"] >= 10]
        rows.sort(key=lambda x: (x["wr"], x["games"]), reverse=True)
    elif sort_by == "kda":
        rows = [r for r in rows if r["games"] >= 10]
        rows.sort(key=lambda x: (x["kda"], x["games"]), reverse=True)

    top = rows[:15]
    header = "игры" if sort_by=='games' else "винрейт" if sort_by=='wr' else "KDA"
    lines = [f"🧙 {bold('Герои — топ 15 по ' + header)}"]
    for i, r in enumerate(top, 1):
        lines.append(f"{i}) {r['hero']} — игр: {r['games']}, WR: {r['wr']:.0f}%, KDA: {r['kda']:.2f}")
    await cb.message.answer("\n".join(lines), parse_mode="HTML")

@dp.callback_query(F.data == "heroes_menu")
async def on_heroes_menu(cb: CallbackQuery):
    await cb.message.answer("Выбери сортировку:", reply_markup=heroes_keyboard()); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_games")
async def heroes_games(cb: CallbackQuery):
    await render_heroes_list(cb, "games"); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_wr")
async def heroes_wr(cb: CallbackQuery):
    await render_heroes_list(cb, "wr"); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_kda")
async def heroes_kda(cb: CallbackQuery):
    await render_heroes_list(cb, "kda"); await cb.answer()

# -------- Последние 10 матчей --------
@dp.callback_query(F.data == "last10")
async def on_last10(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam."); await cb.answer(); return

    rows = db_get_last_matches(u["steam32"], 10)
    if not rows:
        async with aiohttp.ClientSession() as sess:
            arr = await fetch_latest_matches(sess, u["steam32"], 10)
            for m in arr:
                detail = await fetch_match_detail(sess, m.get("match_id"))
                nw = None
                if detail and "players" in detail:
                    for p in detail["players"]:
                        if p.get("account_id") == int(u["steam32"]):
                            nw = p.get("net_worth"); break
                db_upsert_match({
                    "steam32": u["steam32"],
                    "match_id": m.get("match_id"),
                    "when_ts": m.get("start_time", 0),
                    "hero_id": m.get("hero_id", 0),
                    "k": m.get("kills", 0), "d": m.get("deaths", 0), "a": m.get("assists", 0),
                    "duration": m.get("duration", 0),
                    "radiant_win": int(m.get("radiant_win", False)),
                    "player_slot": m.get("player_slot", 0),
                    "net_worth": nw if isinstance(nw, int) else None,
                    "delta_mmr": None, "mmr_after": None
                })
        rows = db_get_last_matches(u["steam32"], 10)

    async with aiohttp.ClientSession() as sess:
        hero_map = await fetch_heroes(sess)

    lines = ["🕓 " + bold("Последние 10 матчей")]
    for i, r in enumerate(rows, 1):
        hero = hero_map.get(r["hero_id"], f"Hero {r['hero_id']}")
        flag = "✅" if is_win(r["player_slot"], bool(r["radiant_win"])) else "❌"
        date = ts_to_msk(r["when_ts"]).split(" ")[0]
        dlt = f" {r['delta_mmr']:+d}" if isinstance(r["delta_mmr"], int) else ""
        aft = f" ({r['mmr_after']})" if isinstance(r["mmr_after"], int) else ""
        lines.append(f"{i}) {hero} | {date} | {flag}{dlt}{aft}  —  <a href='https://www.opendota.com/matches/{r['match_id']}'>match</a>")
    await cb.message.answer("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()

# -------- AI совет по сборке (последняя игра) --------
SILENCE_HEROES = {  # базовый набор
    75, # Silencer
    43, # Death Prophet
    35, # Riki (cloud)
    22, # Drow (gust)
    15  # Skywrath Mage
}
MAGIC_NUKERS = {  # маг урон
    22,25,31,36,74,62,66,74,101, # drow, lina, lion, lich, zeus, lesh, tinker, zeus(id dup ok)
}
PHY_DPS = {8,99,114,95, # jug, pa, sven, sniper
}
ILLUSION_CORES = {12,19,111}  # PL,Naga,Terrorblade

def ai_suggest(items_bought: List[str], allies: List[int], enemies: List[int], role_hint: Optional[str]) -> List[str]:
    tips = []
    s = set(items_bought)
    def need(name: str, cond=True):
        if cond and (name not in s):
            tips.append(name)

    # против молчаний
    if SILENCE_HEROES & set(enemies):
        need("Black King Bar (BKB)")
        need("Manta Style", cond=role_hint in ("carry","mid"))
        need("Lotus Orb", cond=role_hint in ("offlane","support"))

    # много маг урона
    if MAGIC_NUKERS & set(enemies):
        need("Hood of Defiance / Pipe of Insight")
        need("Black King Bar (BKB)")

    # физический урон
    if PHY_DPS & set(enemies):
        need("Force Staff", cond=True)
        need("Ghost Scepter / Ethereal Blade", cond=role_hint in ("support","mid"))
        need("Shiva's Guard / Assault Cuirass", cond=role_hint in ("offlane","carry"))
        need("Heaven's Halberd", cond=role_hint in ("offlane","support"))

    # иллюзионисты
    if ILLUSION_CORES & set(enemies):
        need("Maelstrom / Battle Fury / Cleave")
        need("Crimson Guard / Radiance (ситуационно)")

    # универсальные
    need("Observer & Sentry Wards", cond=role_hint in ("support",))
    need("Black King Bar (BKB)", cond=("Black King Bar" not in s and role_hint in ("carry","mid","offlane")))

    if not tips:
        tips.append("Сборка ок 👍 (по простым правилам)")
    return tips[:8]

@dp.callback_query(F.data == "ai_last")
async def on_ai_last(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam."); await cb.answer(); return
    if not u.get("last_match_id"):
        await cb.message.answer("Пока не найден последний матч."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        items_map = await fetch_items(sess)
        detail = await fetch_match_detail(sess, u["last_match_id"])
        hero_map = await fetch_heroes(sess)

    if not detail:
        await cb.message.answer("Не удалось получить детали матча."); await cb.answer(); return

    allies, enemies = [], []
    you = None
    for p in detail.get("players", []):
        if p.get("account_id") == int(u["steam32"]):
            you = p
    if not you:
        await cb.message.answer("В деталях матча нет твоего слота."); await cb.answer(); return

    your_slot = you.get("player_slot", 0)
    your_team_is_radiant = your_slot < 128
    for p in detail.get("players", []):
        hid = p.get("hero_id")
        if (p.get("player_slot", 0) < 128) == your_team_is_radiant:
            allies.append(hid)
        else:
            enemies.append(hid)

    # предметы по purchase_log (названия уже строками)
    purchased = [log.get("key") for log in you.get("purchase_log", []) if isinstance(log.get("key"), str)]

    # роль (упрощённо): по неймингу предметов
    role_hint = None
    core_items = {"battle_fury","manta","bkb","daedalus","skadi","desolator"}
    sup_items  = {"mekansm","glimmer_cape","force_staff","guardian_greaves","solar_crest","lotus_orb"}
    if any(x in purchased for x in core_items): role_hint = "carry"
    if any(x in purchased for x in sup_items): role_hint = "support"
    if role_hint is None:
        role_hint = "mid" if you.get("gold_per_min",0) > 450 else "offlane"

    tips = ai_suggest(purchased, allies, enemies, role_hint)

    enemy_list = ", ".join(hero_map.get(h, f"Hero {h}") for h in enemies)
    text = (
        "🤖 " + bold("AI-разбор последней игры") + "\n"
        f"Твоя роль: {bold(role_hint)}\n"
        f"Враги: {enemy_list}\n"
        "Рекомендации по предметам:\n"
        "• " + "\n• ".join(tips) + "\n\n"
        f"Открыть матч: <a href='https://www.opendota.com/matches/{u['last_match_id']}'>OpenDota</a>"
    )
    await cb.message.answer(text, parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()

# ===================== Background workers =====================
async def poll_new_matches_worker():
    await asyncio.sleep(3)
    while True:
        try:
            users = db_get_all_users_with_steam()
            if not users:
                await asyncio.sleep(POLL_INTERVAL); continue
            async with aiohttp.ClientSession() as sess:
                hero_map = await fetch_heroes(sess)
                for u in users:
                    steam = u["steam32"]; tlg = u["telegram_id"]
                    arr = await fetch_latest_matches(sess, steam, 1)
                    if not arr: continue
                    m = arr[0]
                    mid = m.get("match_id")
                    if not mid or u.get("last_match_id") == mid:
                        continue

                    # оценим ммр
                    win = is_win(m.get("player_slot", 0), m.get("radiant_win", False))
                    delta = ASSUMED_MMR_DELTA if win else -ASSUMED_MMR_DELTA
                    curr = u.get("current_mmr")
                    new_curr = curr + delta if isinstance(curr, int) else None
                    db_set_user_mmr(tlg, new_curr)

                    # enrich
                    nw = None
                    detail = await fetch_match_detail(sess, mid)
                    if detail and "players" in detail:
                        for p in detail["players"]:
                            if p.get("account_id") == int(steam):
                                nw = p.get("net_worth"); break
                    m["net_worth"] = nw if isinstance(nw, int) else None

                    db_upsert_match({
                        "steam32": steam,
                        "match_id": mid,
                        "when_ts": m.get("start_time", 0),
                        "hero_id": m.get("hero_id", 0),
                        "k": m.get("kills", 0), "d": m.get("deaths", 0), "a": m.get("assists", 0),
                        "duration": m.get("duration", 0),
                        "radiant_win": int(m.get("radiant_win", False)),
                        "player_slot": m.get("player_slot", 0),
                        "net_worth": m["net_worth"],
                        "delta_mmr": delta,
                        "mmr_after": new_curr
                    })
                    db_set_last_match(tlg, mid)

                    await send_match_card(bot, tlg, hero_map, m, new_curr, delta)
        except Exception as e:
            print("poll_new_matches_worker error:", e)
        await asyncio.sleep(POLL_INTERVAL)

def seconds_until_2359_msk() -> int:
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc + timedelta(hours=MSK_UTC_OFFSET)
    target_msk = now_msk.replace(hour=23, minute=59, second=0, microsecond=0)
    if target_msk <= now_msk: target_msk += timedelta(days=1)
    return max(5, int((target_msk - now_msk).total_seconds()))

async def daily_summary_worker():
    await asyncio.sleep(5)
    while True:
        try:
            await asyncio.sleep(seconds_until_2359_msk())
            users = db_get_all_users_with_steam()
            if not users: continue

            now_utc = datetime.now(timezone.utc)
            now_msk = now_utc + timedelta(hours=MSK_UTC_OFFSET)
            start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
            end_msk = now_msk.replace(hour=23, minute=59, second=59, microsecond=0)
            start_utc = start_msk - timedelta(hours=MSK_UTC_OFFSET)
            end_utc = end_msk - timedelta(hours=MSK_UTC_OFFSET)
            start_ts, end_ts = int(start_utc.timestamp()), int(end_utc.timestamp())

            async with aiohttp.ClientSession() as sess:
                for u in users:
                    steam = u["steam32"]; tlg = u["telegram_id"]
                    arr = await od_get(sess, f"/players/{steam}/matches", params={"limit": 50, **({"lobby_type":7} if TRACK_RANKED_ONLY else {})}) or []
                    today = [m for m in arr if start_ts <= m.get("start_time", 0) <= end_ts]
                    if not today: continue
                    wins = sum(1 for m in today if is_win(m.get("player_slot",0), m.get("radiant_win",False)))
                    loses = len(today) - wins
                    delta = wins * ASSUMED_MMR_DELTA - loses * ASSUMED_MMR_DELTA
                    curr = u.get("current_mmr")
                    wr = round(100 * wins / len(today)) if today else 0
                    text = (
                        "📊 " + bold(f"Итоги дня ({now_msk.strftime('%d.%m.%Y')})") + "\n"
                        f"• Сыграно игр: {bold(str(len(today)))}\n"
                        f"• Побед/Поражений: {bold(str(wins))}/{bold(str(loses))} (WR {bold(str(wr))}%)\n"
                        f"• Δ MMR: {bold(f'{delta:+d}')}\n"
                        f"• Текущий рейтинг: {bold(str(curr)) if curr is not None else '—'}"
                    )
                    await bot.send_message(tlg, text, parse_mode="HTML")
        except Exception as e:
            print("daily_summary_worker error:", e)
            await asyncio.sleep(10)

# -------- helpers for cards --------
async def send_match_card(bot: Bot, chat_id: int, hero_map: Dict[int, str], m: dict,
                          mmr_after: Optional[int], delta: Optional[int]):
    hero = hero_map.get(m.get("hero_id", 0), f"Hero {m.get('hero_id')}")
    win = is_win(m.get("player_slot", 0), m.get("radiant_win", False))
    outcome = "✅ Победа" if win else "❌ Поражение"
    k, d, a = m.get("kills", 0), m.get("deaths", 0), m.get("assists", 0)
    dur = fmt_duration(m.get("duration", 0))
    when = ts_to_msk(m.get("start_time", 0))
    nw = m.get("net_worth")
    mmr_line = ""
    if isinstance(mmr_after, int) and isinstance(delta, int):
        arrow = "▲" if delta > 0 else "▼" if delta < 0 else "•"
        mmr_line = f"\n📈 Изменение: {arrow} {delta:+d}\n📊 Текущий рейтинг: {bold(str(mmr_after))}"
    nw_line = f"\n💰 Нетворс: {nw:,}".replace(",", " ") if isinstance(nw, int) else ""
    mid = m.get("match_id")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть матч в OpenDota", url=f"https://www.opendota.com/matches/{mid}")],
        [InlineKeyboardButton(text="🤖 Совет по сборке", callback_data="ai_last")]
    ]) if mid else None
    text = (
        "🎮 " + bold("Новая игра") + "\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🏆 Исход: {outcome}\n"
        f"🧙 Герой: {bold(hero)}\n"
        f"⚔️ K/D/A: {bold(f'{k}/{d}/{a}')}  ⏱ {bold(dur)}\n"
        f"📅 Время: {bold(when)}"
        f"{nw_line}{mmr_line}\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

# ===================== Entry =====================
async def main():
    db_init()
    asyncio.create_task(poll_new_matches_worker())
    asyncio.create_task(daily_summary_worker())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
