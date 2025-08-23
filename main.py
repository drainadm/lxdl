import os
import asyncio
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

# ===================== ENV =====================
TOKEN = os.getenv("TELEGRAM_TOKEN", "8475681655:AAE10f4jbdYZ0Q2fgTLvQ1HhXK8U6KQ9gD0")
DB_PATH = os.getenv("DB_PATH", "data.db")
OPEN_DOTA = "https://api.opendota.com/api"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "90"))          # сек, опрос новых матчей
ASSUMED_MMR_DELTA = int(os.getenv("ASSUMED_MMR_DELTA", "30"))  # шаг ±MMR (оценочно)
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
            last_match_id INTEGER,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )""")
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
        )""")
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

def db_set_user_mmr(tg_id: int, mmr: int):
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.execute("UPDATE users SET current_mmr=? WHERE telegram_id=?", (mmr, tg_id))
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
        rs = con.execute("""
            SELECT * FROM matches WHERE steam32=? ORDER BY when_ts DESC LIMIT ?
        """, (steam32, limit)).fetchall()
        return [dict(r) for r in rs]

def db_get_all_users_with_steam() -> List[dict]:
    with closing(sqlite3.connect(DB_PATH)) as con:
        con.row_factory = sqlite3.Row
        rs = con.execute("SELECT * FROM users WHERE steam32 IS NOT NULL").fetchall()
        return [dict(r) for r in rs]

# ===================== FSM =====================
class BindSteam(StatesGroup):
    wait_steam = State()

class SetMMR(StatesGroup):
    wait_mmr = State()

# ===================== Utils =====================
_hero_cache: Dict[int, str] = {}
_last_api_error: Optional[str] = None

def to_steam32(maybe_id: str) -> Optional[str]:
    s = (maybe_id or "").strip()
    if not s.isdigit():
        return None
    if len(s) >= 16:  # steam64
        try:
            return str(int(s) - 76561197960265728)
        except Exception:
            return None
    return s

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

# ===================== OpenDota (async) =====================
async def od_get(session: aiohttp.ClientSession, path: str, params: dict = None) -> Any:
    global _last_api_error
    try:
        async with session.get(f"{OPEN_DOTA}{path}", params=params, timeout=25) as r:
            if r.status == 404:
                _last_api_error = f"404 Not Found: {path}"
                return None
            r.raise_for_status()
            _last_api_error = None
            return await r.json()
    except Exception as e:
        _last_api_error = f"{type(e).__name__}: {e}"
        return None

async def fetch_heroes(session: aiohttp.ClientSession) -> Dict[int, str]:
    global _hero_cache
    if _hero_cache:
        return _hero_cache
    data = await od_get(session, "/heroes")
    _hero_cache = {h["id"]: h["localized_name"] for h in (data or [])}
    return _hero_cache

async def fetch_player(session: aiohttp.ClientSession, steam32: str) -> Optional[dict]:
    return await od_get(session, f"/players/{steam32}")

async def fetch_player_wl(session: aiohttp.ClientSession, steam32: str) -> Optional[dict]:
    return await od_get(session, f"/players/{steam32}/wl")

async def fetch_player_totals(session: aiohttp.ClientSession, steam32: str) -> Optional[List[dict]]:
    return await od_get(session, f"/players/{steam32}/totals")

async def fetch_player_heroes(session: aiohttp.ClientSession, steam32: str) -> Optional[List[dict]]:
    # Возвращает массив по героям: hero_id, games, win, last_played, k, d, a (в некоторых схемах k/d/a есть)
    return await od_get(session, f"/players/{steam32}/heroes")

async def fetch_latest_matches(session: aiohttp.ClientSession, steam32: str, limit: int = 10) -> List[dict]:
    params = {"limit": limit}
    if TRACK_RANKED_ONLY:
        params["lobby_type"] = 7
    arr = await od_get(session, f"/players/{steam32}/matches", params=params)
    return arr or []

async def fetch_match_detail(session: aiohttp.ClientSession, match_id: int) -> Optional[dict]:
    return await od_get(session, f"/matches/{match_id}")

# ===================== Keyboards =====================
def main_menu(bound: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🕓 Последние 10 матчей", callback_data="last10")],
        [InlineKeyboardButton(text="📊 Статус", callback_data="status")],
        [InlineKeyboardButton(text="📈 Общая статистика", callback_data="overall")],
        [InlineKeyboardButton(text="🧙 Герои", callback_data="heroes_menu")],
        [InlineKeyboardButton(text="⚙️ Указать MMR", callback_data="set_mmr")],
    ]
    rows.insert(0, [InlineKeyboardButton(text=("🔁 Сменить Steam ID" if bound else "➕ Привязать Steam ID"),
                                         callback_data="bind_steam")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def heroes_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Топ по играм", callback_data="heroes_sort_games")],
        [InlineKeyboardButton(text="🎯 Топ по винрейту", callback_data="heroes_sort_wr")],
        [InlineKeyboardButton(text="⚔️ Топ по KDA", callback_data="heroes_sort_kda")],
    ])

# ===================== UI helpers =====================
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
        [InlineKeyboardButton(text="Открыть матч в OpenDota", url=f"https://www.opendota.com/matches/{mid}")]
    ]) if mid else None

    text = (
        "🎮 " + bold("Новая игра в Dota 2") + "\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🏆 Исход: {outcome}\n"
        f"🧙 Герой: {bold(hero)}\n"
        f"⚔️ KDA: {bold(f'{k}/{d}/{a}')}  ⏱ {bold(dur)}\n"
        f"📅 Время: {bold(when)}"
        f"{nw_line}{mmr_line}\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

def parse_rank_tier(rank_tier: Optional[int]) -> str:
    # rank_tier = (major*10 + minor), major: 0..8, minor: 0..5
    if not isinstance(rank_tier, int):
        return "—"
    major = rank_tier // 10
    minor = rank_tier % 10
    names = {
        1:"Herald",2:"Guardian",3:"Crusader",4:"Archon",
        5:"Legend",6:"Ancient",7:"Divine",8:"Immortal"
    }
    return f"{names.get(major, '?')} {minor}" if major in names else str(rank_tier)

# ===================== Bot =====================
bot = Bot(TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def on_start(m: Message, state: FSMContext):
    u = db_get_user(m.from_user.id)
    await m.answer(
        f"Привет, {bold(m.from_user.first_name)}!\n"
        "Привяжи Steam ID и я покажу статус, последние матчи, общую статистику и разбор героев. "
        "Отчёт дня приходит в 23:59 МСК. Чтобы начать — нажми кнопку ниже.",
        reply_markup=main_menu(bool(u and u.get("steam32"))),
        parse_mode="HTML"
    )

@dp.message(Command("debug"))
async def on_debug(m: Message):
    global _last_api_error
    u = db_get_user(m.from_user.id)
    await m.answer(
        f"steam32={u.get('steam32') if u else None}\n"
        f"last_api_error={_last_api_error}", parse_mode=None
    )

@dp.callback_query(F.data == "bind_steam")
async def on_bind(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Пришли свой Steam ID:\n• Можно Steam64 (начинается с 7656119...) или Steam32 (7–10 цифр)")
    await state.set_state(BindSteam.wait_steam); await cb.answer()

@dp.message(BindSteam.wait_steam)
async def on_bind_value(m: Message, state: FSMContext):
    steam32 = to_steam32(m.text)
    if not steam32:
        await m.answer("Это не похоже на корректный Steam ID. Пришли число (Steam32 или Steam64).")
        return

    async with aiohttp.ClientSession() as sess:
        player = await fetch_player(sess, steam32)

    if not player or not player.get("profile"):
        await m.answer("OpenDota не нашла профиль. Убедись, что профиль Dota публичный "
                       "и авторизуйся на opendota.com через Steam.")
        return

    db_set_user_steam(m.from_user.id, steam32)

    # Получим оценочный MMR (если есть)
    est = player.get("mmr_estimate", {}).get("estimate")
    if isinstance(est, (int, float)):
        db_set_user_mmr(m.from_user.id, int(est))

    await state.clear()
    await m.answer(
        f"Готово! Привязал Steam32: {bold(steam32)}\n"
        f"Оценка MMR: {bold(str(int(est))) if isinstance(est,(int,float)) else '—'}",
        reply_markup=main_menu(True),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "status")
async def on_status(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam ID."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        player = await fetch_player(sess, u["steam32"])
    prof = (player or {}).get("profile") or {}
    rank_tier = player.get("rank_tier")
    mmr_est = player.get("mmr_estimate", {}).get("estimate")

    text = (
        f"👤 Ник: {bold(prof.get('personaname') or '—')}\n"
        f"🆔 Steam32: {bold(u['steam32'])}\n"
        f"🏅 Ранг: {bold(parse_rank_tier(rank_tier))}\n"
        f"📊 Оценочный MMR: {bold(str(int(mmr_est))) if isinstance(mmr_est,(int,float)) else '—'}\n"
        f"🧾 Последний матч id: {bold(str(u['last_match_id'])) if u.get('last_match_id') else '—'}"
    )
    await cb.message.answer(text, parse_mode="HTML"); await cb.answer()

@dp.callback_query(F.data == "overall")
async def on_overall(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam ID."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        wl = await fetch_player_wl(sess, u["steam32"])
        totals = await fetch_player_totals(sess, u["steam32"])

    wins = wl.get("win", 0) if wl else 0
    loses = wl.get("lose", 0) if wl else 0
    games = wins + loses
    wr = round(100 * wins / games) if games else 0

    # средняя K/D/A по totals
    k = d = a = 0
    if totals:
        for item in totals:
            if item.get("field") == "kills": k = item.get("sum", 0)
            if item.get("field") == "deaths": d = item.get("sum", 0)
            if item.get("field") == "assists": a = item.get("sum", 0)
    avg_k = round(k / games, 1) if games else 0
    avg_d = round(d / games, 1) if games else 0
    avg_a = round(a / games, 1) if games else 0

    text = (
        "📈 " + bold("Общая статистика") + "\n"
        f"• Всего игр: {bold(str(games))}\n"
        f"• Побед / Поражений: {bold(str(wins))} / {bold(str(loses))}\n"
        f"• Winrate: {bold(str(wr))}%\n"
        f"• Средняя K/D/A: {bold(f'{avg_k}/{avg_d}/{avg_a}')}\n"
        "• *Примечание:* дата самой первой игры недоступна напрямую у OpenDota; "
        "показываются агрегаты по всей базе."
    )
    await cb.message.answer(text, parse_mode="HTML"); await cb.answer()

@dp.callback_query(F.data == "heroes_menu")
async def on_heroes_menu(cb: CallbackQuery):
    await cb.message.answer("Выбери способ сортировки:", reply_markup=heroes_keyboard())
    await cb.answer()

async def render_heroes(cb: CallbackQuery, sort_by: str):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam ID."); await cb.answer(); return

    async with aiohttp.ClientSession() as sess:
        hero_map = await fetch_heroes(sess)
        data = await fetch_player_heroes(sess, u["steam32"])

    if not data:
        await cb.message.answer("Нет данных по героям. Возможно, профиль приватный.")
        await cb.answer(); return

    # Считаем kda, wr, и сортируем
    rows = []
    for it in data:
        hid = it.get("hero_id", 0)
        games = it.get("games", 0) or 0
        wins = it.get("win", 0) or 0
        k = it.get("k", 0) or 0
        d = it.get("d", 0) or 0
        a = it.get("a", 0) or 0
        wr = (wins / games * 100) if games else 0.0
        kda = (k + a) / max(1, d)
        rows.append({
            "hero": hero_map.get(hid, f"Hero {hid}"),
            "games": games,
            "wins": wins,
            "wr": wr,
            "kda": kda
        })

    if sort_by == "games":
        rows.sort(key=lambda x: x["games"], reverse=True)
    elif sort_by == "wr":
        rows = [r for r in rows if r["games"] >= 10]  # чтобы не было фейков по 1 игре
        rows.sort(key=lambda x: (x["wr"], x["games"]), reverse=True)
    elif sort_by == "kda":
        rows = [r for r in rows if r["games"] >= 10]
        rows.sort(key=lambda x: (x["kda"], x["games"]), reverse=True)

    top = rows[:15]
    lines = [f"🧙 {bold('Герои — топ 15 (' + ('игры' if sort_by=='games' else 'винрейт' if sort_by=='wr' else 'KDA') + ')')}"]
    for i, r in enumerate(top, 1):
        lines.append(
            f"{i}) {r['hero']} — игр: {r['games']}, WR: {r['wr']:.0f}%, KDA: {r['kda']:.2f}"
        )
    await cb.message.answer("\n".join(lines), parse_mode="HTML")

@dp.callback_query(F.data == "heroes_sort_games")
async def on_heroes_games(cb: CallbackQuery):
    await render_heroes(cb, "games"); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_wr")
async def on_heroes_wr(cb: CallbackQuery):
    await render_heroes(cb, "wr"); await cb.answer()

@dp.callback_query(F.data == "heroes_sort_kda")
async def on_heroes_kda(cb: CallbackQuery):
    await render_heroes(cb, "kda"); await cb.answer()

@dp.callback_query(F.data == "set_mmr")
async def on_set_mmr(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Пришли число — твой текущий MMR. Пример: 3770")
    await state.set_state(SetMMR.wait_mmr); await cb.answer()

@dp.message(SetMMR.wait_mmr)
async def on_set_mmr_value(m: Message, state: FSMContext):
    try:
        val = int((m.text or "").strip())
    except Exception:
        await m.answer("Не понял число. Пример: 3770"); return
    db_set_user_mmr(m.from_user.id, val)
    await state.clear()
    await m.answer(f"Ок! Текущий MMR установлен: {bold(str(val))}",
                   reply_markup=main_menu(True), parse_mode="HTML")

@dp.callback_query(F.data == "last10")
async def on_last10(cb: CallbackQuery):
    u = db_get_user(cb.from_user.id)
    if not u or not u.get("steam32"):
        await cb.message.answer("Сначала привяжи Steam ID."); await cb.answer(); return

    rows = db_get_last_matches(u["steam32"], 10)
    if not rows:
        # подгрузим и закэшируем
        async with aiohttp.ClientSession() as sess:
            hero_map = await fetch_heroes(sess)
            arr = await fetch_latest_matches(sess, u["steam32"], 10)
            if not arr:
                await cb.message.answer("Матчи не найдены. Если профиль приватный — открой игровой профиль в Dota 2.")
                await cb.answer(); return
            for m in arr:
                nw = None
                detail = await fetch_match_detail(sess, m.get("match_id"))
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

    lines = ["🕓 " + bold("Последние 10 матчей:")]
    for i, r in enumerate(rows, 1):
        hero = hero_map.get(r["hero_id"], f"Hero {r['hero_id']}")
        win = is_win(r["player_slot"], bool(r["radiant_win"]))
        flag = "✅" if win else "❌"
        date = ts_to_msk(r["when_ts"]).split(" ")[0]
        delta = r["delta_mmr"]
        after = r["mmr_after"]
        dlt = f" {delta:+d}" if isinstance(delta, int) else ""
        aft = f" ({after})" if isinstance(after, int) else ""
        lines.append(f"{i}) {hero} | {date} | {flag}{dlt}{aft}")
    await cb.message.answer("\n".join(lines), parse_mode="HTML"); await cb.answer()

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
                    if not arr:
                        continue
                    m = arr[0]
                    mid = m.get("match_id")
                    if not mid or u.get("last_match_id") == mid:
                        continue

                    win = is_win(m.get("player_slot", 0), m.get("radiant_win", False))
                    delta = ASSUMED_MMR_DELTA if win else -ASSUMED_MMR_DELTA
                    curr = u.get("current_mmr")
                    new_curr = curr + delta if isinstance(curr, int) else None
                    if isinstance(new_curr, int):
                        db_set_user_mmr(tlg, new_curr)

                    # net worth
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
    if target_msk <= now_msk:
        target_msk += timedelta(days=1)
    return max(5, int((target_msk - now_msk).total_seconds()))

async def daily_summary_worker():
    await asyncio.sleep(5)
    while True:
        try:
            await asyncio.sleep(seconds_until_2359_msk())
            users = db_get_all_users_with_steam()
            if not users:
                continue

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
                    arr = await od_get(sess, f"/players/{steam}/matches", params={
                        "limit": 50, **({"lobby_type": 7} if TRACK_RANKED_ONLY else {})
                    }) or []
                    today = [m for m in arr if start_ts <= m.get("start_time", 0) <= end_ts]
                    if not today:
                        continue
                    wins = sum(1 for m in today if is_win(m.get("player_slot", 0), m.get("radiant_win", False)))
                    loses = len(today) - wins
                    delta = wins * ASSUMED_MMR_DELTA + loses * (-ASSUMED_MMR_DELTA)
                    curr = u.get("current_mmr")
                    wr = round(100 * wins / len(today)) if today else 0
                    text = (
                        "📊 " + bold(f"Итоги дня ({now_msk.strftime('%d.%m.%Y')})") + "\n"
                        f"• Сыграно игр: {bold(str(len(today)))}\n"
                        f"• Побед: {bold(str(wins))} / Поражений: {bold(str(loses))}\n"
                        f"• Δ MMR за день: {bold(f'{delta:+d}')}\n"
                        f"• Winrate: {bold(str(wr))}%\n"
                        f"• Текущий рейтинг: {bold(str(curr)) if curr is not None else '—'}"
                    )
                    await bot.send_message(tlg, text, parse_mode="HTML")
        except Exception as e:
            print("daily_summary_worker error:", e)
            await asyncio.sleep(10)

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
