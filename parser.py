"""
TG Parser — single account, config fully in Google Sheets, GitHub Actions cron (*/20 * * * *).

GitHub Secrets (минимум):
  GOOGLE_CREDENTIALS_BASE64
  SPREADSHEET_ID

Всё остальное — в таблице.

Структура таблицы:
  Каналы:    A2+ = username / https://t.me/xxx  (ТОЛЬКО публичные каналы с username,
                                                   числовые -100... id без ссылки игнорируются)
  Настройки: лист "Параметр / Значение / Описание", фиксированные строки:
             B2 = TG-бот токен
             B3 = Чат-получатель (один chat_id)
             B4 = API_ID
             B5 = API_HASH
             B6 = STRING_SESSION
             B7 = Порог скора
             B8 = Мин. длина текста
  Минус-слова: A2+ = слово/фраза, B2+ = комментарий (необязательно)
  Скоринг:     A2+ = категория, B2+ = вес, C2+ = ключевые слова через запятую
  Кеш:       A=username  B=entity_id  C=chat_name      ← ЧИТАЕТСЯ и ПИШЕТСЯ
  Посты:     A=дата B=канал C=автор D=аккаунт E=ссылка F=текст G=скор
  Логи:      A=дата B=уровень C=сообщение
  Состояние: A=username B=last_id

Фильтрация постов (без AI, по модели monitor_tg_2/utils.py):
  0. Медиаконтент: посты с фото/документами/альбомами пропускаются сразу,
     без скачивания и без отправки (с вероятностью 99,9% не подходят).
  1. Минус-слова: если найдено хотя бы одно — пост отбрасывается.
     Короткие слова (<=4 символа) ищутся по границе кириллица/латиница,
     длинные — простым substring-поиском.
  2. Мин. длина текста: если короче порога — отбрасывается.
  3. Скоринг: по каждой категории из листа "Скоринг" — если хотя бы одно
     ключевое слово/фраза найдено (substring; поддержка wildcard "слово*"
     как префиксный поиск) — категория даёт свой вес один раз. Сумма весов
     по всем категориям = итоговый скор поста.
  4. Если скор >= "Порог скора" — публикуем, иначе отбрасываем.
"""

import asyncio
import base64
import json
import logging
import os
import random
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError, FloodWaitError,
    UsernameInvalidError, UsernameNotOccupiedError,
)
from telethon.sessions import StringSession

# ── config (из GitHub Secrets — минимум) ───────────────────────────────────────
GS_B64         = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '').strip()
MSG_LIMIT      = int(os.environ.get('MESSAGES_LIMIT', '10'))
MAX_BACKLOG    = int(os.environ.get('MAX_BACKLOG', '100'))

DELAY_MIN = 1.5   # минимальная пауза между каналами (сек)
DELAY_MAX = 3.0   # максимальная пауза (джиттер)
STATE_SAVE_INTERVAL = 20  # сохранять state каждые N каналов

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)
pool = ThreadPoolExecutor(max_workers=8)


# ── google sheets ──────────────────────────────────────────────────────────────

def _gs_open(ss_id):
    creds = Credentials.from_service_account_info(
        json.loads(base64.b64decode(GS_B64)),
        scopes=['https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'],
    )
    return gspread.authorize(creds).open_by_key(ss_id)


def _gs_retry(fn, *args, retries=3, delay=10, **kwargs):
    """Повторяет вызов fn при временных ошибках Sheets API (503, quota)."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if any(x in msg for x in ('invalid', 'permission', 'not found', 'credentials')):
                raise
            log.warning(f'GS retry {attempt + 1}/{retries}: {e}')
            time.sleep(delay * (attempt + 1))
    raise last_exc


def _gs_settings(ss):
    """Читает лист Настройки (формат Параметр/Значение/Описание, фиксированные строки):
       B2=токен, B3=chat_id, B4=API_ID, B5=API_HASH, B6=SESSION,
       B7=порог скора, B8=мин. длина текста.
    """
    try:
        d = ss.worksheet('Настройки').get_all_values()

        def cell(row, col):
            return d[row][col].strip() if len(d) > row and len(d[row]) > col else ''

        token       = cell(1, 1)   # B2
        chat_id     = cell(2, 1)   # B3
        api_id_raw  = cell(3, 1)   # B4
        api_hash    = cell(4, 1)   # B5
        session     = cell(5, 1)   # B6
        threshold_r = cell(6, 1)   # B7
        min_len_r   = cell(7, 1)   # B8

        try:
            score_threshold = int(float(threshold_r)) if threshold_r else 7
        except ValueError:
            score_threshold = 7
        try:
            min_len = int(float(min_len_r)) if min_len_r else 0
        except ValueError:
            min_len = 0

        log.info(f'token:{"OK" if token else "NO"} chat:{"OK" if chat_id else "NO"} '
                  f'account:{"OK" if (api_id_raw and api_hash and session) else "MISSING"} '
                  f'threshold:{score_threshold} min_len:{min_len}')

        return dict(
            token=token, chats=[chat_id] if chat_id else [],
            api_id=int(api_id_raw) if api_id_raw.isdigit() else None,
            api_hash=api_hash, session=session,
            score_threshold=score_threshold, min_len=min_len,
        )
    except Exception as e:
        log.error(f'settings: {e}')
        return None


def _gs_minus_words(ss):
    """Читает лист Минус-слова: A2+ = слово/фраза."""
    try:
        ws = ss.worksheet('Минус-слова')
        words = [r[0].strip() for r in ws.get_all_values()[1:] if r and r[0].strip()]
        log.info(f'minus-words: {len(words)}')
        return words
    except Exception as e:
        log.error(f'minus_words: {e}')
        return []


def _gs_scoring(ss):
    """Читает лист Скоринг: A2+=категория, B2+=вес, C2+=ключевые слова через запятую."""
    try:
        ws = ss.worksheet('Скоринг')
        rules = []
        for r in ws.get_all_values()[1:]:
            if not r or not r[0].strip():
                continue
            try:
                weight = int(float(r[1].strip())) if len(r) > 1 and r[1].strip() else 0
            except ValueError:
                weight = 0
            kws_raw = r[2].strip() if len(r) > 2 else ''
            keywords = [k.strip().lower() for k in kws_raw.split(',') if k.strip()]
            if keywords and weight:
                rules.append(dict(category=r[0].strip(), weight=weight, keywords=keywords))
        log.info(f'scoring rules: {len(rules)}')
        return rules
    except Exception as e:
        log.error(f'scoring: {e}')
        return []


def _gs_channels(ss):
    """Читает лист Каналы. Пропускает строки без username (числовые -100... id без ссылки)."""
    try:
        raws = [r[0].strip() for r in ss.worksheet('Каналы').get_all_values()[1:]
                if r and r[0].strip()]
        good, skipped = [], []
        for raw in raws:
            uname = _parse_username(raw)
            if uname and not re.match(r'^-?\d+$', uname):
                good.append(raw)
            else:
                skipped.append(raw)
        if skipped:
            log.warning(f'channels skipped (no username, numeric id w/o link): {skipped}')
        return good
    except Exception as e:
        log.error(f'channels: {e}')
        return []


def _gs_read_cache_full(ss):
    """Читает лист Кеш → {username: (entity_id, chat_name)}"""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
            ws.append_row(['username', 'entity_id', 'chat_name'])
            return {}
        cache = {}
        for r in ws.get_all_values()[1:]:
            if len(r) >= 2 and r[0].strip() and r[1].strip():
                try:
                    eid = int(float(r[1].strip()))
                    name = r[2].strip() if len(r) > 2 else ''
                    cache[r[0].strip()] = (eid, name)
                except ValueError:
                    pass
        log.info(f'entity cache: {len(cache)} entries')
        return cache
    except Exception as e:
        log.error(f'read_cache: {e}')
        return {}


def _gs_write_cache(ss, cache: dict):
    """Перезаписывает лист Кеш полностью."""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
        rows = [['username', 'entity_id', 'chat_name']]
        rows += [[u, str(eid), name] for u, (eid, name) in cache.items()]
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'entity cache saved: {len(cache)}')
    except Exception as e:
        log.error(f'write_cache: {e}')


def _gs_read_state(ss):
    try:
        try:
            ws = ss.worksheet('Состояние')
        except Exception:
            ws = ss.add_worksheet('Состояние', 1000, 2)
            ws.append_row(['username', 'last_id'])
            return {}
        state = {}
        for r in ws.get_all_values()[1:]:
            if len(r) >= 2 and r[0].strip() and r[1].strip():
                try:
                    state[r[0].strip()] = int(float(r[1].strip()))
                except ValueError:
                    pass
        log.info(f'state: {len(state)} channels')
        return state
    except Exception as e:
        log.error(f'read_state: {e}')
        return {}


def _gs_write_state(ss, state):
    try:
        try:
            ws = ss.worksheet('Состояние')
        except Exception:
            ws = ss.add_worksheet('Состояние', 1000, 2)
        rows = [['username', 'last_id']] + [[u, str(v)] for u, v in state.items()]
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'state saved: {len(state)}')
    except Exception as e:
        log.error(f'write_state: {e}')


def _gs_write_post(ss, date, channel, author, account, link, text, score):
    try:
        ss.worksheet('Посты').append_row(
            [date.strftime('%Y-%m-%d %H:%M:%S'), channel, author, account, link, text, score],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error(f'write_post: {e}')


def _gs_read_recent(ss):
    try:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=3)
        texts = set()
        for r in ss.worksheet('Посты').get_all_values()[1:]:
            if len(r) < 6:
                continue
            try:
                dt = datetime.strptime(r[0].strip(), '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                if dt >= cutoff:
                    texts.add(' '.join(r[5].lower().split()))
            except ValueError:
                pass
        log.info(f'dedup: {len(texts)} posts')
        return texts
    except Exception as e:
        log.error(f'read_recent: {e}')
        return set()


LOGS_MAX_ROWS = 500  # максимум строк в листе Логи


def _gs_log(ss, level, msg):
    try:
        ws = ss.worksheet('Логи')
        ws.append_row(
            [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, str(msg)],
            value_input_option='USER_ENTERED',
        )
        all_rows = ws.get_all_values()
        if len(all_rows) > LOGS_MAX_ROWS + 1:
            excess = len(all_rows) - LOGS_MAX_ROWS - 1
            ws.delete_rows(2, excess + 1)
    except Exception:
        pass


# ── utils ──────────────────────────────────────────────────────────────────────

def _parse_username(raw):
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([A-Za-z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^-?100\d+$', raw) or re.match(r'^-?\d+$', raw):
        return raw  # числовой id — отфильтровывается выше в _gs_channels
    if re.match(r'^[A-Za-z0-9_]+$', raw):
        return raw
    return None


def _find_minus_word(text: str, minus_words: list):
    """Из monitor_tg_2/utils.py: короткие (<=4) — по границе кириллица/латиница,
       длинные — простым substring-поиском."""
    lower = text.lower()
    for w in minus_words:
        if not w:
            continue
        if len(w) <= 4:
            if re.search(r'(?<![а-яёa-z])' + re.escape(w) + r'(?![а-яёa-z])', lower):
                return w
        else:
            if w in lower:
                return w
    return None


def _calc_score(text: str, rules: list) -> int:
    """Из monitor_tg_2/utils.py: substring-поиск, wildcard 'слово*' = префикс,
       вес категории засчитывается один раз при первом совпадении."""
    lower = text.lower()
    total = 0
    for rule in rules:
        for kw in rule['keywords']:
            if kw.endswith('*'):
                if kw[:-1] in lower:
                    total += rule['weight']
                    break
            else:
                if kw in lower:
                    total += rule['weight']
                    break
    return total


def _passes_filters(text, minus_words, scoring_rules, min_len, score_threshold, label_for_log=''):
    """Полный пайплайн: минус-слова → мин. длина → скоринг → порог.
       Возвращает (passed: bool, score: int)."""
    if not text.strip():
        return False, 0

    hit = _find_minus_word(text, minus_words)
    if hit:
        log.info(f'{label_for_log} skip: минус-слово "{hit}"')
        return False, 0

    if len(text) < min_len:
        log.info(f'{label_for_log} skip: текст короче {min_len} символов ({len(text)})')
        return False, 0

    score = _calc_score(text, scoring_rules)
    if score < score_threshold:
        return False, score

    return True, score


def _make_link(uname, msg_id):
    return f'https://t.me/{uname}/{msg_id}'


def _get_sender(m):
    """Возвращает (author_name, account_link) из сообщения."""
    sender = getattr(m, 'sender', None) or getattr(m, 'from_id', None)
    if sender is None:
        return '', ''
    first = getattr(sender, 'first_name', '') or ''
    last  = getattr(sender, 'last_name',  '') or ''
    uname = getattr(sender, 'username',   '') or ''
    author  = (first + ' ' + last).strip() or uname or ''
    account = f'https://t.me/{uname}' if uname else ''
    return author, account


def _has_media(m):
    """Пост содержит фото/документ/альбом — такие посты пропускаем целиком."""
    return bool(m.photo or m.document or getattr(m, 'grouped_id', None))


# ── telegram send (bot API) ──────────────────────────────────────────────────────

def _bot_request(req, timeout, label):
    """Выполняет запрос к Bot API с retry при 429."""
    for attempt in range(5):
        try:
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get('Retry-After', 15))
                log.warning(f'[{label}] Bot API 429 — waiting {retry_after}s')
                time.sleep(retry_after + 1)
            else:
                raise
    log.error(f'[{label}] Bot API failed after 5 attempts')
    return False


def _tg_text(token, chats, text):
    for chat in chats:
        try:
            data = json.dumps({
                'chat_id': chat,
                'text': text[:4096],
                'disable_web_page_preview': False,
            }).encode()
            req = urllib.request.Request(
                f'https://api.telegram.org/bot{token}/sendMessage', data=data,
                headers={'Content-Type': 'application/json'})
            _bot_request(req, timeout=10, label=f'tg_text {chat}')
            time.sleep(0.3)
        except Exception as e:
            log.error(f'tg_text {chat}: {e}')


# ── safe TG call ───────────────────────────────────────────────────────────────

async def _tg_call(fn, *args, label='', **kwargs):
    """FloodWait <= 120s: ждёт и повторяет. > 120s: возвращает None."""
    for attempt in range(3):
        try:
            return await fn(*args, **kwargs)
        except FloodWaitError as e:
            if e.seconds > 120:
                log.warning(f'[{label}] FloodWait {e.seconds}s > 120 — skip channel')
                return None
            log.warning(f'[{label}] FloodWait {e.seconds}s — waiting...')
            await asyncio.sleep(e.seconds + 3)
        except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:
            log.warning(f'[{label}] unavailable: {e}')
            return None
        except Exception as e:
            log.error(f'[{label}] error attempt {attempt + 1}: {e}')
            if attempt < 2:
                await asyncio.sleep(5)
    return None


# ── main worker ────────────────────────────────────────────────────────────────

async def run(channels: list, ss, cfg, state, cache, dedup, minus_words, scoring_rules):
    label = 'ACC'
    loop  = asyncio.get_event_loop()

    client = TelegramClient(
        StringSession(cfg['session']),
        cfg['api_id'],
        cfg['api_hash'],
    )

    def _no_input(prompt=''):
        raise RuntimeError(f'[{label}] session expired or invalid — regenerate session in Настройки!B6')

    try:
        await client.start(phone=_no_input, password=_no_input)
    except RuntimeError as e:
        log.error(str(e))
        return
    except Exception as e:
        log.error(f'[{label}] connect failed: {e}')
        return
    log.info(f'[{label}] connected, channels: {len(channels)}')

    channels = channels.copy()
    random.shuffle(channels)

    processed = 0
    cache_dirty = False

    try:
        for raw in channels:
            uname = _parse_username(raw)
            if not uname:
                continue

            # ── resolve entity (из кеша или get_entity по username) ────────
            if uname in cache:
                eid, _name = cache[uname]
            else:
                entity = await _tg_call(client.get_entity, uname, label=label)
                if entity is None:
                    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                    continue
                eid = abs(entity.id)
                chat_name = getattr(entity, 'title', uname)
                cache[uname] = (eid, chat_name)
                cache_dirty = True
                log.info(f'[{label}] [{uname}] entity resolved: {eid}')
                await asyncio.sleep(2.0)

            known_last = state.get(uname, 0)

            if known_last == 0:
                # первый запуск — просто запоминаем last_id текущего верхнего поста, ничего не шлём
                messages = await _tg_call(client.get_messages, uname,
                                          limit=MSG_LIMIT, label=label)
                if not messages:
                    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                    continue
                state[uname] = messages[0].id
                log.info(f'[{label}] [{uname}] first run, last_id={messages[0].id}')
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                processed += 1
                continue

            # ── забираем ВСЮ разницу от known_last до текущего верха, не только MSG_LIMIT штук ──
            new_msgs = await _tg_call(client.get_messages, uname,
                                      min_id=known_last, limit=MAX_BACKLOG, label=label)
            if new_msgs is None:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                continue
            if not new_msgs:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                processed += 1
                continue

            if len(new_msgs) >= MAX_BACKLOG:
                log.warning(f'[{label}] [{uname}] backlog >= {MAX_BACKLOG}, часть постов будет '
                            f'добрана на следующих прогонах')

            new_msgs = sorted(
                [m for m in new_msgs if m.action is None],
                key=lambda m: m.id,
            )
            if not new_msgs:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                processed += 1
                continue

            media_skipped = 0
          
            for m in new_msgs:
                # ── медиаконтент (фото/документы/альбомы) — сразу пропускаем ──
                if _has_media(m):
                    media_skipped += 1
                    continue

                text = m.text or m.message or ''
                text = ' '.join(text.split())

                if not text.strip():
                    continue
                norm = ' '.join(text.lower().split())
                if norm in dedup:
                    continue

                passed, score = _passes_filters(
                    text, minus_words, scoring_rules,
                    cfg['min_len'], cfg['score_threshold'],
                    label_for_log=f'[{label}] [{uname}]')
                if not passed:
                    dedup.add(norm)
                    continue

                link = _make_link(uname, m.id)
                author, account = _get_sender(m)
                author_line = f'👤 {author}  {account}'.strip() if author or account else ''
                body = (f'📢 {uname}\n{author_line}\n\n{text}\n\n🔗 {link}'
                        if author_line else f'📢 {uname}\n\n{text}\n\n🔗 {link}')

                token = cfg['token']
                dest  = cfg['chats']
                if token and dest:
                    await loop.run_in_executor(
                        pool, _tg_text, token, dest, body)

                await loop.run_in_executor(
                    pool, _gs_write_post, ss,
                    m.date.replace(tzinfo=None), uname, author, account, link, text, score)
                dedup.add(norm)
                log.info(f'[{label}] sent {uname} {link} score={score}')

            if media_skipped:
                log.info(f'[{label}] [{uname}] media skipped: {media_skipped}')

            if new_msgs:
                state[uname] = new_msgs[-1].id
            processed += 1

            # ── периодическое сохранение state ────────────────────────────
            if processed % STATE_SAVE_INTERVAL == 0:
                await loop.run_in_executor(pool, _gs_write_state, ss, state)
                log.info(f'[{label}] periodic state save at {processed} channels')

            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    finally:
        await client.disconnect()
        log.info(f'[{label}] disconnected')

    return cache_dirty


# ── entrypoint ───────────────────────────────────────────────────────────────────

async def main():
    if not SPREADSHEET_ID:
        log.error('SPREADSHEET_ID not set')
        return

    loop = asyncio.get_event_loop()

    try:
        ss = await loop.run_in_executor(pool, _gs_retry, _gs_open, SPREADSHEET_ID)
    except Exception as e:
        log.error(f'open failed: {e}')
        return

    cfg = await loop.run_in_executor(pool, _gs_retry, _gs_settings, ss)
    if not cfg:
        log.error('settings read failed')
        return
    if not (cfg['api_id'] and cfg['api_hash'] and cfg['session']):
        log.error('account credentials missing in Настройки (B4/B5/B6)')
        return

    state = await loop.run_in_executor(pool, _gs_retry, _gs_read_state, ss)
    cache = await loop.run_in_executor(pool, _gs_retry, _gs_read_cache_full, ss)
    dedup = await loop.run_in_executor(pool, _gs_retry, _gs_read_recent, ss)
    channels = await loop.run_in_executor(pool, _gs_retry, _gs_channels, ss)
    minus_words = await loop.run_in_executor(pool, _gs_retry, _gs_minus_words, ss)
    scoring_rules = await loop.run_in_executor(pool, _gs_retry, _gs_scoring, ss)

    if not channels:
        log.error('no channels')
        return

    log.info(f'total channels: {len(channels)}')

    cache_dirty = await run(channels, ss, cfg, state, cache, dedup, minus_words, scoring_rules)

    # ── финальное сохранение state и кеша ────────────────────────────────
    await loop.run_in_executor(pool, _gs_write_state, ss, state)
    if cache_dirty:
        await loop.run_in_executor(pool, _gs_write_cache, ss, cache)
    await loop.run_in_executor(pool, _gs_log, ss, 'INFO',
                               f'done | каналов: {len(channels)}')

    log.info('all done')


if __name__ == '__main__':
    asyncio.run(main())
