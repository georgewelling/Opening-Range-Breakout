import MetaTrader5 as mt5
from datetime import datetime, timedelta, time as dtime, timezone
import time
import logging
from collections import defaultdict
from time import monotonic

try:
    from zoneinfo import ZoneInfo
    TZ_LONDON = ZoneInfo("Europe/London")
except Exception:
    TZ_LONDON = None
    try:
        import pytz  
        TZ_LONDON = pytz.timezone("Europe/London")
    except Exception:
        raise RuntimeError("Timezone support not available. Install Python 3.9+ (zoneinfo) or 'pytz'.")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    level=LOG_LEVEL,
)
log = logging.getLogger("orb")

_last_msgs = {}
_last_time = defaultdict(lambda: 0.0)

def log_once(key: str, message: str, level=logging.INFO, min_interval=30.0):
    now = monotonic()
    last_m = _last_msgs.get(key)
    if message != last_m or (now - _last_time[key]) >= min_interval:
        _last_msgs[key] = message
        _last_time[key] = now
        log.log(level, message)


def ensure_connected():
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    if not mt5.symbol_select(SYMBOL, True):
        raise RuntimeError(f"Cannot select symbol {SYMBOL}")
    info = mt5.symbol_info(SYMBOL)
    if not info or not info.trade_mode:
        raise RuntimeError(f"Symbol not tradeable or no info for {SYMBOL}")
    return info

def get_rates(symbol: str, timeframe: int, count: int):
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
    return list(rates) if rates is not None else []

def normalize_price(price: float, digits: int) -> float:
    return round(float(price), digits)

def round_volume(vol: float, step: float, vmin: float, vmax: float = None) -> float:
    if vol <= 0:
        vol = vmin
    steps = max(1, round(vol / step))
    v = round(steps * step, 8)
    v = max(v, vmin)
    if vmax is not None:
        v = min(v, vmax)
    return v

def already_traded_today(tag_key: int) -> bool:
    poss = mt5.positions_get(symbol=SYMBOL)
    if poss:
        for p in poss:
            if getattr(p, "magic", 0) == MAGIC and str(tag_key) in getattr(p, "comment", ""):
                return True
    ords = mt5.orders_get(symbol=SYMBOL)
    if ords:
        for o in ords:
            if getattr(o, "magic", 0) == MAGIC and str(tag_key) in getattr(o, "comment", ""):
                return True
    now_ldn = datetime.now(TZ_LONDON)
    now_utc = datetime.utcnow().replace(tzinfo=TZ_UTC).replace(tzinfo=None)
    start_utc = (datetime.utcnow() - timedelta(days=5)).replace(tzinfo=None)
    deals = mt5.history_deals_get(start_utc, now_utc)
    if deals:
        for d in deals:
            if getattr(d, "symbol", "") == SYMBOL and getattr(d, "magic", 0) == MAGIC and str(tag_key) in getattr(d, "comment", ""):
                return True
    return False

def cancel_gtd_orders_for_symbol(tag_prefix=None):
    ords = mt5.orders_get(symbol=SYMBOL)
    if not ords:
        return
    for o in ords:
        if getattr(o, "magic", 0) != MAGIC:
            continue
        if tag_prefix and tag_prefix not in getattr(o, "comment", ""):
            continue
        mt5.order_delete(o.ticket)


def london_now():
    return datetime.now(TZ_LONDON)

def london_today_bounds():
    now = london_now()
    day_start = datetime.combine(now.date(), dtime(0, 0), TZ_LONDON)
    day_end   = day_start + timedelta(days=1)
    return day_start, day_end

def session_window():
    day_start, _ = london_today_bounds()
    tag_key = int(day_start.strftime("%Y%m%d"))

    if USE_NY_OPEN:
        hh, mm = LONDON_OPEN_HHMM
        orb_start = datetime.combine(day_start.date(), dtime(hh, mm), TZ_LONDON)
    else:
        orb_start = day_start

    orb_end = orb_start + timedelta(minutes=ORB_MINUTES)
    t_expire = datetime.combine(day_start.date(), dtime(23, 59), TZ_LONDON)
    return orb_start, orb_end, t_expire, tag_key


def to_london_from_epoch_utc(epoch_seconds: int) -> datetime:
    return datetime.fromtimestamp(int(epoch_seconds), TZ_UTC).astimezone(TZ_LONDON)

def compute_orb(m5_bars, orb_start_dt: datetime, orb_end_dt: datetime):
    sel = []
    for r in m5_bars:
        t_ldn = to_london_from_epoch_utc(r["time"])
        if orb_start_dt <= t_ldn < orb_end_dt:
            sel.append(r)

    need = max(2, ORB_MINUTES // 5)
    if len(sel) < need:
        return None, None

    hi = max(b["high"] for b in sel)
    lo = min(b["low"] for b in sel)
    if hi <= lo:
        return None, None
    return hi, lo

def range_is_sane(orb_high, orb_low, point):
    if orb_high is None or orb_low is None:
        return False
    rng_pts = (orb_high - orb_low) / point
    if MIN_ORB_POINTS is not None and rng_pts < MIN_ORB_POINTS:
        return False
    if MAX_ORB_POINTS is not None and rng_pts > MAX_ORB_POINTS:
        return False
    return True

def fixed_volume(info) -> float:
    vmin  = info.volume_min
    vstep = info.volume_step
    vmax  = getattr(info, "volume_max", None)
    return round_volume(FIXED_VOLUME, vstep, vmin, vmax)

def place_orb_breakout(orb_high, orb_low, rr=RR, tag_key=None, t_expire=None):
    info = mt5.symbol_info(SYMBOL)
    tick = mt5.symbol_info_tick(SYMBOL)
    if not info or not tick:
        log.warning("No symbol info/tick.")
        return False

    digits = info.digits
    point  = info.point
    rng    = orb_high - orb_low
    if rng <= 0:
        log.warning("Invalid ORB range.")
        return False

    stops_gap  = (info.trade_stops_level  or 0) * point
    freeze_gap = (info.trade_freeze_level or 0) * point
    pend_gap   = max(stops_gap, freeze_gap)
    tiny       = max(2 * point, 0.0)
    buf        = (BREAK_BUFFER_POINTS or 0) * point

    breaking_up   = tick.ask >= (orb_high + buf)
    breaking_down = tick.bid <= (orb_low  - buf)

    tag = f"ORB{tag_key}" if tag_key else "ORB"
    filling_mode = getattr(info, "filling_mode", mt5.ORDER_FILLING_RETURN) or mt5.ORDER_FILLING_RETURN

    type_time = mt5.ORDER_TIME_GTC
    req_extra = {}
    if t_expire:
        exp_ts = int(t_expire.astimezone(TZ_UTC).timestamp())
        type_time = mt5.ORDER_TIME_SPECIFIED
        req_extra["expiration"] = exp_ts

    if breaking_up:
        entry = max(orb_high + buf, tick.ask + pend_gap + tiny)
        sl    = orb_low
        if sl > entry - (stops_gap + tiny):
            sl = entry - (stops_gap + tiny)

        risk = max(entry - sl, stops_gap + tiny)
        tp   = entry + max(rr * risk, (stops_gap + tiny))

        volume = fixed_volume(info)

        req = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": SYMBOL,
            "volume": volume,
            "type": mt5.ORDER_TYPE_BUY_STOP,
            "price": normalize_price(entry, digits),
            "sl":    normalize_price(sl, digits),
            "tp":    normalize_price(tp, digits),
            "deviation": DEVIATION,
            "magic": MAGIC,
            "comment": f"{tag}_RR{rr}",
            "type_time": type_time,
            "type_filling": filling_mode,
            **req_extra,
        }
        r = mt5.order_send(req)
        if r and r.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"BUY STOP placed | order={r.order} price={req['price']} sl={req['sl']} tp={req['tp']} vol={volume}")
            return True
        else:
            reason = getattr(r, "comment", "Unknown")
            log_once("order_error", f"BUY STOP not placed (retcode={getattr(r,'retcode',None)}): {reason}", min_interval=60.0)
            time.sleep(1.0)  
            return False

    if breaking_down:
        entry = min(orb_low - buf, tick.bid - pend_gap - tiny)
        sl    = orb_high
        if sl < entry + (stops_gap + tiny):
            sl = entry + (stops_gap + tiny)

        risk = max(sl - entry, stops_gap + tiny)
        tp   = entry - max(rr * risk, (stops_gap + tiny))

        volume = fixed_volume(info)

        req = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": SYMBOL,
            "volume": volume,
            "type": mt5.ORDER_TYPE_SELL_STOP,
            "price": normalize_price(entry, digits),
            "sl":    normalize_price(sl, digits),
            "tp":    normalize_price(tp, digits),
            "deviation": DEVIATION,
            "magic": MAGIC,
            "comment": f"{tag}_RR{rr}",
            "type_time": type_time,
            "type_filling": filling_mode,
            **req_extra,
        }
        r = mt5.order_send(req)
        if r and r.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"SELL STOP placed | order={r.order} price={req['price']} sl={req['sl']} tp={req['tp']} vol={volume}")
            return True
        else:
            reason = getattr(r, "comment", "Unknown")
            log_once("order_error", f"SELL STOP not placed (retcode={getattr(r,'retcode',None)}): {reason}", min_interval=60.0)
            time.sleep(1.0)
            return False

    log_once("placement", "No break yet; inside range or blocked by broker distances.", min_interval=60.0)
    return False

def main_loop():
    info = ensure_connected()
    log.info(f"Running ORB bot on {SYMBOL} | RR={RR} | ORB {ORB_MINUTES}m | NY_open={USE_NY_OPEN} | vol={FIXED_VOLUME}")

    last_session_key = None
    orb_done_this_day = False
    orb_high = None
    orb_low  = None

    while True:
        try:
            orb_start, orb_end, t_expire, tag_key = session_window()

            if last_session_key != tag_key:
                log.info(f"=== New Session {tag_key} (London) ===")
                log.info(f"ORB window: {orb_start.strftime('%H:%M %Z')}–{orb_end.strftime('%H:%M %Z')}")
                last_session_key = tag_key
                orb_done_this_day = False
                orb_high = None
                orb_low  = None
                cancel_gtd_orders_for_symbol(tag_prefix=f"ORB{tag_key}")

            if orb_done_this_day or already_traded_today(tag_key):
                time.sleep(5)
                continue

            m5 = get_rates(SYMBOL, mt5.TIMEFRAME_M5, 500)
            if len(m5) < 20:
                time.sleep(1)
                continue

            now_ldn = london_now()

            if now_ldn < orb_end:
                hi, lo = compute_orb(m5, orb_start, orb_end)
                orb_high, orb_low = hi, lo
                if hi and lo:
                    log_once(
                        "building_orb",
                        f"[Building ORB] Hi={hi:.2f} Lo={lo:.2f} (until {orb_end.strftime('%H:%M %Z')})",
                        min_interval=120.0
                    )
                else:
                    log_once(
                        "waiting_orb",
                        f"Waiting for ORB window bars... ({orb_start.strftime('%H:%M %Z')}–{orb_end.strftime('%H:%M %Z')})",
                        min_interval=120.0
                    )
                time.sleep(5)
                continue

            if orb_high is None or orb_low is None:
                hi, lo = compute_orb(m5, orb_start, orb_end)
                orb_high, orb_low = hi, lo
                if orb_high is None or orb_low is None:
                    log_once("no_orb", "No valid ORB computed; waiting.", min_interval=60.0)
                    time.sleep(5)
                    continue

            if not range_is_sane(orb_high, orb_low, info.point):
                log.info("ORB range outside sanity limits; skipping today.")
                orb_done_this_day = True
                time.sleep(5)
                continue

            if now_ldn > (orb_end + timedelta(minutes=MAX_WAIT_AFTER_BREAK_MIN)):
                log.info("Post-break window timeout. Skipping today.")
                orb_done_this_day = True
                time.sleep(5)
                continue

            placed = place_orb_breakout(orb_high, orb_low, rr=RR, tag_key=tag_key, t_expire=t_expire)
            if placed:
                log.info(f"Placed ORB order for session {tag_key}.")
                orb_done_this_day = True
            time.sleep(5)

        except Exception as e:
            log.error(f"Loop error: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main_loop()
