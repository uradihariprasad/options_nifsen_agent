#!/usr/bin/env python3
"""
TRADE TIGER INDEX PRO - Institutional AI-Powered NIFTY & SENSEX Trading Intelligence Terminal
"""

import os
import json
import time
import math
import asyncio
import logging
import threading
import statistics
from datetime import datetime, timedelta
from collections import deque
from functools import wraps

from flask import Flask, render_template, request, jsonify, session
from flask_sock import Sock
import requests

app = Flask(__name__)
app.secret_key = os.urandom(32)
sock = Sock(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# GLOBAL STATE & CONFIGURATION
# ============================================================

UPSTOX_BASE = "https://api.upstox.com/v2"

INSTRUMENT_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "SENSEX": "BSE_INDEX|SENSEX",
    "INDIA_VIX": "NSE_INDEX|India VIX",
    "NIFTY_FUT": "NSE_FO|NIFTY",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}

NIFTY_OPTION_BASE = "NSE_FO|NIFTY"

class MarketDataStore:
    """Thread-safe global market data store"""
    def __init__(self):
        self.lock = threading.Lock()
        self.nifty_price = 0
        self.sensex_price = 0
        self.vix_value = 0
        self.vix_change = 0
        self.nifty_volume = 0
        self.sensex_volume = 0
        self.nifty_vwap = 0
        self.sensex_vwap = 0
        self.nifty_open = 0
        self.nifty_high = 0
        self.nifty_low = 0
        self.nifty_close = 0
        self.sensex_open = 0
        self.sensex_high = 0
        self.sensex_low = 0
        self.sensex_close = 0
        self.nifty_candles_5m = deque(maxlen=500)
        self.sensex_candles_5m = deque(maxlen=500)
        self.option_chain = {}
        self.nifty_futures_oi = 0
        self.nifty_futures_oi_change = 0
        self.nifty_futures_volume = 0
        self.last_update = None
        self.market_open = False
        self.prev_vix = 0
        self.nifty_prev_close = 0
        self.sensex_prev_close = 0
        self.tick_history = deque(maxlen=2000)
        self.volume_profile = {}
        self.cumulative_delta = 0
        self.alerts = deque(maxlen=100)
        self.regime_history = deque(maxlen=50)
        self.smc_labels = []
        self.order_blocks = []
        self.fvg_zones = []
        self.liquidity_levels = []
        self.support_resistance = {"supports": [], "resistances": []}
        self.trade_setups = {"call": None, "put": None}
        self.ai_commentary = ""
        self.ai_decision = {}
        self.regime = {"state": "Initializing", "confidence": 0}

    def update(self, **kwargs):
        with self.lock:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)
            self.last_update = datetime.now()

    def get_snapshot(self):
        with self.lock:
            return {
                "nifty_price": self.nifty_price,
                "sensex_price": self.sensex_price,
                "vix_value": self.vix_value,
                "vix_change": self.vix_change,
                "nifty_volume": self.nifty_volume,
                "sensex_volume": self.sensex_volume,
                "nifty_vwap": self.nifty_vwap,
                "sensex_vwap": self.sensex_vwap,
                "nifty_open": self.nifty_open,
                "nifty_high": self.nifty_high,
                "nifty_low": self.nifty_low,
                "nifty_close": self.nifty_close,
                "sensex_open": self.sensex_open,
                "sensex_high": self.sensex_high,
                "sensex_low": self.sensex_low,
                "sensex_close": self.sensex_close,
                "last_update": self.last_update.isoformat() if self.last_update else None,
                "market_open": self.market_open,
            }

store = MarketDataStore()
active_sessions = {}

# ============================================================
# UPSTOX API CLIENT
# ============================================================

class UpstoxClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def validate_token(self):
        try:
            r = self.session.get(f"{UPSTOX_BASE}/user/profile", timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return {"valid": True, "user": data.get("data", {})}
            elif r.status_code == 401:
                return {"valid": False, "error": "Expired Token"}
            return {"valid": False, "error": "Invalid Token"}
        except Exception as e:
            return {"valid": False, "error": str(e)}

    def get_market_quote(self, instrument_keys):
        try:
            keys_str = ",".join(instrument_keys)
            r = self.session.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                params={"instrument_key": keys_str},
                timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", {})
            return None
        except Exception as e:
            logger.error(f"Quote error: {e}")
            return None

    def get_market_quote_ohlc(self, instrument_keys, interval="1d"):
        try:
            keys_str = ",".join(instrument_keys)
            r = self.session.get(
                f"{UPSTOX_BASE}/market-quote/ohlc",
                params={"instrument_key": keys_str, "interval": interval},
                timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", {})
            return None
        except Exception as e:
            logger.error(f"OHLC error: {e}")
            return None

    def get_historical_candles(self, instrument_key, interval, from_date, to_date):
        try:
            encoded_key = requests.utils.quote(instrument_key, safe='')
            url = f"{UPSTOX_BASE}/historical-candle/{encoded_key}/{interval}/{to_date}/{from_date}"
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", {}).get("candles", [])
            return []
        except Exception as e:
            logger.error(f"Historical candle error: {e}")
            return []

    def get_intraday_candles(self, instrument_key, interval="5minute"):
        try:
            encoded_key = requests.utils.quote(instrument_key, safe='')
            url = f"{UPSTOX_BASE}/historical-candle/intraday/{encoded_key}/{interval}"
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", {}).get("candles", [])
            return []
        except Exception as e:
            logger.error(f"Intraday candle error: {e}")
            return []

    def get_option_chain(self, instrument_key="NSE_INDEX|Nifty 50", expiry_date=None):
        try:
            params = {"instrument_key": instrument_key}
            if expiry_date:
                params["expiry_date"] = expiry_date
            r = self.session.get(
                f"{UPSTOX_BASE}/option/chain",
                params=params,
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", [])
            return []
        except Exception as e:
            logger.error(f"Option chain error: {e}")
            return []

    def get_option_contracts(self, instrument_key="NSE_INDEX|Nifty 50"):
        try:
            params = {"instrument_key": instrument_key}
            r = self.session.get(
                f"{UPSTOX_BASE}/option/contract",
                params=params,
                timeout=15
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", [])
            return []
        except Exception as e:
            logger.error(f"Option contracts error: {e}")
            return []

    def get_full_market_quote(self, instrument_keys):
        try:
            keys_str = ",".join(instrument_keys)
            r = self.session.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                params={"instrument_key": keys_str},
                timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    return data.get("data", {})
            return None
        except Exception as e:
            logger.error(f"Full quote error: {e}")
            return None

# ============================================================
# TECHNICAL ANALYSIS ENGINE
# ============================================================

class TechnicalEngine:
    @staticmethod
    def calculate_ema(data, period):
        if len(data) < period:
            return data[-1] if data else 0
        multiplier = 2 / (period + 1)
        ema = sum(data[:period]) / period
        for price in data[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    @staticmethod
    def calculate_sma(data, period):
        if len(data) < period:
            return sum(data) / len(data) if data else 0
        return sum(data[-period:]) / period

    @staticmethod
    def calculate_rsi(closes, period=14):
        if len(closes) < period + 1:
            return 50
        gains = []
        losses = []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i - 1]
            if diff > 0:
                gains.append(diff)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(diff))

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_atr(candles, period=14):
        if len(candles) < 2:
            return 0
        trs = []
        for i in range(1, len(candles)):
            high = candles[i]['high']
            low = candles[i]['low']
            prev_close = candles[i - 1]['close']
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if len(trs) < period:
            return sum(trs) / len(trs) if trs else 0
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return atr

    @staticmethod
    def calculate_vwap(candles):
        if not candles:
            return 0
        cum_vol = 0
        cum_tp_vol = 0
        for c in candles:
            tp = (c['high'] + c['low'] + c['close']) / 3
            vol = c.get('volume', 1)
            cum_tp_vol += tp * vol
            cum_vol += vol
        return cum_tp_vol / cum_vol if cum_vol > 0 else 0

    @staticmethod
    def calculate_vwap_bands(candles, std_dev_mult=2):
        if not candles:
            return 0, 0, 0
        vwap = TechnicalEngine.calculate_vwap(candles)
        cum_vol = 0
        cum_sq_diff = 0
        for c in candles:
            tp = (c['high'] + c['low'] + c['close']) / 3
            vol = c.get('volume', 1)
            cum_sq_diff += vol * (tp - vwap) ** 2
            cum_vol += vol
        if cum_vol > 0:
            std = math.sqrt(cum_sq_diff / cum_vol)
        else:
            std = 0
        return vwap, vwap + std_dev_mult * std, vwap - std_dev_mult * std

    @staticmethod
    def calculate_bollinger(closes, period=20, std_dev=2):
        if len(closes) < period:
            mid = sum(closes) / len(closes) if closes else 0
            return mid, mid, mid
        sma = sum(closes[-period:]) / period
        variance = sum((x - sma) ** 2 for x in closes[-period:]) / period
        std = math.sqrt(variance)
        return sma, sma + std_dev * std, sma - std_dev * std

    @staticmethod
    def calculate_macd(closes, fast=12, slow=26, signal=9):
        if len(closes) < slow:
            return 0, 0, 0
        fast_ema = TechnicalEngine.calculate_ema(closes, fast)
        slow_ema = TechnicalEngine.calculate_ema(closes, slow)
        macd_line = fast_ema - slow_ema
        return macd_line, 0, macd_line

    @staticmethod
    def find_swing_points(candles, lookback=5):
        swing_highs = []
        swing_lows = []
        if len(candles) < lookback * 2 + 1:
            return swing_highs, swing_lows
        for i in range(lookback, len(candles) - lookback):
            is_high = True
            is_low = True
            for j in range(1, lookback + 1):
                if candles[i]['high'] <= candles[i - j]['high'] or candles[i]['high'] <= candles[i + j]['high']:
                    is_high = False
                if candles[i]['low'] >= candles[i - j]['low'] or candles[i]['low'] >= candles[i + j]['low']:
                    is_low = False
            if is_high:
                swing_highs.append({
                    "index": i,
                    "price": candles[i]['high'],
                    "time": candles[i].get('time', ''),
                })
            if is_low:
                swing_lows.append({
                    "index": i,
                    "price": candles[i]['low'],
                    "time": candles[i].get('time', ''),
                })
        return swing_highs, swing_lows


# ============================================================
# SMART MONEY CONCEPTS ENGINE
# ============================================================

class SmartMoneyEngine:
    @staticmethod
    def detect_bos_choch(candles, swing_highs, swing_lows):
        labels = []
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return labels

        # Detect BOS (Break of Structure)
        for i in range(1, len(swing_highs)):
            if swing_highs[i]['price'] > swing_highs[i - 1]['price']:
                labels.append({
                    "type": "BOS",
                    "direction": "Bullish",
                    "price": swing_highs[i]['price'],
                    "index": swing_highs[i]['index'],
                    "time": swing_highs[i].get('time', ''),
                })

        for i in range(1, len(swing_lows)):
            if swing_lows[i]['price'] < swing_lows[i - 1]['price']:
                labels.append({
                    "type": "BOS",
                    "direction": "Bearish",
                    "price": swing_lows[i]['price'],
                    "index": swing_lows[i]['index'],
                    "time": swing_lows[i].get('time', ''),
                })

        # Detect CHOCH (Change of Character)
        for i in range(1, len(swing_highs)):
            if i < len(swing_lows):
                if swing_lows[i]['price'] < swing_lows[i - 1]['price'] and \
                   swing_highs[i]['price'] < swing_highs[i - 1]['price']:
                    labels.append({
                        "type": "CHOCH",
                        "direction": "Bearish",
                        "price": swing_lows[i]['price'],
                        "index": swing_lows[i]['index'],
                        "time": swing_lows[i].get('time', ''),
                    })

        for i in range(1, len(swing_lows)):
            if i < len(swing_highs):
                if swing_highs[i]['price'] > swing_highs[i - 1]['price'] and \
                   swing_lows[i]['price'] > swing_lows[i - 1]['price']:
                    labels.append({
                        "type": "CHOCH",
                        "direction": "Bullish",
                        "price": swing_highs[i]['price'],
                        "index": swing_highs[i]['index'],
                        "time": swing_highs[i].get('time', ''),
                    })

        return labels

    @staticmethod
    def detect_order_blocks(candles, lookback=3):
        order_blocks = []
        if len(candles) < lookback + 2:
            return order_blocks

        for i in range(lookback, len(candles) - 1):
            # Bullish OB: last bearish candle before strong bullish move
            if candles[i]['close'] < candles[i]['open']:  # bearish candle
                if candles[i + 1]['close'] > candles[i + 1]['open']:  # next is bullish
                    move = candles[i + 1]['close'] - candles[i + 1]['open']
                    avg_range = sum(abs(candles[j]['close'] - candles[j]['open'])
                                    for j in range(i - lookback, i)) / lookback
                    if avg_range > 0 and move > avg_range * 1.5:
                        ob = {
                            "type": "Bullish",
                            "high": candles[i]['high'],
                            "low": candles[i]['low'],
                            "open": candles[i]['open'],
                            "close": candles[i]['close'],
                            "index": i,
                            "time": candles[i].get('time', ''),
                            "status": "Fresh",
                            "strength": min(95, int(60 + (move / avg_range) * 10)),
                        }
                        # Check if tested
                        for j in range(i + 2, min(i + 20, len(candles))):
                            if candles[j]['low'] <= ob['high']:
                                ob['status'] = "Tested"
                                if candles[j]['close'] < ob['low']:
                                    ob['status'] = "Invalidated"
                                break
                        order_blocks.append(ob)

            # Bearish OB: last bullish candle before strong bearish move
            if candles[i]['close'] > candles[i]['open']:  # bullish candle
                if candles[i + 1]['close'] < candles[i + 1]['open']:  # next is bearish
                    move = candles[i + 1]['open'] - candles[i + 1]['close']
                    avg_range = sum(abs(candles[j]['close'] - candles[j]['open'])
                                    for j in range(i - lookback, i)) / lookback
                    if avg_range > 0 and move > avg_range * 1.5:
                        ob = {
                            "type": "Bearish",
                            "high": candles[i]['high'],
                            "low": candles[i]['low'],
                            "open": candles[i]['open'],
                            "close": candles[i]['close'],
                            "index": i,
                            "time": candles[i].get('time', ''),
                            "status": "Fresh",
                            "strength": min(95, int(60 + (move / avg_range) * 10)),
                        }
                        for j in range(i + 2, min(i + 20, len(candles))):
                            if candles[j]['high'] >= ob['low']:
                                ob['status'] = "Tested"
                                if candles[j]['close'] > ob['high']:
                                    ob['status'] = "Invalidated"
                                break
                        order_blocks.append(ob)

        return order_blocks

    @staticmethod
    def detect_fvg(candles):
        fvg_zones = []
        if len(candles) < 3:
            return fvg_zones

        for i in range(2, len(candles)):
            # Bullish FVG
            if candles[i]['low'] > candles[i - 2]['high']:
                fvg_zones.append({
                    "type": "Bullish FVG",
                    "top": candles[i]['low'],
                    "bottom": candles[i - 2]['high'],
                    "index": i - 1,
                    "time": candles[i - 1].get('time', ''),
                    "filled": False,
                })
            # Bearish FVG
            if candles[i]['high'] < candles[i - 2]['low']:
                fvg_zones.append({
                    "type": "Bearish FVG",
                    "top": candles[i - 2]['low'],
                    "bottom": candles[i]['high'],
                    "index": i - 1,
                    "time": candles[i - 1].get('time', ''),
                    "filled": False,
                })

        return fvg_zones

    @staticmethod
    def detect_liquidity_levels(candles, swing_highs, swing_lows, option_chain_data=None):
        levels = {"above": [], "below": []}
        if not candles:
            return levels

        current_price = candles[-1]['close']

        # Equal highs
        for i in range(len(swing_highs)):
            for j in range(i + 1, len(swing_highs)):
                diff = abs(swing_highs[i]['price'] - swing_highs[j]['price'])
                avg = (swing_highs[i]['price'] + swing_highs[j]['price']) / 2
                if avg > 0 and diff / avg < 0.001:  # within 0.1%
                    level = {
                        "type": "Equal Highs",
                        "price": avg,
                        "strength": 85,
                    }
                    if avg > current_price:
                        levels["above"].append(level)
                    else:
                        levels["below"].append(level)

        # Equal lows
        for i in range(len(swing_lows)):
            for j in range(i + 1, len(swing_lows)):
                diff = abs(swing_lows[i]['price'] - swing_lows[j]['price'])
                avg = (swing_lows[i]['price'] + swing_lows[j]['price']) / 2
                if avg > 0 and diff / avg < 0.001:
                    level = {
                        "type": "Equal Lows",
                        "price": avg,
                        "strength": 85,
                    }
                    if avg > current_price:
                        levels["above"].append(level)
                    else:
                        levels["below"].append(level)

        # OI-based liquidity from option chain
        if option_chain_data:
            call_oi_levels = []
            put_oi_levels = []
            for strike_data in option_chain_data:
                strike = strike_data.get('strike_price', 0)
                call_oi = strike_data.get('call_oi', 0)
                put_oi = strike_data.get('put_oi', 0)
                if call_oi > 0:
                    call_oi_levels.append({"strike": strike, "oi": call_oi})
                if put_oi > 0:
                    put_oi_levels.append({"strike": strike, "oi": put_oi})

            # Top call OI as resistance
            call_oi_levels.sort(key=lambda x: x['oi'], reverse=True)
            for cl in call_oi_levels[:3]:
                levels["above"].append({
                    "type": "Call OI Cluster",
                    "price": cl['strike'],
                    "strength": min(95, int(70 + (cl['oi'] / max(1, call_oi_levels[0]['oi'])) * 25)),
                    "oi": cl['oi'],
                })

            # Top put OI as support
            put_oi_levels.sort(key=lambda x: x['oi'], reverse=True)
            for pl in put_oi_levels[:3]:
                levels["below"].append({
                    "type": "Put OI Cluster",
                    "price": pl['strike'],
                    "strength": min(95, int(70 + (pl['oi'] / max(1, put_oi_levels[0]['oi'])) * 25)),
                    "oi": pl['oi'],
                })

        return levels

    @staticmethod
    def detect_premium_discount_zones(candles):
        if len(candles) < 20:
            return None, None
        recent = candles[-20:]
        high = max(c['high'] for c in recent)
        low = min(c['low'] for c in recent)
        mid = (high + low) / 2
        premium = {"top": high, "bottom": mid, "label": "Premium Zone"}
        discount = {"top": mid, "bottom": low, "label": "Discount Zone"}
        return premium, discount

    @staticmethod
    def detect_liquidity_sweeps(candles, swing_highs, swing_lows):
        sweeps = []
        if len(candles) < 5 or not swing_highs or not swing_lows:
            return sweeps

        current = candles[-1]
        # Check if recent candle swept a swing high then closed below
        for sh in swing_highs[-5:]:
            if current['high'] > sh['price'] and current['close'] < sh['price']:
                sweeps.append({
                    "type": "Liquidity Sweep",
                    "direction": "Bearish",
                    "level": sh['price'],
                    "time": current.get('time', ''),
                })

        for sl in swing_lows[-5:]:
            if current['low'] < sl['price'] and current['close'] > sl['price']:
                sweeps.append({
                    "type": "Liquidity Grab",
                    "direction": "Bullish",
                    "level": sl['price'],
                    "time": current.get('time', ''),
                })

        return sweeps


# ============================================================
# OPTION CHAIN ANALYSIS ENGINE
# ============================================================

class OptionChainEngine:
    @staticmethod
    def analyze(option_chain_data, spot_price):
        if not option_chain_data or spot_price <= 0:
            return OptionChainEngine._empty_result()

        total_call_oi = 0
        total_put_oi = 0
        total_call_oi_change = 0
        total_put_oi_change = 0
        total_call_volume = 0
        total_put_volume = 0
        call_wall = {"strike": 0, "oi": 0}
        put_wall = {"strike": 0, "oi": 0}
        max_pain_data = {}
        gamma_exposure = {}
        delta_exposure = 0

        strikes_data = []

        for item in option_chain_data:
            strike = 0
            call_oi = 0
            put_oi = 0
            call_oi_chg = 0
            put_oi_chg = 0
            call_vol = 0
            put_vol = 0
            call_delta = 0
            put_delta = 0
            call_gamma = 0
            put_gamma = 0

            if isinstance(item, dict):
                strike = item.get('strike_price', 0)
                call_data = item.get('call_options', {})
                put_data = item.get('put_options', {})

                if isinstance(call_data, dict):
                    mq = call_data.get('market_data', {})
                    if isinstance(mq, dict):
                        call_oi = mq.get('oi', 0) or 0
                        call_oi_chg = mq.get('oi_day_change', 0) or 0
                        call_vol = mq.get('volume', 0) or 0
                    greeks = call_data.get('option_greeks', {})
                    if isinstance(greeks, dict):
                        call_delta = greeks.get('delta', 0) or 0
                        call_gamma = greeks.get('gamma', 0) or 0

                if isinstance(put_data, dict):
                    mq = put_data.get('market_data', {})
                    if isinstance(mq, dict):
                        put_oi = mq.get('oi', 0) or 0
                        put_oi_chg = mq.get('oi_day_change', 0) or 0
                        put_vol = mq.get('volume', 0) or 0
                    greeks = put_data.get('option_greeks', {})
                    if isinstance(greeks, dict):
                        put_delta = greeks.get('delta', 0) or 0
                        put_gamma = greeks.get('gamma', 0) or 0

            if strike <= 0:
                continue

            total_call_oi += call_oi
            total_put_oi += put_oi
            total_call_oi_change += call_oi_chg
            total_put_oi_change += put_oi_chg
            total_call_volume += call_vol
            total_put_volume += put_vol
            delta_exposure += (call_delta * call_oi) + (put_delta * put_oi)

            if call_oi > call_wall['oi']:
                call_wall = {"strike": strike, "oi": call_oi}
            if put_oi > put_wall['oi']:
                put_wall = {"strike": strike, "oi": put_oi}

            max_pain_data[strike] = {"call_oi": call_oi, "put_oi": put_oi}
            gamma_exposure[strike] = (call_gamma * call_oi) + (put_gamma * put_oi)

            strikes_data.append({
                "strike": strike,
                "call_oi": call_oi,
                "put_oi": put_oi,
                "call_oi_change": call_oi_chg,
                "put_oi_change": put_oi_chg,
                "call_volume": call_vol,
                "put_volume": put_vol,
                "call_delta": call_delta,
                "put_delta": put_delta,
                "call_gamma": call_gamma,
                "put_gamma": put_gamma,
            })

        # PCR
        pcr = total_put_oi / total_call_oi if total_call_oi > 0 else 1

        # Max Pain
        max_pain = OptionChainEngine._calculate_max_pain(max_pain_data)

        # Gamma Wall
        gamma_wall_strike = max(gamma_exposure, key=gamma_exposure.get) if gamma_exposure else 0

        # Dealer positioning
        if pcr > 1.2:
            dealer_pos = "Net Long Puts (Bullish Hedge)"
        elif pcr < 0.8:
            dealer_pos = "Net Long Calls (Bearish Hedge)"
        else:
            dealer_pos = "Neutral"

        return {
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "total_call_oi_change": total_call_oi_change,
            "total_put_oi_change": total_put_oi_change,
            "pcr": round(pcr, 3),
            "dynamic_pcr": round(
                (total_put_oi + total_put_oi_change) /
                max(1, total_call_oi + total_call_oi_change), 3
            ),
            "call_wall": call_wall,
            "put_wall": put_wall,
            "gamma_wall": gamma_wall_strike,
            "max_pain": max_pain,
            "delta_exposure": round(delta_exposure, 2),
            "total_gamma_exposure": round(sum(gamma_exposure.values()), 4),
            "dealer_positioning": dealer_pos,
            "strikes_data": strikes_data,
            "total_call_volume": total_call_volume,
            "total_put_volume": total_put_volume,
        }

    @staticmethod
    def _calculate_max_pain(data):
        if not data:
            return 0
        min_pain = float('inf')
        max_pain_strike = 0
        strikes = sorted(data.keys())
        for strike in strikes:
            pain = 0
            for s in strikes:
                if s < strike:
                    pain += data[s]['call_oi'] * (strike - s)
                elif s > strike:
                    pain += data[s]['put_oi'] * (s - strike)
            if pain < min_pain:
                min_pain = pain
                max_pain_strike = strike
        return max_pain_strike

    @staticmethod
    def _empty_result():
        return {
            "total_call_oi": 0, "total_put_oi": 0,
            "total_call_oi_change": 0, "total_put_oi_change": 0,
            "pcr": 1.0, "dynamic_pcr": 1.0,
            "call_wall": {"strike": 0, "oi": 0},
            "put_wall": {"strike": 0, "oi": 0},
            "gamma_wall": 0, "max_pain": 0,
            "delta_exposure": 0, "total_gamma_exposure": 0,
            "dealer_positioning": "N/A",
            "strikes_data": [],
            "total_call_volume": 0, "total_put_volume": 0,
        }


# ============================================================
# SUPPORT & RESISTANCE ENGINE
# ============================================================

class SupportResistanceEngine:
    @staticmethod
    def calculate(candles, option_data=None, current_price=0):
        if not candles or current_price <= 0:
            return {"supports": [], "resistances": []}

        levels = []

        # Pivot points
        last = candles[-1]
        h, l, c = last['high'], last['low'], last['close']
        pivot = (h + l + c) / 3
        r1 = 2 * pivot - l
        r2 = pivot + (h - l)
        r3 = h + 2 * (pivot - l)
        s1 = 2 * pivot - h
        s2 = pivot - (h - l)
        s3 = l - 2 * (h - pivot)

        # Swing-based levels
        swing_highs, swing_lows = TechnicalEngine.find_swing_points(candles, lookback=3)

        # Collect resistance candidates
        res_candidates = [
            {"price": r1, "source": "Pivot R1"},
            {"price": r2, "source": "Pivot R2"},
            {"price": r3, "source": "Pivot R3"},
        ]
        for sh in swing_highs[-5:]:
            if sh['price'] > current_price:
                res_candidates.append({"price": sh['price'], "source": "Swing High"})

        # Collect support candidates
        sup_candidates = [
            {"price": s1, "source": "Pivot S1"},
            {"price": s2, "source": "Pivot S2"},
            {"price": s3, "source": "Pivot S3"},
        ]
        for sl in swing_lows[-5:]:
            if sl['price'] < current_price:
                sup_candidates.append({"price": sl['price'], "source": "Swing Low"})

        # Score levels
        resistances = []
        for rc in res_candidates:
            if rc['price'] <= current_price:
                continue
            strength = SupportResistanceEngine._score_level(
                rc['price'], candles, option_data, "resistance"
            )
            reactions = SupportResistanceEngine._count_reactions(rc['price'], candles)
            oi_str = SupportResistanceEngine._oi_strength(rc['price'], option_data, "call")
            gamma_str = SupportResistanceEngine._gamma_strength(rc['price'], option_data)
            hold_prob = min(99, int(strength * 0.5 + oi_str * 0.3 + reactions * 5))

            resistances.append({
                "price": round(rc['price'], 2),
                "source": rc['source'],
                "strength": strength,
                "reactions": reactions,
                "oi_strength": oi_str,
                "gamma_strength": gamma_str,
                "hold_probability": hold_prob,
            })

        supports = []
        for sc in sup_candidates:
            if sc['price'] >= current_price:
                continue
            strength = SupportResistanceEngine._score_level(
                sc['price'], candles, option_data, "support"
            )
            reactions = SupportResistanceEngine._count_reactions(sc['price'], candles)
            oi_str = SupportResistanceEngine._oi_strength(sc['price'], option_data, "put")
            gamma_str = SupportResistanceEngine._gamma_strength(sc['price'], option_data)
            hold_prob = min(99, int(strength * 0.5 + oi_str * 0.3 + reactions * 5))

            supports.append({
                "price": round(sc['price'], 2),
                "source": sc['source'],
                "strength": strength,
                "reactions": reactions,
                "oi_strength": oi_str,
                "gamma_strength": gamma_str,
                "hold_probability": hold_prob,
            })

        resistances.sort(key=lambda x: x['price'])
        supports.sort(key=lambda x: x['price'], reverse=True)

        return {
            "resistances": resistances[:3],
            "supports": supports[:3],
        }

    @staticmethod
    def _score_level(price, candles, option_data, level_type):
        score = 50
        touches = 0
        for c in candles:
            if abs(c['high'] - price) / price < 0.002:
                touches += 1
            if abs(c['low'] - price) / price < 0.002:
                touches += 1
        score += min(30, touches * 5)

        if option_data:
            for sd in option_data.get('strikes_data', []):
                if abs(sd['strike'] - price) < 100:
                    if level_type == "resistance":
                        score += min(20, sd.get('call_oi', 0) // 100000)
                    else:
                        score += min(20, sd.get('put_oi', 0) // 100000)

        return min(99, score)

    @staticmethod
    def _count_reactions(price, candles, tolerance=0.002):
        count = 0
        for i in range(1, len(candles)):
            if abs(candles[i]['high'] - price) / price < tolerance:
                if candles[i]['close'] < candles[i]['open']:
                    count += 1
            if abs(candles[i]['low'] - price) / price < tolerance:
                if candles[i]['close'] > candles[i]['open']:
                    count += 1
        return count

    @staticmethod
    def _oi_strength(price, option_data, oi_type):
        if not option_data or not option_data.get('strikes_data'):
            return 50
        for sd in option_data.get('strikes_data', []):
            if abs(sd['strike'] - price) < 100:
                if oi_type == "call":
                    max_oi = option_data.get('call_wall', {}).get('oi', 1)
                    return min(99, int(50 + (sd.get('call_oi', 0) / max(1, max_oi)) * 49))
                else:
                    max_oi = option_data.get('put_wall', {}).get('oi', 1)
                    return min(99, int(50 + (sd.get('put_oi', 0) / max(1, max_oi)) * 49))
        return 50

    @staticmethod
    def _gamma_strength(price, option_data):
        if not option_data or not option_data.get('strikes_data'):
            return 50
        for sd in option_data.get('strikes_data', []):
            if abs(sd['strike'] - price) < 100:
                gamma = abs(sd.get('call_gamma', 0)) + abs(sd.get('put_gamma', 0))
                return min(99, int(50 + gamma * 10000))
        return 50


# ============================================================
# VIX INTELLIGENCE ENGINE
# ============================================================

class VIXEngine:
    @staticmethod
    def analyze(vix_value, vix_change, price_change_pct, prev_vix=0):
        result = {
            "value": round(vix_value, 2),
            "change": round(vix_change, 2),
            "state": "Normal",
            "divergence": False,
            "trap": False,
            "expansion": False,
            "compression": False,
            "trade_bias": "Neutral",
            "risk_score": 50,
            "probability_score": 50,
            "pattern": "",
        }

        # State
        if vix_value > 20:
            result["state"] = "High Volatility"
        elif vix_value > 15:
            result["state"] = "Elevated"
        elif vix_value > 12:
            result["state"] = "Normal"
        else:
            result["state"] = "Low Volatility"

        # Expansion / Compression
        if vix_change > 5:
            result["expansion"] = True
        elif vix_change < -5:
            result["compression"] = True

        # Price-VIX patterns
        if price_change_pct > 0 and vix_change < -1:
            result["pattern"] = "Price ↑ + VIX ↓"
            result["trade_bias"] = "Bullish"
            result["probability_score"] = 82
            result["risk_score"] = 25
        elif price_change_pct < 0 and vix_change > 1:
            result["pattern"] = "Price ↓ + VIX ↑"
            result["trade_bias"] = "Bearish"
            result["probability_score"] = 80
            result["risk_score"] = 70
        elif price_change_pct > 0 and vix_change > 1:
            result["pattern"] = "Price ↑ + VIX ↑"
            result["trade_bias"] = "Cautious Bullish"
            result["probability_score"] = 55
            result["risk_score"] = 60
            result["divergence"] = True
        elif price_change_pct < 0 and vix_change < -1:
            result["pattern"] = "Price ↓ + VIX ↓"
            result["trade_bias"] = "Bullish Reversal"
            result["probability_score"] = 65
            result["risk_score"] = 45
            result["trap"] = True
        elif abs(price_change_pct) < 0.1 and vix_change > 2:
            result["pattern"] = "Price Sideways + VIX ↑"
            result["trade_bias"] = "Breakout Imminent"
            result["probability_score"] = 70
            result["risk_score"] = 55
        elif abs(price_change_pct) < 0.1 and vix_change < -2:
            result["pattern"] = "Price Sideways + VIX ↓"
            result["trade_bias"] = "Range Bound"
            result["probability_score"] = 60
            result["risk_score"] = 30

        return result


# ============================================================
# VWAP BAND ENGINE
# ============================================================

class VWAPEngine:
    @staticmethod
    def analyze(candles, current_price):
        if not candles or current_price <= 0:
            return {
                "vwap": 0, "upper_band": 0, "lower_band": 0,
                "status": "N/A", "bullish_score": 50, "bearish_score": 50,
            }

        vwap, upper, lower = TechnicalEngine.calculate_vwap_bands(candles)

        status = "Neutral"
        bullish_score = 50
        bearish_score = 50

        if current_price > upper:
            status = "Upper Band Expansion"
            bullish_score = 85
            bearish_score = 15
        elif current_price > vwap:
            status = "VWAP Acceptance (Above)"
            bullish_score = 70
            bearish_score = 30
        elif current_price < lower:
            status = "Lower Band Expansion"
            bullish_score = 15
            bearish_score = 85
        elif current_price < vwap:
            status = "VWAP Rejection (Below)"
            bullish_score = 30
            bearish_score = 70
        else:
            status = "At VWAP"
            bullish_score = 50
            bearish_score = 50

        # Compression detection
        band_width = (upper - lower) / vwap * 100 if vwap > 0 else 0
        if band_width < 0.3:
            status = "Compression"
        elif band_width > 1.5:
            if current_price > vwap:
                status = "Bullish Expansion"
            else:
                status = "Bearish Expansion"

        # Mean reversion
        if current_price > upper * 1.005:
            status = "Mean Reversion (Overbought)"
            bearish_score = max(bearish_score, 65)
        elif current_price < lower * 0.995:
            status = "Mean Reversion (Oversold)"
            bullish_score = max(bullish_score, 65)

        return {
            "vwap": round(vwap, 2),
            "upper_band": round(upper, 2),
            "lower_band": round(lower, 2),
            "status": status,
            "bullish_score": bullish_score,
            "bearish_score": bearish_score,
            "band_width": round(band_width, 3),
        }


# ============================================================
# INSTITUTIONAL POSITIONING ENGINE
# ============================================================

class InstitutionalEngine:
    @staticmethod
    def analyze(futures_oi, futures_oi_change, price_change, volume, prev_volume=0):
        result = {
            "positioning": "Neutral",
            "bias": "Neutral",
            "confidence": 50,
            "description": "",
        }

        if futures_oi <= 0:
            return result

        oi_pct = (futures_oi_change / futures_oi * 100) if futures_oi > 0 else 0

        if price_change > 0 and oi_pct > 1:
            result["positioning"] = "Long Build-Up"
            result["bias"] = "Bullish"
            result["confidence"] = min(95, int(60 + abs(oi_pct) * 5))
            result["description"] = f"Price rising with OI increase ({oi_pct:+.1f}%). Institutions building fresh longs."
        elif price_change < 0 and oi_pct > 1:
            result["positioning"] = "Short Build-Up"
            result["bias"] = "Bearish"
            result["confidence"] = min(95, int(60 + abs(oi_pct) * 5))
            result["description"] = f"Price falling with OI increase ({oi_pct:+.1f}%). Institutions building shorts."
        elif price_change < 0 and oi_pct < -1:
            result["positioning"] = "Long Unwinding"
            result["bias"] = "Bearish"
            result["confidence"] = min(90, int(55 + abs(oi_pct) * 4))
            result["description"] = f"Price falling with OI decrease ({oi_pct:+.1f}%). Longs exiting positions."
        elif price_change > 0 and oi_pct < -1:
            result["positioning"] = "Short Covering"
            result["bias"] = "Bullish"
            result["confidence"] = min(90, int(55 + abs(oi_pct) * 4))
            result["description"] = f"Price rising with OI decrease ({oi_pct:+.1f}%). Shorts covering positions."
        else:
            result["positioning"] = "No Clear Pattern"
            result["confidence"] = 40

        return result


# ============================================================
# MARKET REGIME ENGINE
# ============================================================

class MarketRegimeEngine:
    @staticmethod
    def determine(candles, option_data, vix_data, vwap_data, institutional_data, smc_labels):
        scores = {
            "Trending Bullish": 0,
            "Trending Bearish": 0,
            "Strong Breakout": 0,
            "Strong Breakdown": 0,
            "Volatile Expansion": 0,
            "Accumulation": 0,
            "Distribution": 0,
            "Range Bound": 0,
            "Mean Reversion": 0,
        }

        if not candles:
            return {"state": "Initializing", "confidence": 0, "scores": scores}

        closes = [c['close'] for c in candles]
        current = closes[-1] if closes else 0

        # RSI
        rsi = TechnicalEngine.calculate_rsi(closes)

        # ATR based volatility
        atr = TechnicalEngine.calculate_atr(candles)
        atr_pct = (atr / current * 100) if current > 0 else 0

        # Trend via EMA
        if len(closes) >= 20:
            ema9 = TechnicalEngine.calculate_ema(closes, 9)
            ema20 = TechnicalEngine.calculate_ema(closes, 20)
            if ema9 > ema20:
                scores["Trending Bullish"] += 20
            else:
                scores["Trending Bearish"] += 20

        # VWAP
        if vwap_data:
            if "Bullish" in vwap_data.get('status', ''):
                scores["Trending Bullish"] += 15
            elif "Bearish" in vwap_data.get('status', ''):
                scores["Trending Bearish"] += 15
            if "Compression" in vwap_data.get('status', ''):
                scores["Range Bound"] += 15
            if "Mean Reversion" in vwap_data.get('status', ''):
                scores["Mean Reversion"] += 20
            if "Expansion" in vwap_data.get('status', ''):
                scores["Volatile Expansion"] += 15

        # VIX
        if vix_data:
            if vix_data.get('expansion'):
                scores["Volatile Expansion"] += 20
            if vix_data.get('compression'):
                scores["Range Bound"] += 10
            if vix_data.get('trade_bias') == "Bullish":
                scores["Trending Bullish"] += 15
            elif vix_data.get('trade_bias') == "Bearish":
                scores["Trending Bearish"] += 15
            if vix_data.get('divergence'):
                scores["Mean Reversion"] += 15
            if vix_data.get('value', 0) > 18:
                scores["Volatile Expansion"] += 10

        # Option chain
        if option_data:
            pcr = option_data.get('pcr', 1)
            if pcr > 1.3:
                scores["Trending Bullish"] += 15
                scores["Accumulation"] += 10
            elif pcr < 0.7:
                scores["Trending Bearish"] += 15
                scores["Distribution"] += 10
            elif 0.9 <= pcr <= 1.1:
                scores["Range Bound"] += 10

        # Institutional
        if institutional_data:
            pos = institutional_data.get('positioning', '')
            if pos == "Long Build-Up":
                scores["Trending Bullish"] += 20
                scores["Strong Breakout"] += 10
            elif pos == "Short Build-Up":
                scores["Trending Bearish"] += 20
                scores["Strong Breakdown"] += 10
            elif pos == "Long Unwinding":
                scores["Distribution"] += 15
                scores["Trending Bearish"] += 10
            elif pos == "Short Covering":
                scores["Accumulation"] += 15
                scores["Trending Bullish"] += 10

        # SMC
        if smc_labels:
            bullish_bos = sum(1 for l in smc_labels if l['type'] == 'BOS' and l['direction'] == 'Bullish')
            bearish_bos = sum(1 for l in smc_labels if l['type'] == 'BOS' and l['direction'] == 'Bearish')
            if bullish_bos > bearish_bos:
                scores["Trending Bullish"] += 15
                scores["Strong Breakout"] += 10
            elif bearish_bos > bullish_bos:
                scores["Trending Bearish"] += 15
                scores["Strong Breakdown"] += 10

        # ATR volatility
        if atr_pct > 1.5:
            scores["Volatile Expansion"] += 15
        elif atr_pct < 0.5:
            scores["Range Bound"] += 15

        # RSI
        if rsi > 70:
            scores["Trending Bullish"] += 10
            scores["Mean Reversion"] += 10
        elif rsi < 30:
            scores["Trending Bearish"] += 10
            scores["Mean Reversion"] += 10
        elif 45 < rsi < 55:
            scores["Range Bound"] += 10

        # Determine regime
        best_regime = max(scores, key=scores.get)
        total = sum(scores.values())
        confidence = int(scores[best_regime] / max(1, total) * 100) if total > 0 else 0
        confidence = min(99, max(10, confidence))

        return {
            "state": best_regime,
            "confidence": confidence,
            "scores": scores,
        }


# ============================================================
# MARKET STRUCTURE ENGINE
# ============================================================

class MarketStructureEngine:
    @staticmethod
    def determine(candles, smc_labels, vwap_data):
        if not candles:
            return {"structure": "Neutral", "strength": 50}

        score = 50  # Neutral baseline

        # BOS/CHOCH analysis
        if smc_labels:
            recent_labels = smc_labels[-10:]
            bullish_bos = sum(1 for l in recent_labels if l['type'] == 'BOS' and l['direction'] == 'Bullish')
            bearish_bos = sum(1 for l in recent_labels if l['type'] == 'BOS' and l['direction'] == 'Bearish')
            bullish_choch = sum(1 for l in recent_labels if l['type'] == 'CHOCH' and l['direction'] == 'Bullish')
            bearish_choch = sum(1 for l in recent_labels if l['type'] == 'CHOCH' and l['direction'] == 'Bearish')

            score += (bullish_bos - bearish_bos) * 10
            score += (bullish_choch - bearish_choch) * 15

        # Swing structure
        swing_highs, swing_lows = TechnicalEngine.find_swing_points(candles)
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            if swing_highs[-1]['price'] > swing_highs[-2]['price'] and \
               swing_lows[-1]['price'] > swing_lows[-2]['price']:
                score += 15  # Higher highs, higher lows
            elif swing_highs[-1]['price'] < swing_highs[-2]['price'] and \
                 swing_lows[-1]['price'] < swing_lows[-2]['price']:
                score -= 15  # Lower highs, lower lows

        # VWAP
        if vwap_data:
            if vwap_data.get('bullish_score', 50) > 65:
                score += 10
            elif vwap_data.get('bearish_score', 50) > 65:
                score -= 10

        # Classify
        if score >= 80:
            structure = "Strong Bullish"
        elif score >= 60:
            structure = "Bullish"
        elif score >= 40:
            structure = "Neutral"
        elif score >= 20:
            structure = "Bearish"
        else:
            structure = "Strong Bearish"

        return {
            "structure": structure,
            "strength": min(99, max(1, score)),
        }


# ============================================================
# AI DECISION ENGINE
# ============================================================

class AIDecisionEngine:
    @staticmethod
    def generate_decision(index_name, price, regime, sr_data, option_data,
                          institutional_data, vix_data, vwap_data,
                          structure_data, smc_labels):
        decision = {
            "index": index_name,
            "price": price,
            "direction": "Neutral",
            "trend_strength": 50,
            "support_strength": 50,
            "resistance_strength": 50,
            "institutional_bias": "Neutral",
            "smart_money": "Neutral",
            "pcr_bias": "Neutral",
            "vix_bias": "Neutral",
            "market_structure": "Neutral",
            "trade_probability": 50,
            "recommended_bias": "NEUTRAL",
        }

        scores = {"bullish": 0, "bearish": 0}

        # Regime
        if regime:
            state = regime.get('state', '')
            conf = regime.get('confidence', 0)
            if 'Bullish' in state or 'Breakout' in state or 'Accumulation' in state:
                scores['bullish'] += conf * 0.25
            elif 'Bearish' in state or 'Breakdown' in state or 'Distribution' in state:
                scores['bearish'] += conf * 0.25

        # Support/Resistance
        if sr_data:
            supports = sr_data.get('supports', [])
            resistances = sr_data.get('resistances', [])
            if supports:
                avg_sup_str = sum(s['strength'] for s in supports) / len(supports)
                decision['support_strength'] = int(avg_sup_str)
                scores['bullish'] += avg_sup_str * 0.1
            if resistances:
                avg_res_str = sum(r['strength'] for r in resistances) / len(resistances)
                decision['resistance_strength'] = int(avg_res_str)
                scores['bearish'] += avg_res_str * 0.05

        # Option chain
        if option_data:
            pcr = option_data.get('pcr', 1)
            if pcr > 1.2:
                decision['pcr_bias'] = "Bullish"
                scores['bullish'] += 15
            elif pcr < 0.8:
                decision['pcr_bias'] = "Bearish"
                scores['bearish'] += 15
            else:
                decision['pcr_bias'] = "Neutral"

        # Institutional
        if institutional_data:
            bias = institutional_data.get('bias', 'Neutral')
            decision['institutional_bias'] = bias
            conf = institutional_data.get('confidence', 50)
            if bias == "Bullish":
                scores['bullish'] += conf * 0.2
            elif bias == "Bearish":
                scores['bearish'] += conf * 0.2

        # VIX
        if vix_data:
            tb = vix_data.get('trade_bias', 'Neutral')
            decision['vix_bias'] = tb
            if 'Bullish' in tb:
                scores['bullish'] += 10
            elif 'Bearish' in tb:
                scores['bearish'] += 10

        # VWAP
        if vwap_data:
            if vwap_data.get('bullish_score', 50) > 65:
                scores['bullish'] += 10
            elif vwap_data.get('bearish_score', 50) > 65:
                scores['bearish'] += 10

        # Market structure
        if structure_data:
            struct = structure_data.get('structure', 'Neutral')
            decision['market_structure'] = struct
            if 'Bullish' in struct:
                scores['bullish'] += 15
            elif 'Bearish' in struct:
                scores['bearish'] += 15

        # SMC
        if smc_labels:
            recent = smc_labels[-5:]
            bullish_signals = sum(1 for l in recent if l.get('direction') == 'Bullish')
            bearish_signals = sum(1 for l in recent if l.get('direction') == 'Bearish')
            if bullish_signals > bearish_signals:
                decision['smart_money'] = "Buying"
                scores['bullish'] += 10
            elif bearish_signals > bullish_signals:
                decision['smart_money'] = "Selling"
                scores['bearish'] += 10

        # Final decision
        total = scores['bullish'] + scores['bearish']
        if total > 0:
            bull_pct = scores['bullish'] / total * 100
            bear_pct = scores['bearish'] / total * 100
        else:
            bull_pct = bear_pct = 50

        if bull_pct > 60:
            decision['direction'] = "Bullish"
            decision['recommended_bias'] = "CALL"
            decision['trade_probability'] = min(99, int(bull_pct))
            decision['trend_strength'] = min(99, int(bull_pct))
        elif bear_pct > 60:
            decision['direction'] = "Bearish"
            decision['recommended_bias'] = "PUT"
            decision['trade_probability'] = min(99, int(bear_pct))
            decision['trend_strength'] = min(99, int(bear_pct))
        else:
            decision['direction'] = "Neutral"
            decision['recommended_bias'] = "NEUTRAL"
            decision['trade_probability'] = 50
            decision['trend_strength'] = 50

        return decision


# ============================================================
# TRADE FINDER ENGINE
# ============================================================

class TradeFinderEngine:
    @staticmethod
    def generate_setups(candles, decision, sr_data, atr_value):
        if not candles or not decision or atr_value <= 0:
            return {"call": None, "put": None}

        current_price = candles[-1]['close']
        supports = sr_data.get('supports', []) if sr_data else []
        resistances = sr_data.get('resistances', []) if sr_data else []

        call_setup = None
        put_setup = None

        # CALL setup
        if supports:
            entry = current_price
            sl = supports[0]['price'] - atr_value * 0.5 if supports else current_price - atr_value * 2
            t1 = entry + atr_value * 1.5
            t2 = entry + atr_value * 2.5
            t3 = resistances[0]['price'] if resistances else entry + atr_value * 3.5

            risk = abs(entry - sl)
            reward = t2 - entry
            rr = round(reward / risk, 2) if risk > 0 else 0

            bull_prob = decision.get('trade_probability', 50) if decision.get('direction') == 'Bullish' else 40

            call_setup = {
                "entry": round(entry, 2),
                "stop_loss": round(sl, 2),
                "target_1": round(t1, 2),
                "target_2": round(t2, 2),
                "target_3": round(t3, 2),
                "probability": bull_prob,
                "risk_reward": rr,
            }

        # PUT setup
        if resistances:
            entry = current_price
            sl = resistances[0]['price'] + atr_value * 0.5 if resistances else current_price + atr_value * 2
            t1 = entry - atr_value * 1.5
            t2 = entry - atr_value * 2.5
            t3 = supports[0]['price'] if supports else entry - atr_value * 3.5

            risk = abs(sl - entry)
            reward = entry - t2
            rr = round(reward / risk, 2) if risk > 0 else 0

            bear_prob = decision.get('trade_probability', 50) if decision.get('direction') == 'Bearish' else 40

            put_setup = {
                "entry": round(entry, 2),
                "stop_loss": round(sl, 2),
                "target_1": round(t1, 2),
                "target_2": round(t2, 2),
                "target_3": round(t3, 2),
                "probability": bear_prob,
                "risk_reward": rr,
            }

        return {"call": call_setup, "put": put_setup}


# ============================================================
# AI COMMENTARY ENGINE
# ============================================================

class AICommentaryEngine:
    @staticmethod
    def generate(index_name, price, regime, sr_data, option_data,
                 institutional_data, vix_data, vwap_data, structure_data,
                 smc_labels, order_blocks, decision):
        parts = []

        parts.append(f"{index_name} is currently trading at {price:,.2f}.")

        # Regime
        if regime:
            parts.append(f"Market regime is {regime['state']} with {regime['confidence']}% confidence.")

        # Structure
        if structure_data:
            parts.append(f"Market structure is {structure_data['structure']}.")

        # VWAP
        if vwap_data and vwap_data.get('vwap'):
            rel = "above" if price > vwap_data['vwap'] else "below"
            parts.append(f"Price is {rel} VWAP ({vwap_data['vwap']:,.2f}). Status: {vwap_data['status']}.")

        # Option chain
        if option_data and option_data.get('pcr'):
            pcr = option_data['pcr']
            pcr_interpretation = "Bullish" if pcr > 1.2 else "Bearish" if pcr < 0.8 else "Neutral"
            parts.append(f"PCR is {pcr} ({pcr_interpretation}). Max Pain: {option_data.get('max_pain', 'N/A')}.")
            cw = option_data.get('call_wall', {})
            pw = option_data.get('put_wall', {})
            if cw.get('strike'):
                parts.append(f"Call Wall at {cw['strike']}. Put Wall at {pw.get('strike', 'N/A')}.")

        # VIX
        if vix_data:
            parts.append(f"VIX at {vix_data['value']} ({vix_data['state']}). Pattern: {vix_data.get('pattern', 'N/A')}.")

        # Institutional
        if institutional_data and institutional_data.get('positioning') != 'Neutral':
            parts.append(f"Institutional activity shows {institutional_data['positioning']}. {institutional_data.get('description', '')}")

        # SMC
        if smc_labels:
            recent = smc_labels[-3:]
            smc_summary = ", ".join([f"{l['type']} ({l['direction']})" for l in recent])
            parts.append(f"Recent SMC signals: {smc_summary}.")

        # Order Blocks
        active_obs = [ob for ob in (order_blocks or []) if ob.get('status') != 'Invalidated']
        if active_obs:
            ob_types = [f"{ob['type']} OB at {ob['high']:.0f}-{ob['low']:.0f} ({ob['status']})" for ob in active_obs[-2:]]
            parts.append(f"Active Order Blocks: {'; '.join(ob_types)}.")

        # S/R
        if sr_data:
            sups = sr_data.get('supports', [])
            ress = sr_data.get('resistances', [])
            if sups:
                parts.append(f"Key support at {sups[0]['price']:,.2f} (Strength: {sups[0]['strength']}%).")
            if ress:
                parts.append(f"Key resistance at {ress[0]['price']:,.2f} (Strength: {ress[0]['strength']}%).")

        # Decision
        if decision:
            prob = decision.get('trade_probability', 50)
            direction = decision.get('direction', 'Neutral')
            parts.append(f"Overall probability of {direction} continuation: {prob}%.")

        return " ".join(parts)


# ============================================================
# ALERT ENGINE
# ============================================================

class AlertEngine:
    def __init__(self):
        self.prev_regime = None
        self.prev_pcr = None
        self.prev_vix_bias = None
        self.prev_structure = None

    def generate_alerts(self, index_name, regime, option_data, vix_data,
                        institutional_data, structure_data, smc_labels,
                        order_blocks, sr_data, current_price):
        alerts = []
        now = datetime.now().strftime("%H:%M:%S")

        # Regime change
        if self.prev_regime and regime:
            if self.prev_regime != regime.get('state'):
                alerts.append({
                    "type": "Market Regime Change",
                    "message": f"{index_name}: {self.prev_regime} → {regime['state']}",
                    "confidence": regime.get('confidence', 50),
                    "priority": "Critical",
                    "impact": "Very High",
                    "validity": "Until next regime change",
                    "reasoning": f"Multiple indicators confirm regime shift to {regime['state']}.",
                    "time": now,
                    "index": index_name,
                })

        # Structure change
        if structure_data and self.prev_structure:
            if self.prev_structure != structure_data.get('structure'):
                alerts.append({
                    "type": "Market Structure Change",
                    "message": f"{index_name}: Structure changed to {structure_data['structure']}",
                    "confidence": structure_data.get('strength', 50),
                    "priority": "High",
                    "impact": "High",
                    "validity": "15-30 minutes",
                    "reasoning": "BOS/CHOCH and swing structure indicate structural shift.",
                    "time": now,
                    "index": index_name,
                })

        # PCR shift
        if option_data and self.prev_pcr:
            pcr = option_data.get('pcr', 1)
            if self.prev_pcr < 1.0 and pcr >= 1.2:
                alerts.append({
                    "type": "PCR Bullish Shift",
                    "message": f"{index_name}: PCR shifted bullish to {pcr:.2f}",
                    "confidence": 78,
                    "priority": "High",
                    "impact": "High",
                    "validity": "30-60 minutes",
                    "reasoning": "Put writers adding aggressively, indicating support building.",
                    "time": now,
                    "index": index_name,
                })
            elif self.prev_pcr > 1.0 and pcr <= 0.8:
                alerts.append({
                    "type": "PCR Bearish Shift",
                    "message": f"{index_name}: PCR shifted bearish to {pcr:.2f}",
                    "confidence": 78,
                    "priority": "High",
                    "impact": "High",
                    "validity": "30-60 minutes",
                    "reasoning": "Call writers dominating, resistance strengthening.",
                    "time": now,
                    "index": index_name,
                })

        # VIX alerts
        if vix_data:
            vix_bias = vix_data.get('trade_bias', 'Neutral')
            if self.prev_vix_bias and self.prev_vix_bias != vix_bias:
                if 'Bullish' in vix_bias:
                    alerts.append({
                        "type": "VIX Bullish Confirmation",
                        "message": f"{index_name}: VIX confirms bullish - {vix_data.get('pattern', '')}",
                        "confidence": vix_data.get('probability_score', 50),
                        "priority": "Medium",
                        "impact": "High",
                        "validity": "15-45 minutes",
                        "reasoning": f"VIX pattern: {vix_data.get('pattern', '')}. Risk score: {vix_data.get('risk_score', 50)}",
                        "time": now,
                        "index": index_name,
                    })
                elif 'Bearish' in vix_bias:
                    alerts.append({
                        "type": "VIX Bearish Confirmation",
                        "message": f"{index_name}: VIX confirms bearish - {vix_data.get('pattern', '')}",
                        "confidence": vix_data.get('probability_score', 50),
                        "priority": "Medium",
                        "impact": "High",
                        "validity": "15-45 minutes",
                        "reasoning": f"VIX pattern: {vix_data.get('pattern', '')}",
                        "time": now,
                        "index": index_name,
                    })

            if vix_data.get('divergence'):
                alerts.append({
                    "type": "VIX Divergence",
                    "message": f"{index_name}: VIX divergence detected",
                    "confidence": 72,
                    "priority": "High",
                    "impact": "High",
                    "validity": "30 minutes",
                    "reasoning": "Price and VIX moving in same direction - potential reversal signal.",
                    "time": now,
                    "index": index_name,
                })

            if vix_data.get('trap'):
                alerts.append({
                    "type": "VIX Trap Detection",
                    "message": f"{index_name}: VIX trap detected - potential reversal",
                    "confidence": 68,
                    "priority": "High",
                    "impact": "Moderate",
                    "validity": "15-30 minutes",
                    "reasoning": "VIX declining despite price fall - short-term bottom may form.",
                    "time": now,
                    "index": index_name,
                })

            self.prev_vix_bias = vix_bias

        # Institutional
        if institutional_data:
            pos = institutional_data.get('positioning', '')
            if pos == "Long Unwinding":
                alerts.append({
                    "type": "Long Unwinding",
                    "message": f"{index_name}: Institutional long unwinding detected",
                    "confidence": institutional_data.get('confidence', 60),
                    "priority": "High",
                    "impact": "High",
                    "validity": "30-60 minutes",
                    "reasoning": institutional_data.get('description', ''),
                    "time": now,
                    "index": index_name,
                })
            elif pos == "Short Covering":
                alerts.append({
                    "type": "Short Covering",
                    "message": f"{index_name}: Institutional short covering detected",
                    "confidence": institutional_data.get('confidence', 60),
                    "priority": "High",
                    "impact": "High",
                    "validity": "30-60 minutes",
                    "reasoning": institutional_data.get('description', ''),
                    "time": now,
                    "index": index_name,
                })
            elif pos == "Smart Money Buying" or (pos == "Long Build-Up" and institutional_data.get('confidence', 0) > 75):
                alerts.append({
                    "type": "Smart Money Buying",
                    "message": f"{index_name}: Smart money buying detected",
                    "confidence": institutional_data.get('confidence', 70),
                    "priority": "High",
                    "impact": "Very High",
                    "validity": "1-2 hours",
                    "reasoning": institutional_data.get('description', ''),
                    "time": now,
                    "index": index_name,
                })

        # SMC based alerts
        if smc_labels:
            recent = smc_labels[-3:]
            for label in recent:
                if label['type'] == 'BOS':
                    alert_type = "Breakout Alert" if label['direction'] == 'Bullish' else "Breakdown Alert"
                    alerts.append({
                        "type": alert_type,
                        "message": f"{index_name}: {label['direction']} BOS at {label['price']:.2f}",
                        "confidence": 75,
                        "priority": "High",
                        "impact": "High",
                        "validity": "30-60 minutes",
                        "reasoning": f"Break of structure confirmed in {label['direction']} direction.",
                        "time": now,
                        "index": index_name,
                    })

        # Order block alerts
        if order_blocks:
            for ob in order_blocks[-3:]:
                if ob.get('status') == 'Fresh':
                    alerts.append({
                        "type": f"{ob['type']} Order Block Activated",
                        "message": f"{index_name}: Fresh {ob['type']} OB at {ob['low']:.0f}-{ob['high']:.0f}",
                        "confidence": ob.get('strength', 70),
                        "priority": "Medium",
                        "impact": "Moderate",
                        "validity": "Until tested",
                        "reasoning": f"Institutional order block detected with {ob.get('strength', 70)}% strength.",
                        "time": now,
                        "index": index_name,
                    })

        # S/R alerts
        if sr_data and current_price > 0:
            for r in sr_data.get('resistances', []):
                if abs(current_price - r['price']) / current_price < 0.001:
                    if r['strength'] < 60:
                        alerts.append({
                            "type": "Resistance Weakening",
                            "message": f"{index_name}: Resistance at {r['price']:.2f} weakening",
                            "confidence": 70,
                            "priority": "Medium",
                            "impact": "High",
                            "validity": "15-30 minutes",
                            "reasoning": f"Resistance strength dropped to {r['strength']}%. OI strength: {r.get('oi_strength', 'N/A')}%.",
                            "time": now,
                            "index": index_name,
                        })

            for s in sr_data.get('supports', []):
                if abs(current_price - s['price']) / current_price < 0.001:
                    if s['strength'] > 80:
                        alerts.append({
                            "type": "Support Strengthening",
                            "message": f"{index_name}: Strong support at {s['price']:.2f}",
                            "confidence": 80,
                            "priority": "Medium",
                            "impact": "High",
                            "validity": "30-60 minutes",
                            "reasoning": f"Support strength: {s['strength']}%. Put OI building.",
                            "time": now,
                            "index": index_name,
                        })

        # Update state
        self.prev_regime = regime.get('state') if regime else None
        self.prev_pcr = option_data.get('pcr') if option_data else None
        self.prev_structure = structure_data.get('structure') if structure_data else None

        return alerts


# ============================================================
# DATA FETCHER & ANALYSIS ORCHESTRATOR
# ============================================================

alert_engine_nifty = AlertEngine()
alert_engine_sensex = AlertEngine()

def parse_candle(raw_candle):
    """Parse raw candle from Upstox API to dict"""
    if isinstance(raw_candle, list) and len(raw_candle) >= 6:
        return {
            "time": raw_candle[0],
            "open": float(raw_candle[1]),
            "high": float(raw_candle[2]),
            "low": float(raw_candle[3]),
            "close": float(raw_candle[4]),
            "volume": int(raw_candle[5]) if raw_candle[5] else 0,
        }
    return None


def fetch_and_analyze(token):
    """Main data fetch and analysis loop"""
    client = UpstoxClient(token)

    try:
        # Fetch quotes
        quote_keys = [
            INSTRUMENT_KEYS["NIFTY"],
            INSTRUMENT_KEYS["SENSEX"],
            INSTRUMENT_KEYS["INDIA_VIX"],
        ]
        quotes = client.get_market_quote(quote_keys)

        if not quotes:
            store.update(market_open=False)
            return {"error": "Market closed", "market_open": False}

        # Parse NIFTY
        nifty_data = quotes.get(INSTRUMENT_KEYS["NIFTY"], {})
        sensex_data = quotes.get(INSTRUMENT_KEYS["SENSEX"], {})
        vix_data_raw = quotes.get(INSTRUMENT_KEYS["INDIA_VIX"], {})

        nifty_price = 0
        sensex_price = 0
        vix_value = 0

        if nifty_data:
            nifty_price = nifty_data.get('last_price', 0) or 0
            ohlc = nifty_data.get('ohlc', {})
            store.update(
                nifty_price=nifty_price,
                nifty_open=ohlc.get('open', 0) or 0,
                nifty_high=ohlc.get('high', 0) or 0,
                nifty_low=ohlc.get('low', 0) or 0,
                nifty_close=ohlc.get('close', 0) or 0,
                nifty_volume=nifty_data.get('volume', 0) or 0,
                nifty_prev_close=ohlc.get('close', 0) or 0,
            )

        if sensex_data:
            sensex_price = sensex_data.get('last_price', 0) or 0
            ohlc = sensex_data.get('ohlc', {})
            store.update(
                sensex_price=sensex_price,
                sensex_open=ohlc.get('open', 0) or 0,
                sensex_high=ohlc.get('high', 0) or 0,
                sensex_low=ohlc.get('low', 0) or 0,
                sensex_close=ohlc.get('close', 0) or 0,
                sensex_volume=sensex_data.get('volume', 0) or 0,
                sensex_prev_close=ohlc.get('close', 0) or 0,
            )

        if vix_data_raw:
            vix_value = vix_data_raw.get('last_price', 0) or 0
            vix_ohlc = vix_data_raw.get('ohlc', {})
            prev_vix = vix_ohlc.get('close', vix_value) or vix_value
            vix_change = ((vix_value - prev_vix) / prev_vix * 100) if prev_vix > 0 else 0
            store.update(
                vix_value=vix_value,
                vix_change=vix_change,
                prev_vix=prev_vix,
            )

        if nifty_price <= 0 and sensex_price <= 0:
            store.update(market_open=False)
            return {"error": "Market closed", "market_open": False}

        store.update(market_open=True)

        # Fetch intraday candles for NIFTY
        nifty_candles_raw = client.get_intraday_candles(INSTRUMENT_KEYS["NIFTY"], "5minute")
        nifty_candles = []
        if nifty_candles_raw:
            for c in reversed(nifty_candles_raw):  # Upstox returns newest first
                parsed = parse_candle(c)
                if parsed:
                    nifty_candles.append(parsed)

        # If no intraday, try historical
        if not nifty_candles:
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            nifty_candles_raw = client.get_historical_candles(
                INSTRUMENT_KEYS["NIFTY"], "5minute", yesterday, today
            )
            for c in reversed(nifty_candles_raw or []):
                parsed = parse_candle(c)
                if parsed:
                    nifty_candles.append(parsed)

        store.nifty_candles_5m = deque(nifty_candles, maxlen=500)

        # Fetch intraday candles for SENSEX
        sensex_candles_raw = client.get_intraday_candles(INSTRUMENT_KEYS["SENSEX"], "5minute")
        sensex_candles = []
        if sensex_candles_raw:
            for c in reversed(sensex_candles_raw):
                parsed = parse_candle(c)
                if parsed:
                    sensex_candles.append(parsed)

        if not sensex_candles:
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            sensex_candles_raw = client.get_historical_candles(
                INSTRUMENT_KEYS["SENSEX"], "5minute", yesterday, today
            )
            for c in reversed(sensex_candles_raw or []):
                parsed = parse_candle(c)
                if parsed:
                    sensex_candles.append(parsed)

        store.sensex_candles_5m = deque(sensex_candles, maxlen=500)

        # Calculate VWAP
        if nifty_candles:
            store.update(nifty_vwap=round(TechnicalEngine.calculate_vwap(nifty_candles), 2))
        if sensex_candles:
            store.update(sensex_vwap=round(TechnicalEngine.calculate_vwap(sensex_candles), 2))

        # Fetch Option Chain
        option_chain_raw = client.get_option_chain("NSE_INDEX|Nifty 50")
        option_analysis = OptionChainEngine.analyze(option_chain_raw, nifty_price)
        store.option_chain = option_analysis

        # Run analysis for selected index (NIFTY as primary)
        candles = nifty_candles if nifty_candles else sensex_candles
        price = nifty_price if nifty_price > 0 else sensex_price

        if candles and price > 0:
            # Technical
            closes = [c['close'] for c in candles]
            atr = TechnicalEngine.calculate_atr(candles)

            # Swing points
            swing_highs, swing_lows = TechnicalEngine.find_swing_points(candles)

            # SMC
            smc_labels = SmartMoneyEngine.detect_bos_choch(candles, swing_highs, swing_lows)
            order_blocks = SmartMoneyEngine.detect_order_blocks(candles)
            fvg_zones = SmartMoneyEngine.detect_fvg(candles)
            liquidity_sweeps = SmartMoneyEngine.detect_liquidity_sweeps(candles, swing_highs, swing_lows)
            liquidity_levels = SmartMoneyEngine.detect_liquidity_levels(
                candles, swing_highs, swing_lows, option_analysis.get('strikes_data')
            )
            premium_zone, discount_zone = SmartMoneyEngine.detect_premium_discount_zones(candles)

            store.smc_labels = smc_labels
            store.order_blocks = order_blocks
            store.fvg_zones = fvg_zones
            store.liquidity_levels = liquidity_levels

            # S/R
            sr_data = SupportResistanceEngine.calculate(candles, option_analysis, price)
            store.support_resistance = sr_data

            # VWAP analysis
            vwap_analysis = VWAPEngine.analyze(candles, price)

            # VIX analysis
            price_change_pct = ((price - (store.nifty_prev_close or price)) /
                                max(1, store.nifty_prev_close or price) * 100)
            vix_analysis = VIXEngine.analyze(
                store.vix_value, store.vix_change, price_change_pct, store.prev_vix
            )

            # Institutional
            institutional_analysis = InstitutionalEngine.analyze(
                store.nifty_futures_oi,
                store.nifty_futures_oi_change,
                price_change_pct,
                store.nifty_volume,
            )

            # Market regime
            regime = MarketRegimeEngine.determine(
                candles, option_analysis, vix_analysis,
                vwap_analysis, institutional_analysis, smc_labels
            )
            store.regime = regime

            # Market structure
            structure = MarketStructureEngine.determine(candles, smc_labels, vwap_analysis)

            # AI Decision
            decision = AIDecisionEngine.generate_decision(
                "NIFTY", price, regime, sr_data, option_analysis,
                institutional_analysis, vix_analysis, vwap_analysis,
                structure, smc_labels
            )
            store.ai_decision = decision

            # Trade setups
            trade_setups = TradeFinderEngine.generate_setups(
                candles, decision, sr_data, atr
            )
            store.trade_setups = trade_setups

            # AI Commentary
            commentary = AICommentaryEngine.generate(
                "NIFTY", price, regime, sr_data, option_analysis,
                institutional_analysis, vix_analysis, vwap_analysis,
                structure, smc_labels, order_blocks, decision
            )
            store.ai_commentary = commentary

            # Alerts
            new_alerts = alert_engine_nifty.generate_alerts(
                "NIFTY", regime, option_analysis, vix_analysis,
                institutional_analysis, structure, smc_labels,
                order_blocks, sr_data, price
            )
            for alert in new_alerts:
                store.alerts.appendleft(alert)

        return {"success": True, "market_open": True}

    except Exception as e:
        logger.error(f"Fetch error: {e}", exc_info=True)
        return {"error": str(e)}


def run_analysis_for_index(token, index_name):
    """Run analysis specifically for SENSEX"""
    client = UpstoxClient(token)
    candles = list(store.sensex_candles_5m) if index_name == "SENSEX" else list(store.nifty_candles_5m)
    price = store.sensex_price if index_name == "SENSEX" else store.nifty_price

    if not candles or price <= 0:
        return {}

    closes = [c['close'] for c in candles]
    atr = TechnicalEngine.calculate_atr(candles)
    swing_highs, swing_lows = TechnicalEngine.find_swing_points(candles)

    smc_labels = SmartMoneyEngine.detect_bos_choch(candles, swing_highs, swing_lows)
    order_blocks = SmartMoneyEngine.detect_order_blocks(candles)
    fvg_zones = SmartMoneyEngine.detect_fvg(candles)
    liquidity_levels = SmartMoneyEngine.detect_liquidity_levels(candles, swing_highs, swing_lows)

    sr_data = SupportResistanceEngine.calculate(candles, None, price)
    vwap_analysis = VWAPEngine.analyze(candles, price)

    price_change_pct = ((price - (store.sensex_prev_close or price)) /
                        max(1, store.sensex_prev_close or price) * 100)
    vix_analysis = VIXEngine.analyze(store.vix_value, store.vix_change, price_change_pct)

    institutional_analysis = InstitutionalEngine.analyze(0, 0, price_change_pct, store.sensex_volume)

    regime = MarketRegimeEngine.determine(
        candles, None, vix_analysis, vwap_analysis, institutional_analysis, smc_labels
    )

    structure = MarketStructureEngine.determine(candles, smc_labels, vwap_analysis)

    decision = AIDecisionEngine.generate_decision(
        "SENSEX", price, regime, sr_data, None,
        institutional_analysis, vix_analysis, vwap_analysis,
        structure, smc_labels
    )

    trade_setups = TradeFinderEngine.generate_setups(candles, decision, sr_data, atr)

    commentary = AICommentaryEngine.generate(
        "SENSEX", price, regime, sr_data, None,
        institutional_analysis, vix_analysis, vwap_analysis,
        structure, smc_labels, order_blocks, decision
    )

    new_alerts = alert_engine_sensex.generate_alerts(
        "SENSEX", regime, None, vix_analysis,
        institutional_analysis, structure, smc_labels,
        order_blocks, sr_data, price
    )
    for alert in new_alerts:
        store.alerts.appendleft(alert)

    return {
        "regime": regime,
        "structure": structure,
        "decision": decision,
        "sr_data": sr_data,
        "vwap_analysis": vwap_analysis,
        "vix_analysis": vix_analysis,
        "institutional_analysis": institutional_analysis,
        "smc_labels": smc_labels,
        "order_blocks": order_blocks,
        "fvg_zones": fvg_zones,
        "liquidity_levels": liquidity_levels,
        "trade_setups": trade_setups,
        "commentary": commentary,
    }


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    token = data.get('access_token', '').strip()

    if not token:
        return jsonify({"status": "error", "message": "Token required"}), 400

    client = UpstoxClient(token)
    result = client.validate_token()

    if result['valid']:
        session['token'] = token
        session['user'] = result.get('user', {})
        return jsonify({
            "status": "success",
            "message": "Connected",
            "user": result.get('user', {}),
        })
    else:
        return jsonify({
            "status": "error",
            "message": result.get('error', 'Invalid Token'),
        }), 401


@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    token = session.get('token')
    if not token:
        return jsonify({"error": "Not authenticated"}), 401

    result = fetch_and_analyze(token)
    if result.get('error') and 'Market closed' in result.get('error', ''):
        return jsonify({"status": "market_closed", "message": "Market Closed"})

    snapshot = store.get_snapshot()
    return jsonify({
        "status": "success",
        "data": snapshot,
    })


@app.route('/api/full-analysis', methods=['GET'])
def get_full_analysis():
    token = session.get('token')
    if not token:
        return jsonify({"error": "Not authenticated"}), 401

    index_name = request.args.get('index', 'NIFTY')

    # Fetch fresh data
    fetch_and_analyze(token)

    if index_name == "SENSEX":
        sensex_analysis = run_analysis_for_index(token, "SENSEX")
        candles = list(store.sensex_candles_5m)
        price = store.sensex_price

        return jsonify({
            "status": "success" if store.market_open else "market_closed",
            "market_open": store.market_open,
            "index": "SENSEX",
            "price": price,
            "candles": candles[-100:],
            "regime": sensex_analysis.get('regime', store.regime),
            "support_resistance": sensex_analysis.get('sr_data', {}),
            "option_chain": {},
            "vix": sensex_analysis.get('vix_analysis', {}),
            "vwap": sensex_analysis.get('vwap_analysis', {}),
            "institutional": sensex_analysis.get('institutional_analysis', {}),
            "structure": sensex_analysis.get('structure', {}),
            "smc_labels": sensex_analysis.get('smc_labels', []),
            "order_blocks": sensex_analysis.get('order_blocks', []),
            "fvg_zones": sensex_analysis.get('fvg_zones', []),
            "liquidity_levels": sensex_analysis.get('liquidity_levels', {}),
            "decision": sensex_analysis.get('decision', {}),
            "trade_setups": sensex_analysis.get('trade_setups', {}),
            "commentary": sensex_analysis.get('commentary', ''),
            "alerts": list(store.alerts)[:20],
            "snapshot": store.get_snapshot(),
        })
    else:
        candles = list(store.nifty_candles_5m)
        price = store.nifty_price

        closes = [c['close'] for c in candles] if candles else []
        atr = TechnicalEngine.calculate_atr(candles) if candles else 0

        price_change_pct = ((price - (store.nifty_prev_close or price)) /
                            max(1, store.nifty_prev_close or price) * 100) if price > 0 else 0

        vwap_analysis = VWAPEngine.analyze(candles, price) if candles else {}
        vix_analysis = VIXEngine.analyze(store.vix_value, store.vix_change, price_change_pct, store.prev_vix)

        return jsonify({
            "status": "success" if store.market_open else "market_closed",
            "market_open": store.market_open,
            "index": "NIFTY",
            "price": price,
            "candles": candles[-100:],
            "regime": store.regime,
            "support_resistance": store.support_resistance,
            "option_chain": store.option_chain,
            "vix": vix_analysis,
            "vwap": vwap_analysis,
            "institutional": InstitutionalEngine.analyze(
                store.nifty_futures_oi, store.nifty_futures_oi_change,
                price_change_pct, store.nifty_volume
            ),
            "structure": MarketStructureEngine.determine(candles, store.smc_labels, vwap_analysis) if candles else {},
            "smc_labels": store.smc_labels,
            "order_blocks": store.order_blocks,
            "fvg_zones": store.fvg_zones,
            "liquidity_levels": store.liquidity_levels,
            "decision": store.ai_decision,
            "trade_setups": store.trade_setups,
            "commentary": store.ai_commentary,
            "alerts": list(store.alerts)[:20],
            "snapshot": store.get_snapshot(),
        })


@app.route('/api/option-chain-detail', methods=['GET'])
def get_option_chain_detail():
    token = session.get('token')
    if not token:
        return jsonify({"error": "Not authenticated"}), 401

    return jsonify({
        "status": "success",
        "data": store.option_chain,
    })


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    return jsonify({
        "status": "success",
        "alerts": list(store.alerts)[:50],
    })


@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({"status": "success"})


# ============================================================
# WEBSOCKET FOR REAL-TIME UPDATES
# ============================================================

@sock.route('/ws')
def websocket(ws):
    token = session.get('token')
    if not token:
        ws.send(json.dumps({"error": "Not authenticated"}))
        return

    try:
        while True:
            try:
                fetch_and_analyze(token)
                snapshot = store.get_snapshot()

                ws.send(json.dumps({
                    "type": "market_update",
                    "data": snapshot,
                    "regime": store.regime,
                    "alerts": list(store.alerts)[:5],
                    "timestamp": datetime.now().isoformat(),
                }))
            except Exception as e:
                logger.error(f"WS update error: {e}")

            time.sleep(5)  # Update every 5 seconds

    except Exception as e:
        logger.info(f"WebSocket closed: {e}")


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)