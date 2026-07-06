import asyncio
import websockets
import json
import threading
import time
import os
import signal
import sys
import socket
import requests
import re
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
PORT = int(os.environ.get('PORT', 1234))

# ==================== CẤU HÌNH ====================
MAX_HISTORY = 100000
DATA_FILE = 'history.json'
PREDICTIONS_FILE = 'predictions.json'
STATS_FILE = 'stats.json'
HISTORY_API_URL = os.environ.get('HISTORY_API_URL', 'https://apisunwinhistory.onrender.com/api/history')

# ==================== BIẾN TOÀN CỤC ====================
current_result = {
    "phien": None,
    "xuc_xac_1": None,
    "xuc_xac_2": None,
    "xuc_xac_3": None,
    "tong": None,
    "ket_qua": "",
    "thoi_gian": ""
}

history = []          # Lịch sử phiên (mới nhất ở đầu)
predictions_log = []  # Lịch sử dự đoán
current_session_id = None
ws_connection = None
reconnect_delay = 2.5
reconnect_interval = 10.0
start_time = time.time()

# ==================== THỐNG KÊ ====================
stats = {
    "total_predictions": 0,
    "correct": 0,
    "wrong": 0,
    "accuracy": 0,
    "current_streak": 0,
    "best_streak": 0,
    "worst_streak": 0,
    "last_updated": None
}

detailed_stats = {
    "by_pattern": {},
    "by_confidence": {
        "0-50": {"total": 0, "correct": 0, "wrong": 0},
        "51-60": {"total": 0, "correct": 0, "wrong": 0},
        "61-70": {"total": 0, "correct": 0, "wrong": 0},
        "71-80": {"total": 0, "correct": 0, "wrong": 0},
        "81-90": {"total": 0, "correct": 0, "wrong": 0},
        "91-100": {"total": 0, "correct": 0, "wrong": 0}
    },
    "by_prediction": {
        "Tài": {"total": 0, "correct": 0, "wrong": 0},
        "Xỉu": {"total": 0, "correct": 0, "wrong": 0}
    }
}

# ==================== HÀM TIỆN ÍCH ====================
def get_vietnam_time():
    utc7_time = datetime.utcnow() + timedelta(hours=7)
    return utc7_time.strftime("%d-%m-%Y %H:%M:%S") + " UTC+7"

def vn_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def normalize_result(result):
    if result is None:
        return None
    r = str(result).upper().strip()
    if r in ['TAI', 'TÀI']:
        return 'Tài'
    elif r in ['XIU', 'XỈU']:
        return 'Xỉu'
    return result

# ==================== XỬ LÝ TOKEN ====================
def parse_token_data(token_text):
    try:
        info_match = re.search(r'"info"\x07([^"]+?)"?', token_text)
        if info_match:
            info_str = info_match.group(1)
            info_str = info_str.replace('\x04', '').replace('\x07', '').replace('\x05', '').replace('\x06', '')
            return json.loads(info_str)
        json_match = re.search(r'\{[^{}]*"ipAddress"[^{}]*\}', token_text)
        if json_match:
            return json.loads(json_match.group())
        return None
    except Exception as e:
        print(f"[❌] Lỗi parse token: {e}")
        return None

def load_token():
    try:
        with open('token.txt', 'r', encoding='utf-8') as f:
            token_data = f.read().strip()
        if not token_data:
            print("[❌] File token.txt trống")
            return None
        parsed = parse_token_data(token_data)
        if parsed:
            print("[✅] Đã load token từ token.txt")
            return parsed
        return None
    except FileNotFoundError:
        print("[❌] Không tìm thấy file token.txt")
        return None
    except Exception as e:
        print(f"[❌] Lỗi đọc token: {e}")
        return None

TOKEN_DATA = load_token()

# ==================== CẤU HÌNH WEBSOCKET ====================
if TOKEN_DATA:
    WEBSOCKET_URL = f"wss://websocket.azhkthg1.net/websocket?token={TOKEN_DATA.get('wsToken', '')}"
    WS_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://play.sun.pw"
    }
    initial_messages = [
        [1, "MiniGame", TOKEN_DATA.get('username', 'GM_quapotjz'), "quapit", {
            "signature": "05915B436159B8F4E4DFF537639BD014D54EBEFA18CF62A8EB205B4074010AD72AEA9A780D5A8A4E1BD59BBBAFE03902C594B5DA56FD60D099F1FDDCCD48385FCC2760B5B0B4B8E75D39B8E40DF8CB7C01EA58DBEDA32805927473AB71FA9B798B0C2EDC445C3E36E47EF0AAFAD45601D99AAD1EC642FD2B63573A0401D6EC69",
            "expireIn": TOKEN_DATA.get('timestamp', 1774138177205),
            "wsToken": TOKEN_DATA.get('wsToken', ''),
            "accessToken": "7e9a9ecbff1b4a6393b48346f6d8b709",
            "message": "Thành công",
            "refreshToken": TOKEN_DATA.get('refreshToken', ''),
            "info": TOKEN_DATA
        }],
        [6, "MiniGame", "taixiuPlugin", {"cmd": 1005}],
        [6, "MiniGame", "lobbyPlugin", {"cmd": 10001}]
    ]
else:
    print("[❌] Không load được token, dùng token mặc định (có thể không hoạt động)")
    WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJsb2xtYW1heXN1MTIiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMzkxMDEyNTEsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJlbWFpbCI6IiIsInRpbWVzdGFtcCI6MTc3NDEzODE3NzIwNCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDA1OjQ4MDI6NGU0Mjo0MTcwOjcxMDQ6YjY0Njo2Nzg5Ojg2NDgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzA5LnBuZyIsInBsYXRmb3JtSWQiOjQsInVzZXJJZCI6ImEyOGEwZjA2LWU4OGYtNDRiNy1hMjY4LTVmNmRhZDk0OWZiZiIsImVtYWlsVmVyaWZpZWQiOm51bGwsInJlZ1RpbWUiOjE3NzMxMDY2NDkxOTksInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fcXVhcG90anoifQ.3ycgvK1-PwRpBqANZJ3li00kpuzV6Ike6ZjYPthf3X0"
    WS_HEADERS = {"User-Agent": "Mozilla/5.0", "Origin": "https://play.sun.pw"}
    initial_messages = [
        [1, "MiniGame", "GM_quapotjz", "quapit", {
            "signature": "05915B436159B8F4E4DFF537639BD014D54EBEFA18CF62A8EB205B4074010AD72AEA9A780D5A8A4E1BD59BBBAFE03902C594B5DA56FD60D099F1FDDCCD48385FCC2760B5B0B4B8E75D39B8E40DF8CB7C01EA58DBEDA32805927473AB71FA9B798B0C2EDC445C3E36E47EF0AAFAD45601D99AAD1EC642FD2B63573A0401D6EC69",
            "expireIn": 1774138177205,
            "wsToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJsb2xtYW1heXN1MTIiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMzkxMDEyNTEsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJlbWFpbCI6IiIsInRpbWVzdGFtcCI6MTc3NDEzODE3NzIwNCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDA1OjQ4MDI6NGU0Mjo0MTcwOjcxMDQ6YjY0Njo2Nzg5Ojg2NDgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzA5LnBuZyIsInBsYXRmb3JtSWQiOjQsInVzZXJJZCI6ImEyOGEwZjA2LWU4OGYtNDRiNy1hMjY4LTVmNmRhZDk0OWZiZiIsImVtYWlsVmVyaWZpZWQiOm51bGwsInJlZ1RpbWUiOjE3NzMxMDY2NDkxOTksInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fcXVhcG90anoifQ.3ycgvK1-PwRpBqANZJ3li00kpuzV6Ike6ZjYPthf3X0",
            "accessToken": "7e9a9ecbff1b4a6393b48346f6d8b709",
            "message": "Thành công",
            "refreshToken": "950f5b9974dd4f4c982a3681af9acbc7.f0d252e72ee64f07bd5819d6ca54bba1",
            "info": {
                "ipAddress": "2405:4802:4e42:4170:7104:b646:6789:8648",
                "wsToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJsb2xtYW1heXN1MTIiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMzkxMDEyNTEsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJlbWFpbCI6IiIsInRpbWVzdGFtcCI6MTc3NDEzODE3NzIwNCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDA1OjQ4MDI6NGU0Mjo0MTcwOjcxMDQ6YjY0Njo2Nzg5Ojg2NDgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzA5LnBuZyIsInBsYXRmb3JtSWQiOjQsInVzZXJJZCI6ImEyOGEwZjA2LWU4OGYtNDRiNy1hMjY4LTVmNmRhZDk0OWZiZiIsImVtYWlsVmVyaWZpZWQiOm51bGwsInJlZ1RpbWUiOjE3NzMxMDY2NDkxOTksInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fcXVhcG90anoifQ.3ycgvK1-PwRpBqANZJ3li00kpuzV6Ike6ZjYPthf3X0",
                "locale": "vi",
                "userId": "a28a0f06-e88f-44b7-a268-5f6dad949fbf",
                "username": "GM_quapotjz",
                "timestamp": 1774138177205,
                "refreshToken": "950f5b9974dd4f4c982a3681af9acbc7.f0d252e72ee64f07bd5819d6ca54bba1"
            }
        }],
        [6, "MiniGame", "taixiuPlugin", {"cmd": 1005}],
        [6, "MiniGame", "lobbyPlugin", {"cmd": 10001}]
    ]

# ==================== AI DỰ ĐOÁN ====================
class TXPredictor:
    def __init__(self):
        self.history = []
        self.error_streak = 0
        self.last_prediction = None
        self.last_pattern = None

    def load_history(self, data):
        self.history = sorted(data, key=lambda x: x.get('phien', 0), reverse=True)

    def get_results(self):
        return [normalize_result(s.get('ket_qua', '')) for s in self.history if s.get('ket_qua')]

    def get_points(self):
        return [s.get('tong', 0) for s in self.history if s.get('tong') is not None]

    def detect_pattern(self, results):
        if len(results) < 2:
            return None

        # 1. Đu Bệt
        length = 1
        for i in range(1, len(results)):
            if results[i] == results[0]:
                length += 1
            else:
                break
        if 3 <= length <= 5:
            return {"prediction": results[0], "confidence": 72 + length * 2, "pattern": "Đu Bệt", "reason": f"Bệt {length} phiên"}
        if length >= 6:
            return {"prediction": "Xỉu" if results[0] == "Tài" else "Tài", "confidence": 80, "pattern": "Bẻ Bệt Rồng", "reason": f"Bệt dài {length} -> bẻ"}

        # 2. Cầu nối 1-1
        if len(results) >= 5 and all(results[i] != results[i+1] for i in range(4)):
            return {"prediction": "Xỉu" if results[0] == "Tài" else "Tài", "confidence": 82, "pattern": "Cầu Nối 1-1", "reason": "Nhịp 1-1 ổn định"}

        # 3. Cầu 2-2, 3-3
        if len(results) >= 4 and results[0] == results[1] and results[2] == results[3] and results[0] != results[2]:
            return {"prediction": results[2], "confidence": 78, "pattern": "Cầu 2-2", "reason": "AABB -> B"}
        if len(results) >= 6 and results[0] == results[1] == results[2] and results[3] == results[4] == results[5] and results[0] != results[3]:
            return {"prediction": results[3], "confidence": 80, "pattern": "Cầu 3-3", "reason": "AAABBB -> B"}

        # 4. Gãy cầu
        if len(results) >= 5:
            if results[0] == results[1] == results[2] and results[2] != results[3] and results[3] == results[4]:
                return {"prediction": results[3], "confidence": 74, "pattern": "Gãy 3-2", "reason": "AAABB -> B"}
            if results[0] == results[1] and results[1] != results[2] and results[2] == results[3] == results[4]:
                return {"prediction": results[2], "confidence": 74, "pattern": "Gãy 2-3", "reason": "AABBB -> B"}

        # 5. Mẫu lặp
        if len(results) >= 6:
            for plen in [2, 3, 4]:
                pattern = results[:plen]
                for i in range(plen, len(results) - plen):
                    if results[i:i+plen] == pattern:
                        return {"prediction": results[i-1], "confidence": 88, "pattern": "Mẫu Lặp", "reason": f"Mẫu {pattern}"}

        # 6. Phân tích điểm
        points = self.get_points()
        if len(points) >= 5:
            last = points[0]
            avg = sum(points[:5]) / 5
            if last >= 15:
                return {"prediction": "Xỉu", "confidence": 75, "pattern": "Vị cực đại", "reason": f"Điểm {last} -> Xỉu"}
            if last <= 5:
                return {"prediction": "Tài", "confidence": 75, "pattern": "Vị cực tiểu", "reason": f"Điểm {last} -> Tài"}
            if avg > 12 and len(points) > 1 and last > points[1]:
                return {"prediction": "Xỉu", "confidence": 68, "pattern": "Vị bão hòa", "reason": "Đà tăng chạm ngưỡng"}
            if avg < 9 and len(points) > 1 and last < points[1]:
                return {"prediction": "Tài", "confidence": 68, "pattern": "Vị cạn kiệt", "reason": "Đà giảm chạm đáy"}

        # 7. Theo
        if results:
            return {"prediction": results[0], "confidence": 55, "pattern": "Theo", "reason": "Bám phiên cuối"}
        return None

    def apply_reversal(self, result):
        if not result:
            return result
        if self.error_streak >= 2 and self.last_prediction:
            return {**result, "prediction": "Xỉu" if result["prediction"] == "Tài" else "Tài", "confidence": min(88, result["confidence"] + 10), "reason": f"🔄 Đảo: {result['reason']}"}
        return result

    def predict(self, data):
        self.load_history(data)
        results = self.get_results()
        if len(results) < 5:
            return {"prediction": "Tài", "confidence": 50, "pattern": "Chưa đủ dữ liệu", "reason": "Cần ít nhất 5 phiên"}
        result = self.detect_pattern(results)
        if result:
            result = self.apply_reversal(result)
            self.last_prediction = result["prediction"]
            self.last_pattern = result["pattern"]
            return result
        return {"prediction": results[0] if results else "Tài", "confidence": 50, "pattern": "Theo", "reason": "Mặc định"}

    def update_result(self, actual):
        if self.last_prediction:
            if self.last_prediction == normalize_result(actual):
                self.error_streak = 0
            else:
                self.error_streak += 1

predictor = TXPredictor()

# ==================== LƯU TRỮ DỮ LIỆU ====================
def save_history():
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "history": history[:MAX_HISTORY],
                "total": len(history),
                "last_updated": get_vietnam_time()
            }, f, ensure_ascii=False, indent=2)
        print(f"💾 Đã lưu {len(history)} phiên vào {DATA_FILE}")
    except Exception as e:
        print(f"[❌] Lỗi lưu history: {e}")

def load_history_file():
    global history
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            history = data.get('history', [])
            print(f"📂 Đã tải {len(history)} phiên từ {DATA_FILE}")
            return True
    except FileNotFoundError:
        print(f"📂 Không tìm thấy {DATA_FILE}, bắt đầu mới")
        return False
    except Exception as e:
        print(f"[❌] Lỗi tải history: {e}")
        return False

def save_predictions():
    try:
        with open(PREDICTIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(predictions_log[-5000:], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[❌] Lỗi lưu predictions: {e}")

def save_stats():
    try:
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump({"stats": stats, "detailed_stats": detailed_stats}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[❌] Lỗi lưu stats: {e}")

def load_stats():
    global stats, detailed_stats
    try:
        with open(STATS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            stats.update(data.get('stats', {}))
            detailed_stats.update(data.get('detailed_stats', {}))
            print("📂 Đã tải thống kê")
            return True
    except FileNotFoundError:
        print("📂 Bắt đầu thống kê mới")
        return False
    except Exception as e:
        print(f"[❌] Lỗi tải stats: {e}")
        return False

def fetch_initial_history_from_api():
    """Lấy lịch sử từ API bên ngoài nếu chưa có dữ liệu"""
    global history
    if len(history) > 0:
        return
    try:
        print(f"[📡] Đang lấy lịch sử từ {HISTORY_API_URL}...")
        response = requests.get(HISTORY_API_URL, params={"limit": 1000}, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and "data" in data:
                history = data["data"]
                history.sort(key=lambda x: x.get('phien', 0), reverse=True)
                print(f"[✅] Đã tải {len(history)} phiên từ API")
                save_history()
                return True
        print(f"[⚠️] Không lấy được lịch sử từ API, status: {response.status_code}")
    except Exception as e:
        print(f"[❌] Lỗi fetch initial history: {e}")
    return False

# ==================== XỬ LÝ PHIÊN MỚI ====================
def process_new_session(session):
    global history, current_result, stats, detailed_stats, current_session_id

    if not session or not session.get('phien'):
        return

    phien = session.get('phien')
    # Kiểm tra trùng lặp
    if any(s.get('phien') == phien for s in history[:10]):
        return

    history.insert(0, session)
    if len(history) > MAX_HISTORY:
        history = history[:MAX_HISTORY]

    current_result = session.copy()
    predictor.load_history(history)
    check_prediction_result(session)

    if len(history) % 10 == 0:
        save_history()
        save_stats()

    print(f"📥 Phiên {phien}: {session.get('xuc_xac_1')}-{session.get('xuc_xac_2')}-{session.get('xuc_xac_3')} = {session.get('tong')} ({session.get('ket_qua')}) - {session.get('thoi_gian')}")

# ==================== DỰ ĐOÁN ====================
def predict_next():
    if len(history) < 10:
        return None
    result = predictor.predict(history)
    if not result:
        return None
    latest_phien = history[0].get('phien', 0)
    predicted_phien = latest_phien + 1
    prediction = {
        "phien_hien_tai": latest_phien,
        "phien_du_doan": predicted_phien,
        "du_doan": result["prediction"],
        "do_tin_cay": result["confidence"],
        "pattern": result["pattern"],
        "reason": result["reason"],
        "thoi_gian": get_vietnam_time(),
        "ket_qua": None,
        "ket_qua_dung_sai": None
    }
    if not any(p.get('phien_du_doan') == predicted_phien for p in predictions_log[-100:]):
        predictions_log.append(prediction)
        save_predictions()
    return prediction

def check_prediction_result(session):
    global stats, detailed_stats
    phien = session.get('phien')
    actual = normalize_result(session.get('ket_qua'))
    if not phien or not actual:
        return
    for pred in predictions_log:
        if pred.get('phien_du_doan') == phien and pred.get('ket_qua') is None:
            pred['ket_qua'] = actual
            correct = pred['du_doan'] == actual
            pred['ket_qua_dung_sai'] = '✅ Đúng' if correct else '❌ Sai'
            stats['total_predictions'] += 1
            if correct:
                stats['correct'] += 1
                stats['current_streak'] += 1
                if stats['current_streak'] > stats['best_streak']:
                    stats['best_streak'] = stats['current_streak']
            else:
                stats['wrong'] += 1
                if stats['current_streak'] > stats['worst_streak']:
                    stats['worst_streak'] = stats['current_streak']
                stats['current_streak'] = 0
            stats['accuracy'] = (stats['correct'] / stats['total_predictions'] * 100) if stats['total_predictions'] > 0 else 0
            stats['last_updated'] = get_vietnam_time()

            pattern = pred.get('pattern', 'Theo')
            if pattern not in detailed_stats['by_pattern']:
                detailed_stats['by_pattern'][pattern] = {"total": 0, "correct": 0, "wrong": 0}
            detailed_stats['by_pattern'][pattern]['total'] += 1
            if correct:
                detailed_stats['by_pattern'][pattern]['correct'] += 1
            else:
                detailed_stats['by_pattern'][pattern]['wrong'] += 1

            conf = pred.get('do_tin_cay', 50)
            conf_range = '0-50'
            if 51 <= conf <= 60: conf_range = '51-60'
            elif 61 <= conf <= 70: conf_range = '61-70'
            elif 71 <= conf <= 80: conf_range = '71-80'
            elif 81 <= conf <= 90: conf_range = '81-90'
            elif conf >= 91: conf_range = '91-100'
            if conf_range in detailed_stats['by_confidence']:
                detailed_stats['by_confidence'][conf_range]['total'] += 1
                if correct:
                    detailed_stats['by_confidence'][conf_range]['correct'] += 1
                else:
                    detailed_stats['by_confidence'][conf_range]['wrong'] += 1

            if pred['du_doan'] in detailed_stats['by_prediction']:
                detailed_stats['by_prediction'][pred['du_doan']]['total'] += 1
                if correct:
                    detailed_stats['by_prediction'][pred['du_doan']]['correct'] += 1
                else:
                    detailed_stats['by_prediction'][pred['du_doan']]['wrong'] += 1

            predictor.update_result(actual)
            print(f"🎯 Dự đoán phiên {phien}: {pred['du_doan']} → {actual} → {pred['ket_qua_dung_sai']}")
            save_predictions()
            save_stats()
            break

# ==================== WEBSOCKET ====================
def get_network_info():
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        try:
            response = requests.get('https://api.ipify.org?format=json', timeout=5)
            public_ip = response.json()['ip']
        except:
            public_ip = None
        return {'localIP': local_ip, 'publicIP': public_ip}
    except:
        return {'localIP': '127.0.0.1', 'publicIP': None}

def get_ws_connect_kwargs():
    kwargs = {"ping_interval": 15, "ping_timeout": 10}
    try:
        ws_version = tuple(int(x) for x in websockets.__version__.split('.')[:2])
        if ws_version >= (11, 0):
            kwargs["additional_headers"] = WS_HEADERS
        else:
            kwargs["extra_headers"] = WS_HEADERS
    except:
        kwargs["additional_headers"] = WS_HEADERS
    return kwargs

async def connect_websocket():
    global ws_connection, current_session_id, current_result

    connect_kwargs = get_ws_connect_kwargs()

    while True:
        try:
            print("[🔄] Đang kết nối WebSocket...")
            ws_connection = await websockets.connect(WEBSOCKET_URL, **connect_kwargs)
            print("[✅] WebSocket connected to Sun.Win")

            for i, msg in enumerate(initial_messages):
                await asyncio.sleep(i * 0.6)
                await ws_connection.send(json.dumps(msg))

            conn_start = time.time()

            async for message in ws_connection:
                if time.time() - conn_start >= reconnect_interval:
                    print(f"[⏳] Đã {reconnect_interval}s, tự động reconnect...")
                    await ws_connection.close()
                    break

                try:
                    data = json.loads(message)
                    if not isinstance(data, list) or len(data) < 2:
                        continue

                    if isinstance(data[1], dict):
                        cmd = data[1].get('cmd')
                        sid = data[1].get('sid')
                        d1 = data[1].get('d1')
                        d2 = data[1].get('d2')
                        d3 = data[1].get('d3')
                        gBB = data[1].get('gBB')

                        # Cập nhật phiên mới khi nhận 1008 (giữ nguyên cho đến khi có 1008 mới)
                        if cmd == 1008 and sid:
                            current_session_id = sid
                            print(f"[🎮] Cập nhật phiên mới: {sid}")

                        # Xử lý kết quả
                        if cmd == 1003 and gBB:
                            # Lấy sid từ message, nếu không có thì dùng current_session_id
                            sid_from_msg = data[1].get('sid')
                            session_id = sid_from_msg if sid_from_msg is not None else current_session_id
                            
                            if session_id is None:
                                # Thử lấy từ dữ liệu khác? Nếu không có thì bỏ qua
                                print("[⚠️] Không có session_id cho kết quả, bỏ qua")
                                continue

                            if d1 is None or d2 is None or d3 is None:
                                continue

                            total = d1 + d2 + d3
                            result = "Tài" if total > 10 else "Xỉu"
                            session_data = {
                                "phien": session_id,
                                "xuc_xac_1": d1,
                                "xuc_xac_2": d2,
                                "xuc_xac_3": d3,
                                "tong": total,
                                "ket_qua": result,
                                "thoi_gian": get_vietnam_time()
                            }
                            process_new_session(session_data)
                            # KHÔNG reset current_session_id ở đây

                except json.JSONDecodeError as e:
                    print(f"[❌] JSON Parse error: {e}")
                except Exception as e:
                    print(f"[❌] Xử lý message error: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[❌] WebSocket đóng: {e}")
            await asyncio.sleep(reconnect_delay)
        except Exception as e:
            print(f"[❌] Kết nối WebSocket lỗi: {e}")
            await asyncio.sleep(reconnect_delay)

# ==================== FLASK API ====================
@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "name": "Sun.Win Tài Xỉu VIP",
        "version": "2.0",
        "endpoints": {
            "/api/tx": "Kết quả mới nhất",
            "/api/history": "Lịch sử (limit=100)",
            "/api/predict": "Dự đoán tiếp theo",
            "/api/stats": "Thống kê tổng quan",
            "/api/detailed_stats": "Thống kê chi tiết",
            "/api/predictions": "Lịch sử dự đoán",
            "/api/summary": "Tóm tắt"
        },
        "total_sessions": len(history),
        "current_user": TOKEN_DATA.get('username') if TOKEN_DATA else "Unknown",
        "thoi_gian": get_vietnam_time()
    })

@app.route('/api/tx', methods=['GET'])
def get_tx_result():
    return jsonify(current_result)

@app.route('/api/history', methods=['GET'])
def get_history():
    limit = request.args.get('limit', 100, type=int)
    limit = min(limit, 1000)
    return jsonify({
        "total": len(history),
        "limit": limit,
        "data": history[:limit]
    })

@app.route('/api/predict', methods=['GET'])
def get_prediction():
    if len(history) < 10:
        return jsonify({"error": "Cần ít nhất 10 phiên để dự đoán"}), 400
    pred = predict_next()
    if not pred:
        return jsonify({"error": "Không thể đưa ra dự đoán"}), 400
    latest = history[0] if history else {}
    return jsonify({
        "phien_hien_tai": latest.get('phien'),
        "ket_qua_hien_tai": latest.get('ket_qua'),
        "tong_hien_tai": latest.get('tong'),
        "phien_du_doan": pred["phien_du_doan"],
        "du_doan": pred["du_doan"],
        "do_tin_cay": pred["do_tin_cay"],
        "pattern": pred["pattern"],
        "reason": pred["reason"],
        "thoi_gian_du_doan": pred["thoi_gian"]
    })

@app.route('/api/predictions', methods=['GET'])
def get_predictions():
    limit = request.args.get('limit', 100, type=int)
    return jsonify({
        "total": len(predictions_log),
        "data": predictions_log[-limit:]
    })

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify(stats)

@app.route('/api/detailed_stats', methods=['GET'])
def get_detailed_stats():
    return jsonify(detailed_stats)

@app.route('/api/summary', methods=['GET'])
def get_summary():
    return jsonify({
        "total_sessions": len(history),
        "total_predictions": stats['total_predictions'],
        "accuracy": f"{stats['accuracy']:.2f}%" if stats['accuracy'] else "0%",
        "current_streak": stats['current_streak'],
        "best_streak": stats['best_streak'],
        "worst_streak": stats['worst_streak'],
        "last_updated": stats['last_updated'],
        "patterns": list(detailed_stats['by_pattern'].keys())
    })

@app.route('/api/force_save', methods=['POST'])
def force_save():
    save_history()
    save_predictions()
    save_stats()
    return jsonify({"message": "Đã lưu tất cả dữ liệu", "total": len(history)})

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint không tồn tại"}), 404

# ==================== RUN FLASK ====================
def run_flask():
    try:
        app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
    except Exception as e:
        print(f"[❌] Flask error: {e}")

# ==================== MAIN ====================
async def main():
    global start_time

    load_history_file()
    if not history:
        fetch_initial_history_from_api()
    load_stats()
    if history:
        predictor.load_history(history)

    network_info = get_network_info()

    print("\n" + "="*60)
    print("🎲 SUN.WIN TÀI XỈU VIP - FULL AI")
    print("="*60)
    if TOKEN_DATA:
        print(f"👤 User: {TOKEN_DATA.get('username', 'Unknown')}")
        print(f"🆔 User ID: {TOKEN_DATA.get('userId', 'Unknown')}")
    print(f"📦 Lịch sử: {len(history)} phiên")
    print(f"📡 Server: http://localhost:{PORT}")
    print(f"🌐 Network: http://{network_info['localIP']}:{PORT}")
    print("="*60)
    print("📊 API Endpoints:")
    print("   🎯 /api/tx - Kết quả mới nhất")
    print("   📜 /api/history - Lịch sử")
    print("   🔮 /api/predict - Dự đoán")
    print("   📊 /api/stats - Thống kê")
    print("   📈 /api/detailed_stats - Thống kê chi tiết")
    print("="*60 + "\n")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    await connect_websocket()

def signal_handler(sig, frame):
    print("\n[👋] Đang tắt server...")
    save_history()
    save_predictions()
    save_stats()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[👋] Server stopped")
    except Exception as e:
        print(f"[❌] Main error: {e}")