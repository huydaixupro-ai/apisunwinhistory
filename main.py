import asyncio
import websockets
import json
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import os
import signal
import sys
import socket
import requests
import re

app = Flask(__name__)
CORS(app)
PORT = int(os.environ.get('PORT', 1234))

# Global variables
current_result = {
    "phien": None,
    "xuc_xac_1": None,
    "xuc_xac_2": None,
    "xuc_xac_3": None,
    "tong": None,
    "ket_qua": "",
    "thoi_gian": ""
}

current_session_id = None
ws_connection = None
reconnect_delay = 2.5          # delay khi reconnect do lỗi
reconnect_interval = 10.0      # tự động reconnect mỗi 10s
start_time = time.time()

# Hàm lấy thời gian Việt Nam (UTC+7)
def get_vietnam_time():
    utc7_time = datetime.utcnow() + timedelta(hours=7)
    return utc7_time.strftime("%d-%m-%Y %H:%M:%S") + " UTC+7"

def parse_token_data(token_text):
    """Parse token data từ file token.txt"""
    try:
        info_match = re.search(r'"info"\x07([^"]+?)"?', token_text)
        if info_match:
            info_str = info_match.group(1)
            info_str = info_str.replace('\x04', '').replace('\x07', '').replace('\x05', '').replace('\x06', '')
            info_data = json.loads(info_str)
            return info_data
        
        json_match = re.search(r'\{[^{}]*"ipAddress"[^{}]*\}', token_text)
        if json_match:
            return json.loads(json_match.group())
        
        return None
    except Exception as e:
        print(f"[❌] Lỗi parse token: {e}")
        return None

def load_token():
    """Load token từ file token.txt"""
    try:
        with open('token.txt', 'r', encoding='utf-8') as f:
            token_data = f.read().strip()
        if not token_data:
            print("[❌] File token.txt trống")
            return None
        parsed_data = parse_token_data(token_data)
        if parsed_data:
            print("[✅] Đã load token từ token.txt")
            return parsed_data
        else:
            print("[❌] Không thể parse token từ token.txt")
            return None
    except FileNotFoundError:
        print("[❌] Không tìm thấy file token.txt")
        return None
    except Exception as e:
        print(f"[❌] Lỗi đọc token.txt: {e}")
        return None

# Load token data
TOKEN_DATA = load_token()

if TOKEN_DATA:
    WEBSOCKET_URL = f"wss://websocket.azhkthg1.net/websocket?token={TOKEN_DATA.get('wsToken', '')}"
    WS_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Origin": "https://play.sun.pw"
    }
    initial_messages = [
        [
            1,
            "MiniGame",
            TOKEN_DATA.get('username', 'GM_quapotjz'),
            "quapit",
            {
                "signature": "05915B436159B8F4E4DFF537639BD014D54EBEFA18CF62A8EB205B4074010AD72AEA9A780D5A8A4E1BD59BBBAFE03902C594B5DA56FD60D099F1FDDCCD48385FCC2760B5B0B4B8E75D39B8E40DF8CB7C01EA58DBEDA32805927473AB71FA9B798B0C2EDC445C3E36E47EF0AAFAD45601D99AAD1EC642FD2B63573A0401D6EC69",
                "expireIn": TOKEN_DATA.get('timestamp', 1774138177205),
                "wsToken": TOKEN_DATA.get('wsToken', ''),
                "accessToken": "7e9a9ecbff1b4a6393b48346f6d8b709",
                "message": "Thành công",
                "refreshToken": TOKEN_DATA.get('refreshToken', ''),
                "info": TOKEN_DATA
            }
        ],
        [6, "MiniGame", "taixiuPlugin", {"cmd": 1005}],
        [6, "MiniGame", "lobbyPlugin", {"cmd": 10001}]
    ]
else:
    print("[❌] Không thể load token, sử dụng token mặc định (có thể không hoạt động)")
    WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJsb2xtYW1heXN1MTIiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMzkxMDEyNTEsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJlbWFpbCI6IiIsInRpbWVzdGFtcCI6MTc3NDEzODE3NzIwNCwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyNDA1OjQ4MDI6NGU0Mjo0MTcwOjcxMDQ6YjY0Njo2Nzg5Ojg2NDgiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9pbWFnZXMuc3dpbnNob3AubmV0L2ltYWdlcy9hdmF0YXIvYXZhdGFyXzA5LnBuZyIsInBsYXRmb3JtSWQiOjQsInVzZXJJZCI6ImEyOGEwZjA2LWU4OGYtNDRiNy1hMjY4LTVmNmRhZDk0OWZiZiIsImVtYWlsVmVyaWZpZWQiOm51bGwsInJlZ1RpbWUiOjE3NzMxMDY2NDkxOTksInBob25lIjoiIiwiZGVwb3NpdCI6ZmFsc2UsInVzZXJuYW1lIjoiR01fcXVhcG90anoifQ.3ycgvK1-PwRpBqANZJ3li00kpuzV6Ike6ZjYPthf3X0"
    WS_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Origin": "https://play.sun.pw"
    }
    initial_messages = [
        [
            1,
            "MiniGame",
            "GM_quapotjz",
            "quapit",
            {
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
            }
        ],
        [6, "MiniGame", "taixiuPlugin", {"cmd": 1005}],
        [6, "MiniGame", "lobbyPlugin", {"cmd": 10001}]
    ]

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
    except Exception as e:
        print(f"Lỗi lấy network info: {e}")
        return {'localIP': '127.0.0.1', 'publicIP': None}

def handle_error(context, error):
    error_msg = f"Lỗi - {context}: {str(error)}"
    print(f"[❌] {error_msg}")
    return error_msg

def get_ws_connect_kwargs():
    kwargs = {"ping_interval": 15, "ping_timeout": 10}
    try:
        ws_version = tuple(int(x) for x in websockets.__version__.split('.')[:2])
        if ws_version >= (11, 0):
            kwargs["additional_headers"] = WS_HEADERS
        else:
            kwargs["extra_headers"] = WS_HEADERS
    except Exception:
        kwargs["additional_headers"] = WS_HEADERS
    return kwargs

async def connect_websocket():
    global ws_connection, current_session_id, current_result

    connect_kwargs = get_ws_connect_kwargs()

    while True:
        try:
            print("[🔄] Đang kết nối WebSocket...")
            ws_connection = await websockets.connect(
                WEBSOCKET_URL,
                **connect_kwargs
            )
            print("[✅] WebSocket connected to Sun.Win")

            # Gửi initial messages
            for i, msg in enumerate(initial_messages):
                await asyncio.sleep(i * 0.6)
                await ws_connection.send(json.dumps(msg))

            # Lưu thời gian bắt đầu kết nối để tự động reconnect sau 10s
            conn_start = time.time()

            # Vòng lặp nhận tin
            async for message in ws_connection:
                # Kiểm tra tự động reconnect sau 10s
                if time.time() - conn_start >= reconnect_interval:
                    print(f"[⏳] Đã {reconnect_interval}s, tự động reconnect...")
                    await ws_connection.close()
                    break   # Thoát khỏi vòng lặp nhận tin, vòng while ngoài sẽ reconnect

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

                        # Cập nhật phiên mới
                        if cmd == 1008 and sid:
                            current_session_id = sid
                            print(f"[🎮] Phiên mới từ 1008: {sid}")

                        # Xử lý kết quả
                        if cmd == 1003 and gBB:
                            # Ưu tiên lấy sid trực tiếp từ message 1003
                            session_id = data[1].get('sid')
                            if session_id is None:
                                session_id = current_session_id
                            else:
                                # Nếu có sid trong 1003 thì dùng luôn, không cần reset
                                pass

                            if session_id is None:
                                print("[⚠️] Không có session_id cho kết quả, bỏ qua")
                                continue

                            if d1 is None or d2 is None or d3 is None:
                                continue

                            total = d1 + d2 + d3
                            result = "Tài" if total > 10 else "Xỉu"

                            current_result = {
                                "phien": session_id,
                                "xuc_xac_1": d1,
                                "xuc_xac_2": d2,
                                "xuc_xac_3": d3,
                                "tong": total,
                                "ket_qua": result,
                                "thoi_gian": get_vietnam_time()
                            }

                            print(f"[🎲] Phiên {session_id}: {d1}-{d2}-{d3} = {total} ({result}) - {current_result['thoi_gian']}")

                            # Reset current_session_id nếu không có sid từ 1003
                            if data[1].get('sid') is None:
                                current_session_id = None

                except json.JSONDecodeError as e:
                    handle_error("Parse JSON", e)
                except Exception as e:
                    handle_error("Xử lý message", e)

        except websockets.exceptions.ConnectionClosed as e:
            handle_error("WebSocket đóng", e)
            await asyncio.sleep(reconnect_delay)
        except Exception as e:
            handle_error("Kết nối WebSocket", e)
            await asyncio.sleep(reconnect_delay)

# Flask routes
@app.route('/api/tx', methods=['GET'])
def get_tx_result():
    return jsonify(current_result)

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        "name": "Sun.Win Tài Xỉu Data Stream",
        "version": "1.0",
        "endpoints": {"/api/tx": "Lấy kết quả tài xỉu mới nhất"},
        "thoi_gian": get_vietnam_time(),
        "current_user": TOKEN_DATA.get('username') if TOKEN_DATA else "Unknown"
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint không tồn tại. Chỉ có /api/tx"}), 404

def run_flask():
    try:
        app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
    except Exception as e:
        handle_error("Flask server", e)

async def main():
    global start_time
    start_time = time.time()

    network_info = get_network_info()

    print("\n" + "="*60)
    print("🎲 Sun.Win Tài Xỉu Data Stream")
    print("="*60)
    if TOKEN_DATA:
        print(f"👤 Đang dùng token của: {TOKEN_DATA.get('username', 'Unknown')}")
        print(f"🆔 User ID: {TOKEN_DATA.get('userId', 'Unknown')}")
        print(f"🌐 IP: {TOKEN_DATA.get('ipAddress', 'Unknown')}")
    print(f"📡 Server running on:")
    print(f"   Local: http://localhost:{PORT}")
    print(f"   Network: http://{network_info['localIP']}:{PORT}")
    print(f"🔧 websockets version: {websockets.__version__}")
    print("="*60)
    print("🔌 Connecting to Sun.Win WebSocket...")
    print("="*60 + "\n")
    print("📊 API Endpoint:")
    print(f"   🎯 /api/tx - Lấy kết quả tài xỉu mới nhất")
    print("="*60 + "\n")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    await connect_websocket()

def signal_handler(sig, frame):
    print("\n[👋] Đang tắt server...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[👋] Server stopped by user")
    except Exception as e:
        handle_error("Main", e)