#!/usr/bin/env python3
# logger_ws.py
# TEAM KAI Data Logger - CAN + IMU/GPS raw -> WebSocket + HTTP dashboard + files
# Updated: 2026-08 (WTGAHRS3 통합) / 0x501 Pack_Summed_Voltage 필드 추가

import can
import csv
import os
import json
import time
import struct
import asyncio
import serial
import websockets
from aiohttp import web
from datetime import datetime

from witmotion import Stream, parse_packet

LOG_DIR   = '/home/hsm/fsae-logger/logs'
DASH_PATH = '/home/hsm/fsae-logger/code/dashboard.html'
WS_HOST   = '0.0.0.0'
WS_PORT   = 8765
HTTP_PORT = 8080

IMU_PORT  = '/dev/ttySC0'
IMU_BAUD  = 115200
IMU_BCAST_HZ = 10          # 대시보드 전송률 (50Hz 전부 보내면 과부하)
CAN_BATCH_HZ = 50          # CAN 프레임 묶음 전송률

os.makedirs(LOG_DIR, exist_ok=True)
connected_clients = set()


# ── WebSocket ────────────────────────────────────────────────
async def ws_handler(websocket):
    connected_clients.add(websocket)
    print(f'[WS]  Connected:    {websocket.remote_address}')
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        print(f'[WS]  Disconnected: {websocket.remote_address}')


async def broadcast(data: dict):
    if not connected_clients:
        return
    msg = json.dumps(data)
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send(msg)
        except websockets.exceptions.ConnectionClosed:
            dead.add(ws)
    connected_clients.difference_update(dead)


# ── HTTP ─────────────────────────────────────────────────────
async def http_handler(request):
    return web.FileResponse(DASH_PATH)


async def run_http():
    app = web.Application()
    app.router.add_get('/', http_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, WS_HOST, HTTP_PORT).start()
    print(f'[HTTP] Listening on {HTTP_PORT}')
    await asyncio.Future()


async def run_ws():
    async with websockets.serve(ws_handler, WS_HOST, WS_PORT):
        print(f'[WS]  Listening on {WS_PORT}')
        await asyncio.Future()


# ── CAN ──────────────────────────────────────────────────────
def _drain_can(bus, timeout):
    """블로킹 수신 + 대기 프레임 일괄 비우기 + 스레드 내 파싱 처리"""
    rows, frames = [], []
    m = bus.recv(timeout)
    while m is not None:
        t_mono = time.clock_gettime(time.CLOCK_MONOTONIC)
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        hx = m.data.hex()
        
        # 0x501 (1281) MSGID_0X501: Pack_Summed_Voltage = data[0] * 0.01 (V)
        pack_voltage = ""
        v_val = None
        if m.arbitration_id == 0x501 and len(m.data) >= 1:
            v_val = round(m.data[0] * 0.01, 2)
            pack_voltage = v_val

        rows.append([ts, f'{t_mono:.6f}', hex(m.arbitration_id), hx, pack_voltage])
        frames.append([ts, m.arbitration_id, hx, v_val])
        m = bus.recv(0)          # 논블로킹 배출
    return rows, frames


async def run_can(csv_writer):
    bus  = can.interface.Bus(channel='can0', interface='socketcan')
    loop = asyncio.get_event_loop()
    print('[CAN] Starting receive...')
    try:
        while True:
            rows, frames = await loop.run_in_executor(
                None, _drain_can, bus, 1.0 / CAN_BATCH_HZ)
            if not frames:
                continue
            await loop.run_in_executor(None, csv_writer.writerows, rows)
            await broadcast({'type': 'can', 'frames': frames})
    finally:
        bus.shutdown()


# ── IMU / GPS ────────────────────────────────────────────────
REC_HDR = struct.Struct('<ddI')
WANT = {0x50, 0x51, 0x52, 0x53, 0x57, 0x58, 0x5A}


def _read_and_store(ser, raw_file):
    data = ser.read(512)
    if not data:
        return None, None, b''
    t_mono = time.clock_gettime(time.CLOCK_MONOTONIC)
    t_real = time.clock_gettime(time.CLOCK_REALTIME)
    raw_file.write(REC_HDR.pack(t_mono, t_real, len(data)))
    raw_file.write(data)
    return t_mono, t_real, data


async def run_imu(raw_file):
    loop = asyncio.get_event_loop()
    try:
        ser = serial.Serial(IMU_PORT, IMU_BAUD, timeout=0.2)
    except serial.SerialException as e:
        print(f'[IMU] 포트 열기 실패: {e}  - IMU 없이 계속 진행')
        return

    st = Stream()
    cur = {}
    burst = []
    gps_out = None
    last_loc = None
    last_bcast = 0.0
    last_stat = time.time()
    print(f'[IMU] {IMU_PORT} @ {IMU_BAUD} 수신 시작')
    try:
        while True:
            t_mono, t_real, data = await loop.run_in_executor(
                None, _read_and_store, ser, raw_file)
            if not data:
                continue

            for pkt in st.feed(data):
                if pkt[1] not in WANT:
                    continue
                r = parse_packet(pkt)
                if not r:
                    continue
                cur[r['type']] = r

                if r['type'] == 'angle':
                    a = cur.get('accel')
                    g = cur.get('gyro')
                    if a and g:
                        burst.append([round(a['ax'], 4), round(a['ay'], 4), round(a['az'], 4),
                                      round(g['wx'], 3), round(g['wy'], 3), round(g['wz'], 3)])
                        if len(burst) > 200:
                            del burst[:-200]

                if r['type'] == 'location':
                    key = (r['lat_raw'], r['lon_raw'])
                    if key != last_loc:
                        last_loc = key
                        s = cur.get('groundspd', {})
                        gps_out = {'lat': r['lat'], 'lon': r['lon'],
                                   'mono': round(t_mono, 4),
                                   'speed': s.get('gps_speed'),
                                   'course': s.get('gps_course'),
                                   'alt': s.get('gps_alt')}

            now = time.time()
            if now - last_bcast >= 1.0 / IMU_BCAST_HZ and cur:
                last_bcast = now
                a = cur.get('accel', {})
                g = cur.get('gyro', {})
                n = cur.get('angle', {})
                q = cur.get('posacc', {})
                t = cur.get('time', {})
                utc = None
                if t:
                    utc = (f"{t['yy']:04d}-{t['mo']:02d}-{t['dd']:02d} "
                           f"{t['hh']:02d}:{t['mi']:02d}:{t['ss']:02d}.{t['ms']:03d}")
                msg = {
                    'type': 'imu', 'mono': round(t_mono, 4), 'utc': utc,
                    'burst': burst,
                    'temp': a.get('temp'),
                    'roll': n.get('roll'), 'pitch': n.get('pitch'), 'yaw': n.get('yaw'),
                    'sats': q.get('sats'), 'hdop': q.get('hdop'),
                }
                if gps_out:
                    msg['gps'] = gps_out
                    gps_out = None
                burst = []
                await broadcast(msg)

            if now - last_stat >= 60.0:
                last_stat = now
                print(f'[IMU] pkt {st.n_ok}  체크섬오류 {st.n_bad}  '
                      f'동기이탈 {st.n_desync}  (초기동기 {st.n_init}B)')
                await loop.run_in_executor(None, raw_file.flush)
    finally:
        ser.close()
        raw_file.flush()


# ── Main ─────────────────────────────────────────────────────
async def main():
    stamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(LOG_DIR, f'can_{stamp}.csv')
    imu_path = os.path.join(LOG_DIR, f'imu_{stamp}.bin')
    print(f'[INFO] CAN log : {csv_path}')
    print(f'[INFO] IMU log : {imu_path}')

    with open(csv_path, 'w', newline='') as cf, open(imu_path, 'wb', buffering=1 << 18) as rf:
        w = csv.writer(cf)
        # CSV 헤더에 pack_summed_voltage 추가
        w.writerow(['timestamp', 'mono', 'can_id', 'data_hex', 'pack_summed_voltage'])
        await asyncio.gather(
            run_http(),
            run_ws(),
            run_can(w),
            run_imu(rf),
        )


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n[INFO] 종료')