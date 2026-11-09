#!/usr/bin/env python3
"""
TEAM KAI - WTGAHRS3 (WitMotion) 프로토콜 파서

패킷 구조: 0x55 | TYPE | DATA[8] | CHECKSUM
체크섬 = 앞 10바이트 합의 하위 8비트

주의: 스케일/배치는 WitMotion 표준 프로토콜 기준.
      매뉴얼 4 Communication Protocol 로 대조 확인할 것.
"""
import struct

HEADER = 0x55

# 패킷 타입
T_TIME     = 0x50
T_ACCEL    = 0x51
T_GYRO     = 0x52
T_ANGLE    = 0x53
T_MAG      = 0x54
T_PORT     = 0x55   # 헤더와 값이 같음 - 파싱 주의
T_PRESS    = 0x56
T_LOCATION = 0x57
T_GROUNDSP = 0x58
T_QUAT     = 0x59
T_POSACC   = 0x5A

NAMES = {
    T_TIME:'Time', T_ACCEL:'Accel', T_GYRO:'Gyro', T_ANGLE:'Angle',
    T_MAG:'Mag', T_PORT:'Port', T_PRESS:'Press', T_LOCATION:'Location',
    T_GROUNDSP:'GroundSpd', T_QUAT:'Quat', T_POSACC:'PosAcc',
}

G   = 16.0      # 가속도 풀스케일 (g)
DPS = 2000.0    # 각속도 풀스케일 (deg/s)
DEG = 180.0     # 각도 풀스케일 (deg)


def _i16(b, o):
    return struct.unpack_from('<h', b, o)[0]

def _u16(b, o):
    return struct.unpack_from('<H', b, o)[0]

def _i32(b, o):
    return struct.unpack_from('<i', b, o)[0]


def ddmm_to_deg(raw):
    """
    WitMotion 위경도 인코딩: dd * 10^7 + mm.mmmmm * 10^5
    예) 12704.77258' -> 127*10^7 + 477258 = 1270477258
    """
    neg = raw < 0
    raw = abs(raw)
    dd = raw // 10000000
    mm = (raw % 10000000) / 100000.0
    deg = dd + mm / 60.0
    return -deg if neg else deg


def parse_packet(p):
    """11바이트 패킷 1개를 dict로. 체크섬 불일치면 None."""
    if len(p) != 11 or p[0] != HEADER:
        return None
    if (sum(p[:10]) & 0xFF) != p[10]:
        return None

    t = p[1]
    d = p  # 데이터는 오프셋 2부터

    if t == T_ACCEL:
        return {'type':'accel',
                'ax': _i16(d,2)/32768.0*G,
                'ay': _i16(d,4)/32768.0*G,
                'az': _i16(d,6)/32768.0*G,
                'temp': _i16(d,8)/100.0}

    if t == T_GYRO:
        return {'type':'gyro',
                'wx': _i16(d,2)/32768.0*DPS,
                'wy': _i16(d,4)/32768.0*DPS,
                'wz': _i16(d,6)/32768.0*DPS,
                'temp': _i16(d,8)/100.0}

    if t == T_ANGLE:
        return {'type':'angle',
                'roll':  _i16(d,2)/32768.0*DEG,
                'pitch': _i16(d,4)/32768.0*DEG,
                'yaw':   _i16(d,6)/32768.0*DEG,
                'ver':   _i16(d,8)}

    if t == T_QUAT:
        return {'type':'quat',
                'q0': _i16(d,2)/32768.0,
                'q1': _i16(d,4)/32768.0,
                'q2': _i16(d,6)/32768.0,
                'q3': _i16(d,8)/32768.0}

    if t == T_TIME:
        ms = _u16(d,8)
        return {'type':'time',
                'yy': 2000+d[2], 'mo': d[3], 'dd': d[4],
                'hh': d[5], 'mi': d[6], 'ss': d[7], 'ms': ms}

    if t == T_LOCATION:
        return {'type':'location',
                'lon': ddmm_to_deg(_i32(d,2)),
                'lat': ddmm_to_deg(_i32(d,6)),
                'lon_raw': _i32(d,2), 'lat_raw': _i32(d,6)}

    if t == T_GROUNDSP:
        # gps_course: uint16 / 100.  실측 검증 - raw 24122 -> 241.22 deg (PC 표시와 일치).
        # int16 /10 이면 1399 deg 같은 불가능한 값이 나옴. 360deg 를 담으려면
        # 36000 이 필요하므로 int16(max 32767) 불가 -> uint16 이어야 함.
        return {'type':'groundspd',
                'gps_alt': _i16(d,2)/10.0,        # m   (기지 고도로 재검증 권장)
                'gps_course': _u16(d,4)/100.0,    # deg
                'gps_speed': _i32(d,6)/1000.0}    # km/h

    if t == T_POSACC:
        return {'type':'posacc',
                'sats': _u16(d,2),
                'pdop': _u16(d,4)/100.0,
                'hdop': _u16(d,6)/100.0,
                'vdop': _u16(d,8)/100.0}

    return {'type':'other', 'raw_type': t}


class Stream:
    """
    바이트 스트림 -> 패킷. 동기 이탈 시 재동기.

    포트를 열면 FIFO 중간 바이트부터 읽기 시작하므로, 첫 정상 패킷이
    나올 때까지 최대 10바이트 정도가 버려진다. 이건 정상이므로
    '초기 동기'와 '운용 중 오류'를 분리해서 센다.
    운용 중 오류(n_bad / n_desync)만 0 이어야 한다.
    """
    def __init__(self):
        self.buf = bytearray()
        self.n_ok = 0
        self.n_bad = 0        # 동기 확립 후 체크섬 오류  <- 0 이어야 함
        self.n_desync = 0     # 동기 확립 후 동기 이탈    <- 0 이어야 함
        self.n_init = 0       # 동기 확립 전 폐기 바이트 (정상)
        self.synced = False

    def _drop(self, kind):
        self.buf.pop(0)
        if not self.synced:
            self.n_init += 1
        elif kind == 'bad':
            self.n_bad += 1
        else:
            self.n_desync += 1

    def feed(self, data):
        """새 바이트를 넣고 완성된 패킷 리스트를 반환."""
        self.buf += data
        out = []
        while len(self.buf) >= 11:
            if self.buf[0] != HEADER:
                self._drop('desync')
                continue
            pkt = bytes(self.buf[:11])
            if (sum(pkt[:10]) & 0xFF) == pkt[10]:
                del self.buf[:11]
                self.n_ok += 1
                self.synced = True
                out.append(pkt)
            else:
                self._drop('bad')
        return out


if __name__ == '__main__':
    # ── 자체 검증: PC 소프트웨어 화면값과 대조 ──
    print("── ddmm_to_deg 검증 ──")
    for raw, expect in [(1270477258, 127.079543), (373249885, 37.541648)]:
        got = ddmm_to_deg(raw)
        print(f"  raw={raw:>12d} -> {got:.6f}  (기대 {expect:.6f}, 차 {abs(got-expect)*111320:.2f} m)")

    print("\n── 패킷 조립/파싱 왕복 검증 ──")
    def mk(t, payload):
        p = bytes([HEADER, t]) + payload
        return p + bytes([sum(p) & 0xFF])

    # 가속도: 정지 상태 (0, 0, 1g), 온도 29.85
    az = int(1.0024/G*32768)
    pkt = mk(T_ACCEL, struct.pack('<hhhh', 8, 13, az, 2985))
    print("  accel ->", {k:(round(v,4) if isinstance(v,float) else v)
                          for k,v in parse_packet(pkt).items()})

    # 위치
    pkt = mk(T_LOCATION, struct.pack('<ii', 1270477258, 373251076))
    r = parse_packet(pkt)
    print(f"  location -> lat {r['lat']:.6f}, lon {r['lon']:.6f}")

    # 측위정확도: 위성 29, HDOP 0.90
    pkt = mk(T_POSACC, struct.pack('<HHHH', 29, 0, 90, 130))
    print("  posacc ->", parse_packet(pkt))

    # 체크섬 오류 검출
    bad = bytearray(pkt); bad[10] ^= 0xFF
    print("  체크섬 오류 패킷 ->", parse_packet(bytes(bad)))

    # GPS 코스각 - uint16/100
    pkt = mk(T_GROUNDSP, struct.pack('<hHi', 290, 24122, 148))
    r = parse_packet(pkt)
    print(f"  groundspd -> 고도 {r['gps_alt']:.1f} m, 코스 {r['gps_course']:.2f} deg "
          f"(PC 241.220 기대), 속도 {r['gps_speed']:.3f} km/h")
    assert abs(r['gps_course'] - 241.22) < 0.01, "코스각 스케일 오류"

    print("\n── Stream 동기 분리 검증 ──")
    s = Stream()
    good = mk(T_ANGLE, struct.pack('<hhhh', -94, 17, 10450, 0))
    # 앞쪽 쓰레기 = 초기 동기(정상), 뒤쪽 가짜헤더 = 운용 중 오류
    stream = b'\x12\x34' + good + b'\x55\x99' + good
    pkts = s.feed(stream)
    print(f"  입력 {len(stream)}B -> 정상 {s.n_ok}")
    print(f"    초기 동기 폐기 {s.n_init} B   (정상, 판정 제외)")
    print(f"    운용 중 체크섬오류 {s.n_bad}, 동기이탈 {s.n_desync}   (0 이어야 함)")
    for p in pkts:
        print(f"    yaw={parse_packet(p)['yaw']:.3f}")
