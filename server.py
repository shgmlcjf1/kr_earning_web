#!/usr/bin/env python3
"""
K-어닝 서프라이즈 레이더 웹 서버 & DART + 네이버증권 실시간 재무/컨센서스 연동기
별도 패키지 설치 없이 Python 기본 라이브러리만으로 구동됩니다.
"""
import argparse
import concurrent.futures
import html
import json
import os
import re
import ssl
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_FILE = os.path.join(BASE_DIR, "dart_key.txt")

# =====================================================================
# [여기에 OPENDART 40자리 API 인증키를 넣거나, 실행 시 화면에 붙여넣으세요!]
# =====================================================================
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

SSL_CTX = ssl._create_unverified_context()
FINANCE_CACHE = {}

OCTOBER_3Q26_BASELINE = json.loads(r'''[{"id": "028050", "code": "028050", "name": "삼성E&A", "market": "KOSPI", "sector": "건설/플랜트", "announceDate": "2026-10-22", "announceTime": "08:20", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 55400, "afterHoursChange": 16.14, "rev": {"actual": 29105, "consensus": 27100, "prevY": 19956, "prevQ": 26093}, "op": {"actual": 5006, "consensus": 2526, "prevY": 1765, "prevQ": 2731}, "net": {"actual": 2167, "consensus": 1850, "prevY": 1574, "prevQ": 1825}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [19956, 24100, 22674, 26093, 29105], "op": [1765, 1980, 1882, 2731, 5006]}, "comment": "[10/22 DART 잠정실적] 3Q26 영업이익 5,006억원(YoY +183.6%, QoQ +83.3%)으로 시장 컨센서스(2,526억원)를 +98.2% 대폭 상회"}, {"id": "010120", "code": "010120", "name": "LS ELECTRIC", "market": "KOSPI", "sector": "전력기기/에너지", "announceDate": "2026-10-21", "announceTime": "11:32", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 163900, "afterHoursChange": 5.61, "rev": {"actual": 18778, "consensus": 17200, "prevY": 12163, "prevQ": 15770}, "op": {"actual": 2335, "consensus": 1980, "prevY": 1008, "prevQ": 1785}, "net": {"actual": 1428, "consensus": 1250, "prevY": 659, "prevQ": 1158}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [12163, 13100, 13765, 15770, 18778], "op": [1008, 1120, 1266, 1785, 2335]}, "comment": "[10/21 DART 잠정실적] 북미 AI 데이터센터 배전반·초고압변압기 수주 호조로 영업이익 YoY +131.7%, QoQ +30.8%, 컨센 +17.9% 상회"}, {"id": "114810", "code": "114810", "name": "한솔아이원스", "market": "KOSDAQ", "sector": "반도체/장비", "announceDate": "2026-10-23", "announceTime": "11:30", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 11080, "afterHoursChange": -7.12, "rev": {"actual": 650, "consensus": 580, "prevY": 512, "prevQ": 537}, "op": {"actual": 124, "consensus": 105, "prevY": 94, "prevQ": 66}, "net": {"actual": 97, "consensus": 82, "prevY": 64, "prevQ": 52}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [512, 485, 492, 537, 650], "op": [94, 72, 71, 66, 124]}, "comment": "[10/23 DART 잠정실적] 반도체 정밀가공·세정장비 부품 출하 확대로 매출 YoY +26.9%, 영업이익 YoY +32.0%·QoQ +88.7%, 순이익 YoY +50.6% 기록"}, {"id": "294870", "code": "294870", "name": "HDC현대산업개발", "market": "KOSPI", "sector": "건설/플랜트", "announceDate": "2026-10-22", "announceTime": "11:40", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 21300, "afterHoursChange": 2.4, "rev": {"actual": 10821, "consensus": 10200, "prevY": 10530, "prevQ": 9146}, "op": {"actual": 1658, "consensus": 1350, "prevY": 730, "prevQ": 1227}, "net": {"actual": 1113, "consensus": 910, "prevY": 398, "prevQ": 837}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [10530, 10150, 6739, 9146, 10821], "op": [730, 780, 801, 1227, 1658]}, "comment": "[10/22 DART 잠정실적] 자체 주택사업 원가율 개선으로 영업이익 1,658억원(YoY +127.1%, QoQ +35.1%), 순이익 1,113억원(YoY +179.2%) 달성"}, {"id": "003850", "code": "003850", "name": "보령", "market": "KOSPI", "sector": "바이오/의료기기", "announceDate": "2026-10-22", "announceTime": "15:40", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 11850, "afterHoursChange": 3.85, "rev": {"actual": 3144, "consensus": 3010, "prevY": 2800, "prevQ": 2944}, "op": {"actual": 295, "consensus": 265, "prevY": 220, "prevQ": 250}, "net": {"actual": 218, "consensus": 195, "prevY": 180, "prevQ": 190}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [2800, 2860, 2890, 2944, 3144], "op": [220, 230, 238, 250, 295]}, "comment": "[10/22 DART 잠정실적] 항암제(탁소텔 등) 및 만성질환 전문의약품 매출 호조로 3분기 누적 영업이익이 전년도 연간 실적을 조기 돌파"}, {"id": "300120", "code": "300120", "name": "라온피플", "market": "KOSDAQ", "sector": "IT플랫폼/엔터", "announceDate": "2026-10-21", "announceTime": "13:30", "reportName": "영업(잠정)실적(공정공시)", "price": 3355, "afterHoursChange": 11.09, "rev": {"actual": 38, "consensus": 33, "prevY": 24, "prevQ": 32}, "op": {"actual": 12, "consensus": 5, "prevY": -26, "prevQ": -5}, "net": {"actual": 1, "consensus": 0.5, "prevY": -47, "prevQ": -8}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [24, 28, 44, 32, 38], "op": [-26, -18, -5, -5, 12]}, "comment": "[10/21 DART 잠정실적] AI 비전 검사 솔루션 수주 확대로 매출액 YoY +58.6% 증가 및 영업이익·순이익 전년동기/전분기 대비 동반 흑자전환"}, {"id": "003490", "code": "003490", "name": "대한항공", "market": "KOSPI", "sector": "항공/운송", "announceDate": "2026-10-15", "announceTime": "15:45", "reportName": "영업(잠정)실적(공정공시)", "price": 24850, "afterHoursChange": 2.15, "rev": {"actual": 50448, "consensus": 48900, "prevY": 40102, "prevQ": 45360}, "op": {"actual": 4212, "consensus": 4050, "prevY": 3764, "prevQ": 3882}, "net": {"actual": 3150, "consensus": 2980, "prevY": 2810, "prevQ": 2890}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [40102, 41800, 43200, 45360, 50448], "op": [3764, 3450, 3680, 3882, 4212]}, "comment": "[10/15 DART 잠정실적] 하계 성수기 여객 수요 및 전자상거래 화물 운임 강세로 매출 YoY +25.8%, 영업이익 YoY +11.9% 기록하며 컨센 상회"}, {"id": "086790", "code": "086790", "name": "하나금융지주", "market": "KOSPI", "sector": "금융/증권", "announceDate": "2026-10-23", "announceTime": "15:35", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 64200, "afterHoursChange": 1.9, "rev": {"actual": 124500, "consensus": 121000, "prevY": 112000, "prevQ": 118500}, "op": {"actual": 16850, "consensus": 15900, "prevY": 15150, "prevQ": 15540}, "net": {"actual": 12609, "consensus": 11800, "prevY": 11360, "prevQ": 11520}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [112000, 114500, 116000, 118500, 124500], "op": [15150, 13200, 16400, 15540, 16850]}, "comment": "[10/23 DART 잠정실적] 3분기 당기순이익 1조 2,609억원(YoY +11.0%), 3분기 누적 순이익 3조 6,977억원으로 시장 컨센서스 상회"}, {"id": "005930", "code": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "반도체/장비", "announceDate": "2026-10-08", "announceTime": "08:30", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 118500, "afterHoursChange": 1.25, "rev": {"actual": 1950000, "consensus": 2008000, "prevY": 860600, "prevQ": 1715000}, "op": {"actual": 1074000, "consensus": 1069000, "prevY": 121700, "prevQ": 894900}, "net": {"actual": 845000, "consensus": 832000, "prevY": 101000, "prevQ": 712000}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [860600, 1085000, 1338700, 1715000, 1950000], "op": [121700, 285000, 572400, 894900, 1074000]}, "comment": "[10/08 DART 잠정실적] 영업이익 107.4조원(YoY +782.5%, QoQ +20.0%)으로 컨센서스(106.9조원) 상회, 매출액(195.0조원)은 컨센(200.8조원) 소폭 하회"}, {"id": "064850", "code": "064850", "name": "에프앤가이드", "market": "KOSDAQ", "sector": "금융/증권", "announceDate": "2026-10-21", "announceTime": "16:10", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 12450, "afterHoursChange": 3.1, "rev": {"actual": 148, "consensus": 140, "prevY": 84, "prevQ": 149}, "op": {"actual": 77, "consensus": 70, "prevY": 27, "prevQ": 79}, "net": {"actual": 63, "consensus": 57, "prevY": 26, "prevQ": 62}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [84, 98, 115, 149, 148], "op": [27, 35, 47, 79, 77]}, "comment": "[10/21 DART 잠정실적] ETF 인덱스 및 금융정보 매출 급증으로 영업이익 YoY +187.9% 급증(OPM 52.1%)했으나 전분기 대비(QoQ -1.7%) 소폭 둔화"}, {"id": "229640", "code": "229640", "name": "LS에코에너지", "market": "KOSPI", "sector": "전력기기/에너지", "announceDate": "2026-10-20", "announceTime": "09:40", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 51600, "afterHoursChange": -2.1, "rev": {"actual": 3515, "consensus": 3350, "prevY": 2328, "prevQ": 3503}, "op": {"actual": 222, "consensus": 210, "prevY": 148, "prevQ": 253}, "net": {"actual": 140, "consensus": 135, "prevY": 112, "prevQ": 191}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [2328, 2540, 2855, 3503, 3515], "op": [148, 165, 201, 253, 222]}, "comment": "[10/20 DART 잠정실적] 초고압·랜선 수출 확대로 매출 YoY +51.0%, 영업이익 YoY +50.2% 성장했으나 전분기 대비 영업이익 QoQ -12.2% 감소"}, {"id": "373220", "code": "373220", "name": "LG에너지솔루션", "market": "KOSPI", "sector": "2차전지/소재", "announceDate": "2026-10-16", "announceTime": "09:00", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 412000, "afterHoursChange": 4.2, "rev": {"actual": 68778, "consensus": 67500, "prevY": 82235, "prevQ": 61619}, "op": {"actual": 7560, "consensus": 3517, "prevY": 7312, "prevQ": 1953}, "net": {"actual": 5210, "consensus": 2650, "prevY": 4205, "prevQ": 1120}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [82235, 80010, 61287, 61619, 68778], "op": [7312, 3382, 1573, 1953, 7560]}, "comment": "[10/16 DART 잠정실적] 영업이익 7,560억원으로 컨센서스(3,517억)를 +115.0% 상회했으나, 매출액 전년동기 대비(YoY -16.4%) 역성장으로 YoY 기준 미달"}, {"id": "064350", "code": "064350", "name": "현대로템", "market": "KOSPI", "sector": "방산/조선", "announceDate": "2026-10-19", "announceTime": "15:40", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 101200, "afterHoursChange": -3.4, "rev": {"actual": 16207, "consensus": 18008, "prevY": 16190, "prevQ": 16060}, "op": {"actual": 2471, "consensus": 2761, "prevY": 2777, "prevQ": 2324}, "net": {"actual": 1780, "consensus": 2050, "prevY": 1984, "prevQ": 1710}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [16190, 17200, 15400, 16060, 16207], "op": [2777, 2650, 2180, 2324, 2471]}, "comment": "[10/19 DART 잠정실적] K2전차 상반기 선생산에 따른 3분기 인도분 이연으로 매출액(-10.0%)과 영업이익(-10.5%) 모두 컨센서스 하회"}, {"id": "001500", "code": "001500", "name": "현대차증권", "market": "KOSPI", "sector": "금융/증권", "announceDate": "2026-10-22", "announceTime": "16:02", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 6770, "afterHoursChange": -1.31, "rev": {"actual": 7203, "consensus": 5200, "prevY": 2440, "prevQ": 6120}, "op": {"actual": 127, "consensus": 145, "prevY": 141, "prevQ": 152}, "net": {"actual": 154, "consensus": 130, "prevY": 122, "prevQ": 118}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [2440, 4100, 5800, 6120, 7203], "op": [141, 110, 160, 152, 127]}, "comment": "[10/22 DART 잠정실적] 매출액(7,203억, YoY +195.2%)과 당기순이익(154억, YoY +25.7%)은 증가했으나 영업이익(127억, YoY -10.0%) 감소로 기준 미달"}, {"id": "066570", "code": "066570", "name": "LG전자", "market": "KOSPI", "sector": "가전/IT하드웨어", "announceDate": "2026-10-18", "announceTime": "15:35", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 216000, "afterHoursChange": -1.8, "rev": {"actual": 232000, "consensus": 242000, "prevY": 216400, "prevQ": 216900}, "op": {"actual": 7510, "consensus": 9880, "prevY": 6890, "prevQ": 11960}, "net": {"actual": 4850, "consensus": 6400, "prevY": 4420, "prevQ": 6290}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [216400, 231000, 210900, 216900, 232000], "op": [6890, 3130, 13350, 11960, 7510]}, "comment": "[10/18 DART 잠정실적] 물류비 상승 및 희망퇴직 일회성 비용 반영으로 영업이익(7,510억)이 시장 컨센서스를 24% 하회"}, {"id": "034220", "code": "034220", "name": "LG디스플레이", "market": "KOSPI", "sector": "가전/IT하드웨어", "announceDate": "2026-10-23", "announceTime": "08:25", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 10450, "afterHoursChange": -2.5, "rev": {"actual": 68210, "consensus": 71500, "prevY": 69570, "prevQ": 67080}, "op": {"actual": 2506, "consensus": 3450, "prevY": 4313, "prevQ": -937}, "net": {"actual": 1120, "consensus": 1950, "prevY": 2450, "prevQ": -4700}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [69570, 74000, 52530, 67080, 68210], "op": [4313, 1317, -4694, -937, 2506]}, "comment": "[10/23 DART 잠정실적] 전분기 대비 1분기 만에 흑자전환(영업이익 2,506억)했으나 전년동기 대비 41.9% 감소하며 컨센서스 하회"}, {"id": "014830", "code": "014830", "name": "유니드", "market": "KOSPI", "sector": "화학/소재", "announceDate": "2026-10-21", "announceTime": "14:10", "reportName": "연결재무제표기준영업(잠정)실적(공정공시)", "price": 55600, "afterHoursChange": -5.92, "rev": {"actual": 3780, "consensus": 3650, "prevY": 3290, "prevQ": 3920}, "op": {"actual": -21, "consensus": 216, "prevY": 188, "prevQ": 243}, "net": {"actual": -44, "consensus": 165, "prevY": 168, "prevQ": 195}, "history": {"quarters": ["3Q25", "4Q25", "1Q26", "2Q26", "3Q26(잠정)"], "rev": [3290, 3150, 3923, 3920, 3780], "op": [188, 155, 254, 243, -21]}, "comment": "[10/21 DART 잠정실적] 해상 운임 급증 및 일부 화학제품 수요 둔화로 영업손실 21억원(적자전환)을 기록하며 컨센서스(216억원) 대폭 하회"}]''')



def load_or_prompt_api_key(cli_key: str = "") -> str:
    global DART_API_KEY
    if cli_key.strip():
        DART_API_KEY = cli_key.strip()
    if not DART_API_KEY and os.path.exists(KEY_FILE):
        try:
            DART_API_KEY = open(KEY_FILE, "r", encoding="utf-8").read().strip()
        except Exception:
            pass
    if not DART_API_KEY:
        print("=" * 64)
        print(" [초기 설정] OPENDART 40자리 API 인증키가 아직 등록되지 않았습니다.")
        print(" (한 번만 입력하면 dart_key.txt에 자동 저장되어 다음부터 묻지 않습니다)")
        print("=" * 64)
        try:
            entered = input(" 👉 여기에 40자리 API 인증키를 붙여넣고 엔터(Enter)를 누르세요: ").strip()
            if entered:
                DART_API_KEY = entered
                try:
                    open(KEY_FILE, "w", encoding="utf-8").write(DART_API_KEY)
                    print(" ✓ 인증키가 dart_key.txt 파일에 자동 저장되었습니다!")
                except Exception:
                    pass
        except EOFError:
            pass
    return DART_API_KEY


def http_get_bytes(url: str, timeout: int = 10) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
        },
    )
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
        return resp.read()


def call_dart_api(url: str) -> dict:
    raw = http_get_bytes(url, timeout=12)
    return json.loads(raw.decode("utf-8"))


def clean_num(text: str):
    if not text:
        return None
    t = re.sub(r"<[^>]+>", "", text)
    t = html.unescape(t).replace(",", "").replace(" ", "").strip()
    if not t or t in ("-", "N/A", "완전잠식", "적자지속", "적자전환", "흑자전환"):
        return None
    m = re.search(r"[-+]?\d+(?:\.\d+)?", t)
    return float(m.group(0)) if m else None


def fetch_naver_quarterly_and_consensus(stock_code: str) -> dict:
    """
    네이버페이 증권(finance.naver.com) 종목분석 페이지에서 실제 현재가, 등락률, 업종명,
    최근 5개 분기 실적(매출액, 영업이익, 당기순이익) 및 증권사 컨센서스(E)를 추출합니다.
    """
    if stock_code in FINANCE_CACHE:
        return FINANCE_CACHE[stock_code]

    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    try:
        raw = http_get_bytes(url, timeout=8)
        try:
            page = raw.decode("utf-8")
        except UnicodeDecodeError:
            page = raw.decode("cp949", errors="ignore")

        price = 0
        m_price = re.search(r'<p class="no_today">.*?<span class="blind">([0-9,]+)</span>', page, re.S)
        if m_price:
            price = int(m_price.group(1).replace(",", ""))

        chg_pct = 0.0
        m_exday = re.search(
            r'<p class="no_exday">.*?(ico_up|ico_down|보합).*?<span class="blind">([0-9.]+)</span>\s*<span class="tah[^>]*">%</span>',
            page,
            re.S,
        )
        if m_exday:
            sign = -1.0 if "down" in m_exday.group(1) else 1.0
            chg_pct = round(sign * float(m_exday.group(2)), 2)

        sector = "상장기업"
        m_sec = re.search(r'<h4 class="h_sub sub_tit7">.*?<a[^>]*>([^<]+)</a>', page, re.S)
        if m_sec:
            sector = m_sec.group(1).strip()

        quarters = ["전년동기", "3분기전", "2분기전", "전분기", "이번분기"]
        rev_q, op_q, net_q, q_headers_is_est = [], [], [], []

        m_cop = re.search(r'<div class="section cop_analysis">.*?<table[^>]*>(.*?)</table>', page, re.S)
        if m_cop:
            cop_html = m_cop.group(1)
            thead_m = re.search(r"<thead>(.*?)</thead>", cop_html, re.S)
            if thead_m:
                ths = re.findall(r"<th[^>]*>(.*?)</th>", thead_m.group(1), re.S)
                date_ths = []
                for th in ths:
                    txt = re.sub(r"<[^>]+>", "", th).strip()
                    if re.search(r"\d{4}\.\d{2}", txt):
                        date_ths.append(txt)
                if len(date_ths) >= 6:
                    q_dates = date_ths[4:10]
                    quarters = [re.sub(r"\s+", "", d) for d in q_dates]
                    q_headers_is_est = [("(E)" in d) for d in quarters]

            tbody_m = re.search(r"<tbody>(.*?)</tbody>", cop_html, re.S)
            if tbody_m:
                trs = re.findall(r"<tr[^>]*>(.*?)</tr>", tbody_m.group(1), re.S)
                for tr in trs:
                    th_m = re.search(r"<th[^>]*>(.*?)</th>", tr, re.S)
                    if not th_m:
                        continue
                    row_title = re.sub(r"<[^>]+>", "", th_m.group(1)).strip()
                    tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
                    if len(tds) < 10:
                        continue
                    q_cells = [clean_num(td) for td in tds[4:10]]
                    if "매출액" in row_title and not rev_q:
                        rev_q = q_cells
                    elif "영업이익" in row_title and "률" not in row_title and not op_q:
                        op_q = q_cells
                    elif "당기순이익" in row_title and not net_q:
                        net_q = q_cells

        def build_metric_dict(q_vals):
            valid_idx = [i for i, v in enumerate(q_vals) if v is not None]
            if not valid_idx:
                return {"actual": 0, "consensus": 0, "prevY": 0, "prevQ": 0, "series": [0, 0, 0, 0, 0], "has_consensus": False}

            actual_indices = [i for i in valid_idx if not (i < len(q_headers_is_est) and q_headers_is_est[i])]
            est_indices = [i for i in valid_idx if (i < len(q_headers_is_est) and q_headers_is_est[i])]

            act_i = actual_indices[-1] if actual_indices else valid_idx[-1]
            actual_val = q_vals[act_i] or 0
            prev_q_val = q_vals[act_i - 1] if (act_i - 1 >= 0 and q_vals[act_i - 1] is not None) else actual_val
            prev_y_val = q_vals[0] if (q_vals[0] is not None) else prev_q_val

            if est_indices:
                cons_val = q_vals[est_indices[0]]
                has_cons = True
            else:
                cons_val = round((prev_q_val + prev_y_val) / 2, 1) if (prev_q_val or prev_y_val) else actual_val
                has_cons = False

            series = [v if v is not None else 0 for v in q_vals[:5]]
            while len(series) < 5:
                series.append(actual_val)

            return {
                "actual": actual_val,
                "consensus": cons_val,
                "prevY": prev_y_val,
                "prevQ": prev_q_val,
                "series": series[:5],
                "has_consensus": has_cons,
            }

        res = {
            "price": price,
            "afterHoursChange": chg_pct,
            "sector": sector,
            "quarters": quarters[:5] if len(quarters) >= 5 else ["1Q", "2Q", "3Q", "4Q", "최근분기"],
            "rev": build_metric_dict(rev_q),
            "op": build_metric_dict(op_q),
            "net": build_metric_dict(net_q),
        }
        FINANCE_CACHE[stock_code] = res
        return res
    except Exception as e:
        return {
            "price": 0,
            "afterHoursChange": 0.0,
            "sector": "상장기업",
            "quarters": ["1Q", "2Q", "3Q", "4Q", "최근분기"],
            "rev": {"actual": 0, "consensus": 0, "prevY": 0, "prevQ": 0, "series": [0, 0, 0, 0, 0], "has_consensus": False},
            "op": {"actual": 0, "consensus": 0, "prevY": 0, "prevQ": 0, "series": [0, 0, 0, 0, 0], "has_consensus": False},
            "net": {"actual": 0, "consensus": 0, "prevY": 0, "prevQ": 0, "series": [0, 0, 0, 0, 0], "has_consensus": False},
            "error": str(e),
        }


def build_real_earnings_dataset(api_key: str, bgn_de: str = "") -> dict:
    clean_key = (api_key or DART_API_KEY).strip()
    if not clean_key:
        return {
            "status": "NO_KEY",
            "message": "server.py 실행 창 또는 웹 화면 입력창에 OPENDART 40자리 인증키를 입력해주세요.",
            "items": [],
        }

    # Save key if newly provided via UI
    if api_key and api_key.strip():
        try:
            open(KEY_FILE, "w", encoding="utf-8").write(api_key.strip())
        except Exception:
            pass

    if not bgn_de:
        bgn_de = "20261001"

    try:
        raw_disclosures = []
        # 거래소 공정공시(I) 중 영업(잠정)실적/매출액손익구조변경 및 정기보고서(A: 분기/반기/사업보고서)를 다중 페이지로 스캔
        for p_type, max_p in (("I", 5), ("A", 3)):
            for page_no in range(1, max_p + 1):
                params = {"crtfc_key": clean_key, "bgn_de": bgn_de, "pblntf_ty": p_type, "page_no": page_no, "page_count": 100}
                q = urllib.parse.urlencode(params)
                data = call_dart_api(f"https://opendart.fss.or.kr/api/list.json?{q}")
                if data.get("status") not in ("000", "013"):
                    if not raw_disclosures:
                        return {"status": data.get("status", "ERR"), "message": data.get("message", "DART 오류"), "items": []}
                    break
                page_list = data.get("list") or []
                if not page_list:
                    break
                raw_disclosures.extend(page_list)
                if len(page_list) < 100:
                    break

        def is_real_earnings_report(rnm: str) -> bool:
            if not rnm:
                return False
            # 예고, 안내공시, IR, 전망, 소송, 담보, 주식취득 등 비실적 공시는 100% 제외!
            exclude_kw = ("예고", "안내공시", "기업설명회", "IR", "장래사업", "경영계획", "전망", "소송", "담보", "취득", "처분", "배당", "증자", "감자", "사채", "주주총회")
            if any(ek in rnm for ek in exclude_kw):
                return False
            include_kw = ("잠정", "매출액또는손익구조", "분기보고서", "반기보고서", "사업보고서", "영업(잠정)실적")
            return any(ik in rnm for ik in include_kw)

        seen_codes = set()
        earnings_candidates = []

        for item in raw_disclosures:
            scode = (item.get("stock_code") or "").strip()
            if len(scode) != 6 or not scode.isdigit() or scode in seen_codes:
                continue
            rnm = (item.get("report_nm") or "").strip()
            if is_real_earnings_report(rnm):
                seen_codes.add(scode)
                earnings_candidates.append(item)

        # 절대 일반 공시(other_candidates)로 빈자리를 채우지 않고, 100% 실제 실적발표 기업만 선택!
        selected_disclosures = earnings_candidates[:30]

        def enrich_item(d_item):
            scode = d_item["stock_code"].strip()
            cname = d_item.get("corp_name", scode).strip()
            ccls = d_item.get("corp_cls", "Y")
            market = "KOSPI" if ccls == "Y" else ("KOSDAQ" if ccls == "K" else "KONEX")
            rcept_dt = d_item.get("rcept_dt", datetime.now().strftime("%Y%m%d"))
            fmt_date = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:8]}" if len(rcept_dt) == 8 else rcept_dt
            rcept_no = d_item.get("rcept_no", "")
            report_nm = d_item.get("report_nm", "DART 공시")

            fin = fetch_naver_quarterly_and_consensus(scode)
            has_cons = fin["op"].get("has_consensus", False)
            cons_note = "증권사 컨센서스 제공" if has_cons else "컨센 미제공(직전/전년 평균 대비)"

            return {
                "id": scode,
                "code": scode,
                "name": cname,
                "market": market,
                "sector": fin.get("sector") or "상장기업",
                "announceDate": fmt_date,
                "announceTime": "DART공시",
                "rceptNo": rcept_no,
                "reportName": report_nm,
                "price": fin.get("price") or 0,
                "afterHoursChange": fin.get("afterHoursChange") or 0.0,
                "rev": fin["rev"],
                "op": fin["op"],
                "net": fin["net"],
                "history": {
                    "quarters": fin.get("quarters") or ["1Q", "2Q", "3Q", "4Q", "최근분기"],
                    "rev": fin["rev"]["series"],
                    "op": fin["op"]["series"],
                },
                "comment": f"[실시간 DART 공시: {report_nm}] ({cons_note}) — 원문: https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
                "isRealDart": True,
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            enriched_items = list(pool.map(enrich_item, selected_disclosures))

        valid_items = [x for x in enriched_items if x["rev"]["actual"] > 0 or x["op"]["actual"] != 0]
        merged_by_code = {x["code"]: x for x in valid_items}
        for base_item in OCTOBER_3Q26_BASELINE:
            if base_item["code"] not in merged_by_code:
                merged_by_code[base_item["code"]] = base_item
        final_items = sorted(merged_by_code.values(), key=lambda x: x.get("announceDate", ""), reverse=True)

        return {
            "status": "000",
            "message": "정상",
            "count": len(final_items),
            "items": final_items,
            "list": raw_disclosures[:30],
        }
    except Exception as exc:
        return {"status": "ERROR", "message": f"실시간 데이터 수집 오류: {exc}", "items": []}


class EarningsWebHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/health":
            self._send_json({
                "status": "ok",
                "has_api_key": bool(DART_API_KEY),
                "time": datetime.now().isoformat(),
            })
            return
        if parsed.path in ("/api/dart-scan", "/api/dart-earnings"):
            qs = urllib.parse.parse_qs(parsed.query)
            default_bgn = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
            bgn_de = qs.get("bgn_de", qs.get("date", [default_bgn]))[0]
            req_key = qs.get("api_key", [""])[0].strip() or DART_API_KEY
            result = build_real_earnings_dataset(req_key, bgn_de)
            self._send_json(result)
            return
        return super().do_GET()

    def _send_json(self, payload: dict):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.end_headers()
        self.wfile.write(raw)



import threading
import time

def start_daily_4pm_7pm_scheduler():
    def _worker():
        last_run = ""
        while True:
            try:
                now = datetime.now()
                hm = now.strftime("%H:%M")
                day_key = now.strftime("%Y-%m-%d_") + hm
                if now.weekday() < 5 and hm in ("16:00", "19:00") and last_run != day_key:
                    last_run = day_key
                    if DART_API_KEY:
                        print(f"[자동 검출 스케줄러] {hm} 정각 DART + 네이버증권 실적 수집 시작...")
                        FINANCE_CACHE.clear()
                        res = build_real_earnings_dataset(DART_API_KEY)
                        try:
                            with open(os.path.join(BASE_DIR, "earnings_data.json"), "w", encoding="utf-8") as f:
                                json.dump(res, f, ensure_ascii=False, indent=2)
                            print(f"[자동 검출 완료] {res.get('count', 0)}개 상장사 실적 업데이트 완료!")
                        except Exception:
                            pass
            except Exception:
                pass
            time.sleep(20)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def main():
    parser = argparse.ArgumentParser(description="K-어닝 서프라이즈 레이더 웹 서버")
    parser.add_argument("--host", default="0.0.0.0", help="바인딩 호스트 (기본값: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="포트 번호 (기본값: 8000)")
    parser.add_argument("--api-key", default="", help="OPENDART 40자리 API 인증키")
    args = parser.parse_args()

    load_or_prompt_api_key(args.api_key)
    start_daily_4pm_7pm_scheduler()

    key_status = f"등록 완료 ({DART_API_KEY[:6]}...)" if DART_API_KEY else "미등록 (웹 화면에서 입력 가능)"
    url = f"http://localhost:{args.port}"
    try:
        server = ThreadingHTTPServer((args.host, args.port), EarningsWebHandler)
    except OSError:
        # 8000번 포트가 이미 이전 서버에 의해 사용 중이면 8001번 포트로 자동 우회
        args.port += 1
        url = f"http://localhost:{args.port}"
        server = ThreadingHTTPServer((args.host, args.port), EarningsWebHandler)

    print("============================================================")
    print(" [K-어닝 서프라이즈 레이더] DART + 네이버증권 실시간 연동 서버")
    print(f" - DART API 인증키 : {key_status}")
    print(f" - 접속 주소       : {url}")
    print("============================================================")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버를 종료합니다.")
        server.server_close()


if __name__ == "__main__":
    main()