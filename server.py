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

        m_cop = re.search(r'<div class="section cop_analysis">(.*?)</div>\s*</div>', page, re.S)
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
        bgn_de = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")

    try:
        raw_disclosures = []
        for p_type in ("I", "A", ""):
            params = {"crtfc_key": clean_key, "bgn_de": bgn_de, "page_count": 100}
            if p_type:
                params["pblntf_ty"] = p_type
            q = urllib.parse.urlencode(params)
            data = call_dart_api(f"https://opendart.fss.or.kr/api/list.json?{q}")
            if data.get("status") not in ("000", "013"):
                return {"status": data.get("status", "ERR"), "message": data.get("message", "DART 오류"), "items": []}
            if isinstance(data.get("list"), list):
                raw_disclosures.extend(data["list"])
            if len(raw_disclosures) >= 80:
                break

        seen_codes = set()
        earnings_candidates = []
        other_candidates = []
        earnings_keywords = ("잠정", "실적", "손익", "매출액", "분기보고서", "반기보고서", "사업보고서", "연결재무")

        for item in raw_disclosures:
            scode = (item.get("stock_code") or "").strip()
            if len(scode) != 6 or not scode.isdigit() or scode in seen_codes:
                continue
            seen_codes.add(scode)
            rnm = item.get("report_nm") or ""
            if any(k in rnm for k in earnings_keywords):
                earnings_candidates.append(item)
            else:
                other_candidates.append(item)

        selected_disclosures = (earnings_candidates + other_candidates)[:20]

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
        if not valid_items:
            valid_items = enriched_items

        return {
            "status": "000",
            "message": "정상",
            "count": len(valid_items),
            "items": valid_items,
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


def main():
    parser = argparse.ArgumentParser(description="K-어닝 서프라이즈 레이더 웹 서버")
    parser.add_argument("--host", default="0.0.0.0", help="바인딩 호스트 (기본값: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="포트 번호 (기본값: 8000)")
    parser.add_argument("--api-key", default="", help="OPENDART 40자리 API 인증키")
    args = parser.parse_args()

    load_or_prompt_api_key(args.api_key)

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
