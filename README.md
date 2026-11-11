# K-어닝 서프라이즈 레이더 — 웹 배포 및 로컬 실행 가이드

## 1. 로컬 PC에서 실시간 DART·네이버 종목분석 연동 서버 실행
```bash
python3 server.py --port 8000
```
브라우저에서 `http://localhost:8000` 접속 시 DART 공정공시 및 네이버 증권 종목분석 컨센서스를 실시간으로 연동합니다.

## 2. 정적 웹 호스팅 업로드 (Netlify Drop / GitHub Pages / Vercel)
폴더 내 `index.html` 및 `earnings_data.json`을 업로드하여 즉시 웹에서 조회할 수 있습니다.
