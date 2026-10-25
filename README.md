# K-어닝 서프라이즈 레이더 — 웹 배포 및 외부 접속 가이드

이 패키지는 제작된 **K-어닝 서프라이즈 레이더 (`index.html`)**를 언제 어디서나 웹 브라우저(PC·스마트폰·태블릿)로 접속할 수 있도록 구성한 배포 패키지입니다.

---

## 방법 1. 10초 만에 무료 웹 주소(`https://...`) 만들기 (설치·코딩 없음)

`index.html` 파일 하나에 모든 화면과 차트, 검출 로직이 포함되어 있어 정적 웹 호스팅에 올리면 즉시 고유 URL이 생성됩니다.

1. **Netlify Drop (가장 빠른 방법)**
   - 브라우저에서 `https://app.netlify.com/drop` 접속
   - 압축을 푼 폴더(`kr_earnings_web_deploy`) 또는 `index.html` 파일을 화면에 드래그 앤 드롭
   - 즉시 `https://xxxx.netlify.app` 형태의 전용 웹 주소가 발급되어 스마트폰과 PC 어디서나 접속할 수 있습니다.

2. **Cloudflare Pages / Vercel / GitHub Pages**
   - GitHub 저장소에 이 폴더의 파일들을 업로드한 뒤 GitHub Pages를 켜거나 Vercel/Cloudflare Pages에 연결하면 무료 평생 도메인이 생성됩니다.

---

## 방법 2. 내 PC 또는 클라우드 서버에서 직접 웹 서버 실행하기

파이썬 기본 내장 서버(`server.py`)가 포함되어 있어 별도 라이브러리 설치 없이 즉시 구동됩니다.

```bash
# 압축 해제 후 폴더 이동
cd kr_earnings_web_deploy

# 8000번 포트로 웹 서버 실행 (DART API 키가 있는 경우 환경변수로 전달 가능)
export DART_API_KEY="발급받은_DART_API_키"
python3 server.py --port 8000
```

- 실행 후 브라우저에서 `http://localhost:8000` 또는 `http://내서버IP:8000` 으로 접속하세요.
