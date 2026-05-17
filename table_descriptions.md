# Databricks Genie 테이블 디스크립션

---

## 1. money_digger.equity_derivative.item_info

**한국거래소(KRX)에 상장된 전체 종목의 기본 정보 테이블**

한국거래소에서 제공하는 KRX 상장 종목에 대한 정보로, 종목명·법인명·ISIN코드 등 종목 식별 정보를 담고 있습니다.
KOSPI, KOSDAQ, KONEX 시장에 상장된 주식·ETF·채권 등 모든 종목이 포함됩니다.
외국회사의 경우 국내 법인등록번호(crno)가 제공되지 않을 수 있습니다.
매일 1회 갱신되며, isinCd + basDt 기준으로 upsert됩니다.

**출처:** 금융위원회_KRX상장종목정보 (GetKrxListedInfoService/getItemInfo)

| 컬럼명 | 국문명 | 설명 |
|--------|--------|------|
| basDt | 기준일자 | 조회 기준일 (yyyyMMdd 형식, 통상 거래일) |
| srtnCd | 단축코드 | 종목코드보다 짧으면서 유일성이 보장되는 코드 (예: A000020) |
| isinCd | ISIN코드 | 국제 증권 식별 번호. 유가증권의 국제인증 고유번호 (예: KR7000020008) |
| mrktCtg | 시장구분 | 시장 구분 (KOSPI / KOSDAQ / KONEX 등) |
| itmsNm | 종목명 | 종목의 명칭 (예: 동화약품) |
| crno | 법인등록번호 | 종목의 법인등록번호. 외국회사는 미제공 |
| corpNm | 법인명 | 종목의 법인 명칭 (예: 동화약품(주)) |
| _loaded_at | 적재일자 | Airflow DAG 실행 기준일. 'initial_load'는 초기 수동 적재분 |

---

## 2. money_digger.equity_derivative.etf_price_info

**KRX에 상장된 ETF(상장지수펀드) 종목별 일별 시세 테이블**

한국거래소에서 제공하는 ETF 종목별 시세 정보로, 종가·순자산가치(NAV)·거래량·기초지수 정보 등을 포함합니다.
NAV는 ETF의 순자산총액을 상장좌수로 나눈 값으로, 시장가격과의 괴리율 분석에 활용됩니다.
매일 1회 갱신되며, isinCd + basDt 기준으로 upsert됩니다.

**출처:** 금융위원회_증권상품시세정보 (GetSecuritiesProductInfoService/getETFPriceInfo)

| 컬럼명 | 국문명 | 설명 |
|--------|--------|------|
| basDt | 기준일자 | 조회 기준일 (yyyyMMdd 형식) |
| srtnCd | 단축코드 | 종목코드보다 짧으면서 유일성이 보장되는 코드 |
| isinCd | ISIN코드 | 국제 증권 식별 번호 |
| itmsNm | 종목명 | 종목의 명칭 (예: KODEX 200) |
| clpr | 종가 | 정규시장 매매시간 종료 시까지 형성되는 최종가격 (원) |
| vs | 대비 | 전일 대비 등락 금액 (원) |
| fltRt | 등락률 | 전일 대비 등락에 따른 비율 (%) |
| nav | 순자산가치(NAV) | 순자산총액 / 상장좌수 (Net Asset Value) |
| mkp | 시가 | 정규시장 매매시간 개시 후 형성되는 최초가격 (원) |
| hipr | 고가 | 하루 중 가격의 최고치 (원) |
| lopr | 저가 | 하루 중 가격의 최저치 (원) |
| trqu | 거래량 | 체결수량의 누적 합계 (좌) |
| trPrc | 거래대금 | 체결가격 × 체결수량의 누적 합계 (원) |
| mrktTotAmt | 시가총액 | 종가 × 상장좌수 (원) |
| nPptTotAmt | 순자산총액 | ETF의 순자산총액 (원) |
| stLstgCnt | 상장좌수 | ETF의 상장좌수 |
| bssIdxIdxNm | 기초지수명 | ETF의 기초지수 명칭 (예: 코스피 200) |
| bssIdxClpr | 기초지수 종가 | ETF 기초지수의 종가 |
| _loaded_at | 적재일자 | Airflow DAG 실행 기준일. 'initial_load'는 초기 수동 적재분 |

---

## 3. money_digger.equity_derivative.stock_price_info

**KRX에 상장된 주식 종목별 일별 시세 테이블**

한국거래소에서 제공하는 주식 시세 정보로, KOSPI·KOSDAQ·KONEX에 상장된 주식의 시가·고가·저가·종가(OHLCV) 및 시가총액 데이터를 포함합니다.
매일 1회 갱신되며, isinCd + basDt 기준으로 upsert됩니다.

**출처:** 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getStockPriceInfo)

| 컬럼명 | 국문명 | 설명 |
|--------|--------|------|
| basDt | 기준일자 | 조회 기준일 (yyyyMMdd 형식) |
| srtnCd | 단축코드 | 종목코드보다 짧으면서 유일성이 보장되는 코드 6자리 |
| isinCd | ISIN코드 | 국제 증권 식별 번호 |
| itmsNm | 종목명 | 종목의 명칭 (예: 이스트아시아홀딩스) |
| mrktCtg | 시장구분 | 주식의 시장 구분 (KOSPI / KOSDAQ / KONEX 중 1) |
| clpr | 종가 | 정규시장 매매시간 종료 시까지 형성되는 최종가격 (원) |
| vs | 대비 | 전일 대비 등락 금액 (원) |
| fltRt | 등락률 | 전일 대비 등락에 따른 비율 (%) |
| mkp | 시가 | 정규시장 매매시간 개시 후 형성되는 최초가격 (원) |
| hipr | 고가 | 하루 중 가격의 최고치 (원) |
| lopr | 저가 | 하루 중 가격의 최저치 (원) |
| trqu | 거래량 | 체결수량의 누적 합계 (주) |
| trPrc | 거래대금 | 체결가격 × 체결수량의 누적 합계 (원) |
| lstgStCnt | 상장주식수 | 종목의 상장주식수 |
| mrktTotAmt | 시가총액 | 종가 × 상장주식수 (원) |
| _loaded_at | 적재일자 | Airflow DAG 실행 기준일. 'initial_load'는 초기 수동 적재분 |

---

## 4. money_digger.equity_derivative.preemptive_right_certificate_price_info

**KRX에 상장된 신주인수권증서 일별 시세 테이블**

유상증자 시 기존 주주에게 발행되는 신주인수권증서(R)의 시세 정보를 제공합니다.
신주인수권증서는 신주를 신주발행가격(nstIssPrc)으로 매수할 수 있는 권리로, 상장폐지일(dltDt)까지만 거래됩니다.
purRgtScrtItms 계열 컬럼은 해당 신주인수권증서의 목적주권(원주) 정보입니다.
매일 1회 갱신되며, isinCd + basDt 기준으로 upsert됩니다.

**출처:** 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getPreemptiveRightCertificatePriceInfo)

| 컬럼명 | 국문명 | 설명 |
|--------|--------|------|
| basDt | 기준일자 | 조회 기준일 (yyyyMMdd 형식) |
| srtnCd | 단축코드 | 종목코드보다 짧으면서 유일성이 보장되는 코드 |
| isinCd | ISIN코드 | 국제 증권 식별 번호 |
| itmsNm | 종목명 | 종목의 명칭 (예: 에어부산 7R) |
| mrktCtg | 시장구분 | 시장 구분 (KOSPI / KOSDAQ / KONEX 중 1) |
| clpr | 종가 | 정규시장 매매시간 종료 시까지 형성되는 최종가격 (원) |
| vs | 대비 | 전일 대비 등락 금액 (원) |
| fltRt | 등락률 | 전일 대비 등락에 따른 비율 (%) |
| mkp | 시가 | 정규시장 매매시간 개시 후 형성되는 최초가격 (원) |
| hipr | 고가 | 하루 중 가격의 최고치 (원) |
| lopr | 저가 | 하루 중 가격의 최저치 (원) |
| trqu | 거래량 | 체결수량의 누적 합계 |
| trPrc | 거래대금 | 체결가격 × 체결수량의 누적 합계 (원) |
| mrktTotAmt | 시가총액 | 종가 × 상장증서수 (원) |
| lstgCtfCnt | 상장증서수 | 신주인수권증서의 상장증서수 |
| nstIssPrc | 신주발행가 | 신주인수권증서의 신주발행가격 (원) |
| dltDt | 상장폐지일 | 신주인수권증서의 상장폐지일 (yyyyMMdd) |
| purRgtScrtItmsCd | 목적주권 종목코드 | 신주인수권증서의 목적주권(원주) 종목코드 |
| purRgtScrtItmsNm | 목적주권 종목명 | 신주인수권증서의 목적주권(원주) 종목명 |
| purRgtScrtItmsClpr | 목적주권 종가 | 신주인수권증서의 목적주권(원주) 종가 (원) |
| _loaded_at | 적재일자 | Airflow DAG 실행 기준일. 'initial_load'는 초기 수동 적재분 |

---

## 5. money_digger.equity_derivative.securities_price_info

**KRX에 상장된 수익증권 일별 시세 테이블**

한국거래소에서 제공하는 수익증권(부동산 펀드, 인프라 펀드 등) 시세 정보를 포함합니다.
상장좌수(stLstgCnt) 기준으로 시가총액이 산출됩니다.
매일 1회 갱신되며, isinCd + basDt 기준으로 upsert됩니다.

**출처:** 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getSecuritiesPriceInfo)

| 컬럼명 | 국문명 | 설명 |
|--------|--------|------|
| basDt | 기준일자 | 조회 기준일 (yyyyMMdd 형식) |
| srtnCd | 단축코드 | 종목코드보다 짧으면서 유일성이 보장되는 코드 6자리 |
| isinCd | ISIN코드 | 국제 증권 식별 번호 |
| itmsNm | 종목명 | 종목의 명칭 (예: 벨기에코어오피스부동산(A)) |
| clpr | 종가 | 정규시장 매매시간 종료 시까지 형성되는 최종가격 (원) |
| vs | 대비 | 전일 대비 등락 금액 (원) |
| fltRt | 등락률 | 전일 대비 등락에 따른 비율 (%) |
| mkp | 시가 | 정규시장 매매시간 개시 후 형성되는 최초가격 (원) |
| hipr | 고가 | 하루 중 가격의 최고치 (원) |
| lopr | 저가 | 하루 중 가격의 최저치 (원) |
| trqu | 거래량 | 체결수량의 누적 합계 |
| trPrc | 거래대금 | 체결가격 × 체결수량의 누적 합계 (원) |
| stLstgCnt | 상장좌수 | 수익증권의 상장좌수 |
| mrktTotAmt | 시가총액 | 종가 × 상장좌수 (원) |
| _loaded_at | 적재일자 | Airflow DAG 실행 기준일. 'initial_load'는 초기 수동 적재분 |

---

## 6. money_digger.equity_derivative.preemptive_right_securities_price_info

**KRX에 상장된 신주인수권증권 일별 시세 테이블**

분리형 신주인수권부사채(BW)에서 분리된 신주인수권증권(WR)의 시세 정보를 제공합니다.
행사가격(exertPric)으로 목적주권(원주)을 매수할 수 있는 권리이며, 존속기간(subtPdSttgDt ~ subtPdEdDt) 내에 행사 가능합니다.
purRgtScrtItms 계열 컬럼은 해당 신주인수권증권의 목적주권(원주) 정보입니다.
매일 1회 갱신되며, isinCd + basDt 기준으로 upsert됩니다.

**출처:** 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getPreemptiveRightSecuritiesPriceInfo)

| 컬럼명 | 국문명 | 설명 |
|--------|--------|------|
| basDt | 기준일자 | 조회 기준일 (yyyyMMdd 형식) |
| srtnCd | 단축코드 | 종목코드보다 짧으면서 유일성이 보장되는 코드 |
| isinCd | ISIN코드 | 국제 증권 식별 번호 |
| itmsNm | 종목명 | 종목의 명칭 (예: 대유플러스 12WR) |
| mrktCtg | 시장구분 | 시장 구분 (KOSPI / KOSDAQ / KONEX 중 1) |
| clpr | 종가 | 정규시장 매매시간 종료 시까지 형성되는 최종가격 (원) |
| vs | 대비 | 전일 대비 등락 금액 (원) |
| fltRt | 등락률 | 전일 대비 등락에 따른 비율 (%) |
| mkp | 시가 | 정규시장 매매시간 개시 후 형성되는 최초가격 (원) |
| hipr | 고가 | 하루 중 가격의 최고치 (원) |
| lopr | 저가 | 하루 중 가격의 최저치 (원) |
| trqu | 거래량 | 체결수량의 누적 합계 |
| trPrc | 거래대금 | 체결가격 × 체결수량의 누적 합계 (원) |
| mrktTotAmt | 시가총액 | 종가 × 상장증권수 (원) |
| lstgScrtCnt | 상장증권수 | 신주인수권증권의 상장증권수 |
| exertPric | 행사가격 | 권리를 행사할 때 적용되는 가격 (원). 이 가격으로 목적주권 매수 가능 |
| subtPdSttgDt | 존속기간 시작일 | 신주인수권증권의 존속기간 시작일 (yyyyMMdd) |
| subtPdEdDt | 존속기간 종료일 | 신주인수권증권의 존속기간 종료일 (yyyyMMdd) |
| purRgtScrtItmsCd | 목적주권 종목코드 | 신주인수권증권의 목적주권(원주) 종목코드 |
| purRgtScrtItmsNm | 목적주권 종목명 | 신주인수권증권의 목적주권(원주) 종목명 |
| purRgtScrtItmsClpr | 목적주권 종가 | 신주인수권증권의 목적주권(원주) 종가 (원) |
| _loaded_at | 적재일자 | Airflow DAG 실행 기준일. 'initial_load'는 초기 수동 적재분 |

---

## 공통 참고사항

- **기본 조인 키:** `isinCd` (국제 증권 식별 번호) — 테이블 간 종목 연결 시 사용
- **날짜 필터:** `basDt` 컬럼 사용 (STRING 타입, yyyyMMdd 형식)
  ```sql
  WHERE basDt = '20260512'
  WHERE basDt BETWEEN '20260101' AND '20260512'
  ```
- **금액·수량 계산 시 CAST 필요** (모든 컬럼이 STRING 타입)
  ```sql
  CAST(clpr AS DOUBLE) * CAST(lstgStCnt AS DOUBLE) AS 시가총액_검증
  ```
- **`_loaded_at = 'initial_load'`:** 초기 수동 CSV 적재분 (2026년 이전 과거 데이터 포함)
- **데이터 출처:** 금융위원회 공공데이터포털 (data.go.kr), 한국거래소(KRX) 제공
- **갱신 주기:** 매일 23:59 KST (Airflow DAG 자동 실행, 영업일 기준)
- **적재 방식:** GCS raw JSON → Databricks Delta Lake (MERGE upsert, isinCd + basDt 기준)