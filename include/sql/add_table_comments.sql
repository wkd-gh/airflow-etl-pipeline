-- ============================================================
-- 테이블 및 컬럼 코멘트 등록 스크립트
-- Databricks SQL에서 실행
-- ============================================================


-- ============================================================
-- 1. item_info
-- ============================================================
COMMENT ON TABLE money_digger.equity_derivative.item_info IS
'한국거래소(KRX)에 상장된 전체 종목의 기본 정보 테이블. 종목명·법인명·ISIN코드 등 종목 식별 정보를 담고 있으며, KOSPI·KOSDAQ·KONEX 시장에 상장된 주식·ETF·채권 등 모든 종목이 포함됩니다. 외국회사의 경우 국내 법인등록번호(crno)가 제공되지 않을 수 있습니다. 매일 1회 갱신. 출처: 금융위원회_KRX상장종목정보 (GetKrxListedInfoService/getItemInfo)';

ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN basDt     COMMENT '조회 기준일 (yyyyMMdd 형식, 통상 거래일)';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN srtnCd    COMMENT '종목코드보다 짧으면서 유일성이 보장되는 단축코드 (예: A000020)';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN isinCd    COMMENT '국제 증권 식별 번호. 유가증권의 국제인증 고유번호 (예: KR7000020008)';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN mrktCtg   COMMENT '시장 구분 (KOSPI / KOSDAQ / KONEX 등)';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN itmsNm    COMMENT '종목의 명칭 (예: 동화약품)';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN crno      COMMENT '종목의 법인등록번호. 외국회사는 미제공';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN corpNm    COMMENT '종목의 법인 명칭 (예: 동화약품(주))';
ALTER TABLE money_digger.equity_derivative.item_info ALTER COLUMN _loaded_at COMMENT 'Airflow DAG 실행 기준일. initial_load는 초기 수동 적재분';


-- ============================================================
-- 2. etf_price_info
-- ============================================================
COMMENT ON TABLE money_digger.equity_derivative.etf_price_info IS
'KRX에 상장된 ETF(상장지수펀드) 종목별 일별 시세 테이블. 종가·순자산가치(NAV)·거래량·기초지수 정보 등을 포함합니다. NAV는 ETF의 순자산총액을 상장좌수로 나눈 값으로, 시장가격과의 괴리율 분석에 활용됩니다. 매일 1회 갱신. 출처: 금융위원회_증권상품시세정보 (GetSecuritiesProductInfoService/getETFPriceInfo)';

ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN basDt       COMMENT '조회 기준일 (yyyyMMdd 형식)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN srtnCd      COMMENT '종목코드보다 짧으면서 유일성이 보장되는 단축코드';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN isinCd      COMMENT '국제 증권 식별 번호';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN itmsNm      COMMENT '종목의 명칭 (예: KODEX 200)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN clpr        COMMENT '정규시장 매매시간 종료 시까지 형성되는 최종가격 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN vs          COMMENT '전일 대비 등락 금액 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN fltRt       COMMENT '전일 대비 등락에 따른 비율 (%)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN nav         COMMENT '순자산가치(NAV). 순자산총액 / 상장좌수';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN mkp         COMMENT '정규시장 매매시간 개시 후 형성되는 최초가격 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN hipr        COMMENT '하루 중 가격의 최고치 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN lopr        COMMENT '하루 중 가격의 최저치 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN trqu        COMMENT '체결수량의 누적 합계 (좌)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN trPrc       COMMENT '체결가격 × 체결수량의 누적 합계 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN mrktTotAmt  COMMENT '시가총액. 종가 × 상장좌수 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN nPptTotAmt  COMMENT 'ETF의 순자산총액 (원)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN stLstgCnt   COMMENT 'ETF의 상장좌수';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN bssIdxIdxNm COMMENT 'ETF 기초지수의 명칭 (예: 코스피 200)';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN bssIdxClpr  COMMENT 'ETF 기초지수의 종가';
ALTER TABLE money_digger.equity_derivative.etf_price_info ALTER COLUMN _loaded_at  COMMENT 'Airflow DAG 실행 기준일. initial_load는 초기 수동 적재분';


-- ============================================================
-- 3. stock_price_info
-- ============================================================
COMMENT ON TABLE money_digger.equity_derivative.stock_price_info IS
'KRX에 상장된 주식 종목별 일별 시세 테이블. KOSPI·KOSDAQ·KONEX에 상장된 주식의 시가·고가·저가·종가(OHLCV) 및 시가총액 데이터를 포함합니다. 매일 1회 갱신. 출처: 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getStockPriceInfo)';

ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN basDt      COMMENT '조회 기준일 (yyyyMMdd 형식)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN srtnCd     COMMENT '종목코드보다 짧으면서 유일성이 보장되는 단축코드 6자리';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN isinCd     COMMENT '국제 증권 식별 번호';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN itmsNm     COMMENT '종목의 명칭';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN mrktCtg    COMMENT '주식의 시장 구분 (KOSPI / KOSDAQ / KONEX 중 1)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN clpr       COMMENT '정규시장 매매시간 종료 시까지 형성되는 최종가격 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN vs         COMMENT '전일 대비 등락 금액 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN fltRt      COMMENT '전일 대비 등락에 따른 비율 (%)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN mkp        COMMENT '정규시장 매매시간 개시 후 형성되는 최초가격 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN hipr       COMMENT '하루 중 가격의 최고치 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN lopr       COMMENT '하루 중 가격의 최저치 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN trqu       COMMENT '체결수량의 누적 합계 (주)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN trPrc      COMMENT '체결가격 × 체결수량의 누적 합계 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN lstgStCnt  COMMENT '종목의 상장주식수';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN mrktTotAmt COMMENT '시가총액. 종가 × 상장주식수 (원)';
ALTER TABLE money_digger.equity_derivative.stock_price_info ALTER COLUMN _loaded_at COMMENT 'Airflow DAG 실행 기준일. initial_load는 초기 수동 적재분';


-- ============================================================
-- 4. preemptive_right_certificate_price_info
-- ============================================================
COMMENT ON TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info IS
'KRX에 상장된 신주인수권증서(R) 일별 시세 테이블. 유상증자 시 기존 주주에게 발행되는 신주인수권증서의 시세 정보를 제공합니다. 신주발행가격(nstIssPrc)으로 목적주권을 매수할 수 있는 권리이며, 상장폐지일(dltDt)까지만 거래됩니다. 매일 1회 갱신. 출처: 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getPreemptiveRightCertificatePriceInfo)';

ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN basDt              COMMENT '조회 기준일 (yyyyMMdd 형식)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN srtnCd             COMMENT '종목코드보다 짧으면서 유일성이 보장되는 단축코드';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN isinCd             COMMENT '국제 증권 식별 번호';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN itmsNm             COMMENT '종목의 명칭 (예: 에어부산 7R)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN mrktCtg            COMMENT '시장 구분 (KOSPI / KOSDAQ / KONEX 중 1)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN clpr               COMMENT '정규시장 매매시간 종료 시까지 형성되는 최종가격 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN vs                 COMMENT '전일 대비 등락 금액 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN fltRt              COMMENT '전일 대비 등락에 따른 비율 (%)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN mkp                COMMENT '정규시장 매매시간 개시 후 형성되는 최초가격 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN hipr               COMMENT '하루 중 가격의 최고치 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN lopr               COMMENT '하루 중 가격의 최저치 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN trqu               COMMENT '체결수량의 누적 합계';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN trPrc              COMMENT '체결가격 × 체결수량의 누적 합계 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN mrktTotAmt         COMMENT '시가총액. 종가 × 상장증서수 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN lstgCtfCnt         COMMENT '신주인수권증서의 상장증서수';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN nstIssPrc          COMMENT '신주인수권증서의 신주발행가격 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN dltDt              COMMENT '신주인수권증서의 상장폐지일 (yyyyMMdd)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN purRgtScrtItmsCd   COMMENT '신주인수권증서의 목적주권(원주) 종목코드';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN purRgtScrtItmsNm   COMMENT '신주인수권증서의 목적주권(원주) 종목명';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN purRgtScrtItmsClpr COMMENT '신주인수권증서의 목적주권(원주) 종가 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_certificate_price_info ALTER COLUMN _loaded_at         COMMENT 'Airflow DAG 실행 기준일. initial_load는 초기 수동 적재분';


-- ============================================================
-- 5. securities_price_info
-- ============================================================
COMMENT ON TABLE money_digger.equity_derivative.securities_price_info IS
'KRX에 상장된 수익증권(부동산 펀드, 인프라 펀드 등) 일별 시세 테이블. 상장좌수(stLstgCnt) 기준으로 시가총액이 산출됩니다. 매일 1회 갱신. 출처: 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getSecuritiesPriceInfo)';

ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN basDt      COMMENT '조회 기준일 (yyyyMMdd 형식)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN srtnCd     COMMENT '종목코드보다 짧으면서 유일성이 보장되는 단축코드 6자리';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN isinCd     COMMENT '국제 증권 식별 번호';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN itmsNm     COMMENT '종목의 명칭 (예: 벨기에코어오피스부동산(A))';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN clpr       COMMENT '정규시장 매매시간 종료 시까지 형성되는 최종가격 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN vs         COMMENT '전일 대비 등락 금액 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN fltRt      COMMENT '전일 대비 등락에 따른 비율 (%)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN mkp        COMMENT '정규시장 매매시간 개시 후 형성되는 최초가격 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN hipr       COMMENT '하루 중 가격의 최고치 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN lopr       COMMENT '하루 중 가격의 최저치 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN trqu       COMMENT '체결수량의 누적 합계';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN trPrc      COMMENT '체결가격 × 체결수량의 누적 합계 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN stLstgCnt  COMMENT '수익증권의 상장좌수';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN mrktTotAmt COMMENT '시가총액. 종가 × 상장좌수 (원)';
ALTER TABLE money_digger.equity_derivative.securities_price_info ALTER COLUMN _loaded_at COMMENT 'Airflow DAG 실행 기준일. initial_load는 초기 수동 적재분';


-- ============================================================
-- 6. preemptive_right_securities_price_info
-- ============================================================
COMMENT ON TABLE money_digger.equity_derivative.preemptive_right_securities_price_info IS
'KRX에 상장된 신주인수권증권(WR) 일별 시세 테이블. 분리형 신주인수권부사채(BW)에서 분리된 신주인수권증권의 시세 정보를 제공합니다. 행사가격(exertPric)으로 목적주권을 매수할 수 있는 권리이며, 존속기간(subtPdSttgDt ~ subtPdEdDt) 내에 행사 가능합니다. 매일 1회 갱신. 출처: 금융위원회_주식시세정보 (GetStockSecuritiesInfoService/getPreemptiveRightSecuritiesPriceInfo)';

ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN basDt              COMMENT '조회 기준일 (yyyyMMdd 형식)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN srtnCd             COMMENT '종목코드보다 짧으면서 유일성이 보장되는 단축코드';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN isinCd             COMMENT '국제 증권 식별 번호';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN itmsNm             COMMENT '종목의 명칭 (예: 대유플러스 12WR)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN mrktCtg            COMMENT '시장 구분 (KOSPI / KOSDAQ / KONEX 중 1)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN clpr               COMMENT '정규시장 매매시간 종료 시까지 형성되는 최종가격 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN vs                 COMMENT '전일 대비 등락 금액 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN fltRt              COMMENT '전일 대비 등락에 따른 비율 (%)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN mkp                COMMENT '정규시장 매매시간 개시 후 형성되는 최초가격 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN hipr               COMMENT '하루 중 가격의 최고치 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN lopr               COMMENT '하루 중 가격의 최저치 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN trqu               COMMENT '체결수량의 누적 합계';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN trPrc              COMMENT '체결가격 × 체결수량의 누적 합계 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN mrktTotAmt         COMMENT '시가총액. 종가 × 상장증권수 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN lstgScrtCnt        COMMENT '신주인수권증권의 상장증권수';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN exertPric          COMMENT '권리를 행사할 때 적용되는 가격 (원). 이 가격으로 목적주권 매수 가능';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN subtPdSttgDt       COMMENT '신주인수권증권의 존속기간 시작일 (yyyyMMdd)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN subtPdEdDt         COMMENT '신주인수권증권의 존속기간 종료일 (yyyyMMdd)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN purRgtScrtItmsCd   COMMENT '신주인수권증권의 목적주권(원주) 종목코드';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN purRgtScrtItmsNm   COMMENT '신주인수권증권의 목적주권(원주) 종목명';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN purRgtScrtItmsClpr COMMENT '신주인수권증권의 목적주권(원주) 종가 (원)';
ALTER TABLE money_digger.equity_derivative.preemptive_right_securities_price_info ALTER COLUMN _loaded_at         COMMENT 'Airflow DAG 실행 기준일. initial_load는 초기 수동 적재분';