"""
금융위원회_주식 가격정보 전체 데이터를 CSV로 추출하는 일회성 스크립트
"""
import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("DATA_GO_KR_API_KEY")
URL = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"
PAGE_SIZE = 10000
OUTPUT_DIR = "first_request/csv"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "stock_price_info.csv")


def _fetch_page(params, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(URL, params=params)
            return response.json()['response']['body']['items']['item']
        except Exception as e:
            print(f"  ⚠️ 페이지 {params['pageNo']} 실패 ({attempt+1}/{retries}): {e}")
            time.sleep(2)
    raise Exception(f"페이지 {params['pageNo']} {retries}회 재시도 후 실패")


def fetch_all_items():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    base_params = {
        'serviceKey': API_KEY,
        'resultType': 'json',
        'numOfRows': str(PAGE_SIZE),
    }

    response = requests.get(URL, params={**base_params, 'numOfRows': '1', 'pageNo': '1'})
    total_count = int(response.json()['response']['body']['totalCount'])
    total_pages = -(-total_count // PAGE_SIZE)
    print(f"총 {total_count}건 / {total_pages}페이지 수집 시작")

    all_items = []
    total_saved = 0

    for page in range(1, total_pages + 1):
        params = {**base_params, 'pageNo': str(page)}
        items = _fetch_page(params)

        if isinstance(items, list):
            all_items.extend(items)
        else:
            all_items.append(items)

        if len(all_items) >= 100000:
            _flush_to_csv(all_items, first=total_saved == 0)
            total_saved += len(all_items)
            all_items = []
            print(f"  중간 저장 완료 ({total_saved}건)")

        print(f"  페이지 {page}/{total_pages} 완료 ({total_saved + len(all_items)}/{total_count}건)")

    if all_items:
        _flush_to_csv(all_items, first=total_saved == 0)
        total_saved += len(all_items)

    print(f"✅ 완료: {OUTPUT_FILE} (총 {total_saved}건)")


def _flush_to_csv(items, first=False):
    df = pd.DataFrame(items)
    df.to_csv(
        OUTPUT_FILE,
        mode='w' if first else 'a',
        header=first,
        index=False,
        encoding='utf-8-sig'
    )


if __name__ == "__main__":
    fetch_all_items()