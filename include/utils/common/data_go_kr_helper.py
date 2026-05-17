PAGE_SIZE = 10000  # 공공데이터 포털 API 최대 허용 행 수


def _fetch_all_items(url: str, start_dt: str = None):
    """
    공공데이터 포털 API 페이지네이션으로 전체 데이터 수집
    - start_dt 있으면 해당 날짜 이후 데이터만 수집 (incremental)
    - start_dt 없으면 전체 수집 (full load)

    Args:
        url: 공공데이터 포털 API URL
        start_dt: 수집 시작 날짜 (yyyyMMdd), None이면 전체 수집
    """
    import requests
    from include.utils.common.secret_manager_helper import get_secret
    base_params = {
        'serviceKey': get_secret("DATA_GO_KR_API_KEY"),
        'resultType': 'json',
        'numOfRows': str(PAGE_SIZE),
        'pageNo': '1',
    }
    if start_dt:
        base_params['beginBasDt'] = start_dt

    # totalCount 확인
    response = requests.get(url, params={**base_params, 'numOfRows': '1'})
    body = response.json()['response']['body']
    total_count = int(body['totalCount'])

    if total_count == 0:
        print("새로운 데이터 없음")
        return []

    total_pages = -(-total_count // PAGE_SIZE)  # ceiling division
    print(f"총 {total_count}건 / {total_pages}페이지 수집 시작")

    all_items = []
    for page in range(1, total_pages + 1):
        params = {**base_params, 'pageNo': str(page)}
        response = requests.get(url, params=params)
        items = response.json()['response']['body']['items']['item']

        if isinstance(items, list):
            all_items.extend(items)
        else:
            all_items.append(items)  # 단건이면 dict로 올 수 있음

        print(f"  페이지 {page}/{total_pages} 완료 ({len(all_items)}/{total_count}건)")

    return all_items