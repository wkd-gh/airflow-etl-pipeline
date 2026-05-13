"""
first_request/csv/ 안의 모든 CSV 파일에 _loaded_at 컬럼을 추가하는 일회성 스크립트
"""
import pandas as pd
import os
import glob

CSV_DIR = "first_request/csv"
LOADED_AT_VALUE = "initial_load"


def add_loaded_at():
    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))

    if not csv_files:
        print(f"❌ {CSV_DIR} 안에 CSV 파일이 없습니다.")
        return

    for filepath in csv_files:
        filename = os.path.basename(filepath)

        df = pd.read_csv(filepath, encoding='utf-8-sig')

        if '_loaded_at' in df.columns:
            print(f"⏭️  스킵 (이미 존재): {filename}")
            continue

        df['_loaded_at'] = LOADED_AT_VALUE
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"✅ 완료: {filename} ({len(df)}건)")


if __name__ == "__main__":
    add_loaded_at()