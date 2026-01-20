#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간 뉴스 바이럴 지수 계산 스크립트
1. DB(Supabase)에서 뉴스 데이터를 가져옴
2. 주간(Weekly) 단위로 뉴스 기사 수를 집계
3. WoW, MA4 편차, Z-Score를 결합하여 바이럴 지수 산출
4. 분석용 CSV 파일로 저장
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Supabase 설정
try:
    from supabase import create_client, Client
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    if SUPABASE_URL and SUPABASE_KEY:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        SUPABASE_ENABLED = True
    else:
        SUPABASE_ENABLED = False
        print("[오류] Supabase 환경변수가 설정되지 않았습니다.")
except ImportError:
    SUPABASE_ENABLED = False
    print("[오류] supabase 라이브러리가 설치되지 않았습니다.")

def fetch_news_data_from_db():
    """Supabase news_2025_categorized 테이블에서 모든 뉴스 데이터 로드 (페이지네이션 적용)"""
    if not SUPABASE_ENABLED:
        return None
    
    print("📂 DB에서 뉴스 데이터를 불러오는 중...")
    table_name = "news_2025_categorized"
    all_data = []
    page_size = 1000
    offset = 0
    
    try:
        while True:
            res = supabase.table(table_name).select('news_date, category').range(offset, offset + page_size - 1).execute()
            data = res.data
            if not data:
                break
            all_data.extend(data)
            offset += page_size
            if len(data) < page_size:
                break
            print(f"  >> 로딩 중... ({len(all_data):,}개)", end="\r")
        
        df = pd.DataFrame(all_data)
        if df.empty:
            print("\n  >> 불러온 데이터가 없습니다.")
            return None
        
        df = df.rename(columns={'news_date': 'date'})
        df['date'] = pd.to_datetime(df['date'])
        print(f"\n  >> 로드 완료: {len(df):,}개 기사")
        return df
    except Exception as e:
        print(f"\n  >> DB 로드 중 오류 발생: {e}")
        return None

def calculate_weekly_viral_index(df):
    """주간 바이럴 지수 계산 (수~화 기준, 인피니티 보정 적용)"""
    print("\n📊 주간 바이럴 지수 산출 중 (무한대 값 보정 포함)...")
    
    # 1. 주간/카테고리별 기사 수 집계
    weekly_counts = df.groupby([pd.Grouper(key='date', freq='W-TUE'), 'category']).size().unstack(level=1).fillna(0)
    
    # 2. 지수 구성 요소 계산 (안정성 강화)
    # 2-1. WoW (Week-over-Week) 증가율 - 분모에 1을 더해 0->N 급증 시 inf 방지
    prev_counts = weekly_counts.shift(1)
    wow_growth = ((weekly_counts - prev_counts) / (prev_counts + 1)) * 100
    
    # 2-2. 4주 이동평균 대비 편차 - 분모에 1을 더해 inf 방지
    ma4 = weekly_counts.rolling(window=4, min_periods=1).mean()
    ma_deviation = ((weekly_counts - ma4) / (ma4 + 1)) * 100
    
    # 2-3. Z-Score
    z_scores = (weekly_counts - weekly_counts.mean()) / (weekly_counts.std() + 1e-9)
    
    # 3. 종합 바이럴 지수 (가중합)
    # 극단적인 이상치(Outlier)를 방지하기 위해 각 지수를 적절한 범위로 클리핑(Clipping)
    viral_index = (
        wow_growth.clip(upper=300).fillna(0) * 0.4 +
        ma_deviation.clip(upper=300).fillna(0) * 0.4 +
        z_scores.clip(lower=-3, upper=3).fillna(0) * 10 * 0.2
    )
    
    print(f"  >> 계산 완료: {len(weekly_counts)}개 주차 x {len(weekly_counts.columns)}개 카테고리")
    return weekly_counts, viral_index

def main():
    print("=" * 60)
    print("[주간 뉴스 바이럴 지수 산출 - 수~화 기준]")
    print("=" * 60)
    
    news_df = fetch_news_data_from_db()
    if news_df is None:
        return
    
    counts_df, viral_df = calculate_weekly_viral_index(news_df)
    
    # 3. 데이터 구조 정리
    viral_long = viral_df.stack().reset_index()
    viral_long.columns = ['end_date', 'category', 'viral_index']
    
    counts_long = counts_df.stack().reset_index()
    counts_long.columns = ['end_date', 'category', 'article_count']
    
    result_df = pd.merge(viral_long, counts_long, on=['end_date', 'category'])
    
    # 4. 베스트셀러 테이블과 매칭을 위한 ymw 및 bestseller_week 포맷 생성
    # end_date가 화요일이므로, 시작일은 6일 전인 수요일
    result_df['start_date'] = result_df['end_date'] - pd.Timedelta(days=6)
    
    # ymw 생성 로직: YYYYMM + (해당 월의 n번째 주)
    # 베스트셀러 DB의 ymw 규칙을 따름 (종료일 기준)
    def generate_ymw(dt):
        year = dt.year
        month = dt.month
        week = (dt.day - 1) // 7 + 1
        return f"{year}{month:02d}{week}"
    
    result_df['ymw'] = result_df['end_date'].apply(generate_ymw)
    
    result_df['bestseller_week'] = (
        result_df['start_date'].dt.strftime('%Y.%m.%d') + 
        " ~ " + 
        result_df['end_date'].dt.strftime('%Y.%m.%d')
    )
    
    # 컬럼 순서 조정
    result_df = result_df[['ymw', 'bestseller_week', 'category', 'viral_index', 'article_count', 'start_date', 'end_date']]
    
    # 5. CSV 저장
    output_dir = "/Users/minzzy/Desktop/statrack/book-review-analysis/analysis"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_path = os.path.join(output_dir, "weekly_news_viral_index.csv")
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print("\n" + "=" * 60)
    print(f"✅ 작업 완료: {output_path}")
    print(f"   - 데이터 기간: {result_df['start_date'].min().date()} ~ {result_df['end_date'].max().date()}")
    print(f"   - 총 데이터 행: {len(result_df)}개")
    print("=" * 60)

if __name__ == "__main__":
    main()
