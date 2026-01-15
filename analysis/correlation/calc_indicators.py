#!/usr/bin/env python3
"""가공 지표 계산 및 상관관계 분석"""

import os
import pandas as pd
import numpy as np
from scipy import stats
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# 뉴스 데이터 로드
df_news = pd.read_csv('/Users/minzzy/Desktop/statrack/book-review-analysis/analysis/category_trend_comparison.csv', encoding='utf-8-sig')
df_keywords = pd.read_csv('/Users/minzzy/Desktop/statrack/book-review-analysis/analysis/monthly_all_category_rankings.csv', encoding='utf-8-sig')

# 베스트셀러 데이터
bestsellers_res = supabase.table('bestsellers').select('*').execute()
df_bs = pd.DataFrame(bestsellers_res.data)
books_res = supabase.table('books').select('product_code, title, category_1').execute()
df_books = pd.DataFrame(books_res.data)

df_bs = df_bs.merge(df_books, on='product_code', how='left')
df_bs['month'] = pd.to_datetime(df_bs['bestseller_month']).dt.strftime('%Y-%m')
df_bs = df_bs[df_bs['month'].isin(['2025-09', '2025-10', '2025-11', '2025-12'])]

# 월별 카테고리 도서 수
bs_monthly = df_bs.groupby(['month', 'category_1']).size().unstack(fill_value=0)

months = ['2025-09', '2025-10', '2025-11', '2025-12']
categories = df_news['카테고리'].tolist()

print("=" * 70)
print("가공 지표 계산 결과")
print("=" * 70)

# 1. 이슈 급등 지수 계산
print("\n[1] 이슈 급등 지수 (Surge Index) - 전월 대비 변화율 %")
print("-" * 70)

surge_data = {}
for cat in categories:
    vals = [df_news[df_news['카테고리']==cat][f'{m}_기사수'].values[0] for m in months]
    surge = []
    for i in range(1, len(vals)):
        if vals[i-1] > 0:
            s = (vals[i] - vals[i-1]) / vals[i-1] * 100
        else:
            s = 0
        surge.append(round(s, 1))
    surge_data[cat] = surge
    print(f"{cat:20s}: 10월 {surge[0]:+7.1f}% | 11월 {surge[1]:+7.1f}% | 12월 {surge[2]:+7.1f}%")

# 2. 카테고리 점유율 변화
print("\n[2] 카테고리 점유율 변화 (Share Shift) - 전월 대비 %p")
print("-" * 70)

share_data = {}
for cat in categories:
    shares = []
    for m in months:
        ratio_str = df_news[df_news['카테고리']==cat][f'{m}_비율'].values[0]
        ratio = float(ratio_str.replace('%', ''))
        shares.append(ratio)

    shift = []
    for i in range(1, len(shares)):
        shift.append(round(shares[i] - shares[i-1], 2))
    share_data[cat] = shift
    print(f"{cat:20s}: 10월 {shift[0]:+6.2f}%p | 11월 {shift[1]:+6.2f}%p | 12월 {shift[2]:+6.2f}%p")

# 3. 키워드 집중도 (상위 3개 키워드 비중)
print("\n[3] 키워드 집중도 (상위 3개 키워드 빈도 합)")
print("-" * 70)

conc_data = {}
for cat in categories:
    conc = []
    for m in months:
        m_int = int(m.replace('-', ''))
        row = df_keywords[(df_keywords['카테고리']==cat) & (df_keywords['년월']==m_int)]
        if len(row) > 0:
            top3 = 0
            for i in range(1, 4):
                val = row[f'빈도{i}'].values[0]
                if pd.notna(val):
                    top3 += val
            conc.append(int(top3))
        else:
            conc.append(0)
    conc_data[cat] = conc
    print(f"{cat:20s}: 9월 {conc[0]:5d} | 10월 {conc[1]:5d} | 11월 {conc[2]:5d} | 12월 {conc[3]:5d}")

# 4. 복합 바이럴 지수 계산
print("\n[4] 복합 바이럴 지수 (Viral Score)")
print("    = 급등지수 정규화 × 0.5 + 점유율변화 정규화 × 0.5")
print("-" * 70)

viral_data = {}
for cat in categories:
    viral = []
    for i in range(3):  # 10월, 11월, 12월
        surge_norm = min(max(surge_data[cat][i] / 100, -1), 1)  # -100%~100% → -1~1
        share_norm = min(max(share_data[cat][i] / 5, -1), 1)     # -5%p~5%p → -1~1
        v = (surge_norm * 0.5 + share_norm * 0.5) * 100
        viral.append(round(v, 1))
    viral_data[cat] = viral
    print(f"{cat:20s}: 10월 {viral[0]:+6.1f} | 11월 {viral[1]:+6.1f} | 12월 {viral[2]:+6.1f}")

# 5. 베스트셀러 변화와 비교
print("\n" + "=" * 70)
print("바이럴 지수 vs 베스트셀러 도서 수 변화")
print("=" * 70)

print("\n[도서 수 변화]")
print("-" * 70)
book_change = {}
for cat in categories:
    bs_vals = [bs_monthly.loc[m, cat] if cat in bs_monthly.columns and m in bs_monthly.index else 0 for m in months]
    changes = []
    for i in range(1, len(bs_vals)):
        changes.append(bs_vals[i] - bs_vals[i-1])
    book_change[cat] = changes
    print(f"{cat:20s}: 도서 {bs_vals} → 변화 {changes}")

# 6. 상관관계 분석 (바이럴 지수 vs 도서 변화)
print("\n[바이럴 지수 vs 도서 수 변화 상관관계]")
print("-" * 70)

results = []
for cat in categories:
    viral = viral_data[cat]
    book_ch = book_change[cat]

    if sum([abs(x) for x in book_ch]) > 0:  # 변화가 있는 경우만
        corr, pval = stats.pearsonr(viral, book_ch)
        results.append({
            '카테고리': cat,
            '바이럴': viral,
            '도서변화': book_ch,
            '상관계수': round(corr, 3),
            'p-value': round(pval, 3)
        })
        print(f"{cat:20s}: r={corr:+.3f} (p={pval:.3f})")
    else:
        print(f"{cat:20s}: 도서 변화 없음")

print("\n" + "=" * 70)
print("주요 발견")
print("=" * 70)

# 바이럴 급등 카테고리 찾기
print("\n[바이럴 급등 TOP 3 (월별)]")
for i, m in enumerate(['10월', '11월', '12월']):
    sorted_cats = sorted(categories, key=lambda x: viral_data[x][i], reverse=True)
    print(f"{m}: {sorted_cats[0]} ({viral_data[sorted_cats[0]][i]:+.1f}), {sorted_cats[1]} ({viral_data[sorted_cats[1]][i]:+.1f}), {sorted_cats[2]} ({viral_data[sorted_cats[2]][i]:+.1f})")
