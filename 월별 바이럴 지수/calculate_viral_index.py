"""
경제뉴스 바이럴 지수 계산 스크립트

입력: 카테고리 분류된 뉴스 데이터 (Excel)
출력: 
  1. 2025_viral_index_top3.csv - 월별 TOP 3 바이럴 카테고리
  2. 2025_viral_index_matrix.csv - 전체 바이럴 지수 매트릭스

바이럴 지수 계산 방법:
  - MoM 증가율 (50%)
  - 3개월 이동평균 대비 편차 (30%)
  - Z-Score 표준화 (20%)
"""

import pandas as pd
import numpy as np

def calculate_viral_index(input_file, output_dir='./'):
    """
    바이럴 지수 계산 및 CSV 생성
    
    Parameters:
    -----------
    input_file : str
        카테고리 분류된 뉴스 데이터 파일 경로 (Excel)
    output_dir : str
        출력 파일 저장 경로 (기본값: 현재 디렉토리)
    
    Returns:
    --------
    viral_top3_df : DataFrame
        월별 TOP 3 바이럴 카테고리
    viral_matrix : DataFrame
        전체 바이럴 지수 매트릭스
    """
    
    print("="*100)
    print("🔥 바이럴 지수 계산 시작")
    print("="*100)
    
    # 1. 데이터 로드
    print("\n📂 데이터 로드 중...")
    df = pd.read_excel(input_file)
    
    # 미분류 제외
    df_classified = df[df['카테고리'] != '미분류'].copy()
    
    print(f"총 기사: {len(df):,}개")
    print(f"분류된 기사: {len(df_classified):,}개 ({len(df_classified)/len(df)*100:.1f}%)")
    
    # 2. 월별 카테고리 기사수 매트릭스 생성
    print("\n📊 월별 기사수 집계 중...")
    months = sorted(df_classified['년월'].unique())
    categories = sorted(df_classified['카테고리'].unique())
    
    monthly_counts = {}
    for month in months:
        month_data = df_classified[df_classified['년월'] == month]
        monthly_counts[month] = {}
        for cat in categories:
            cat_count = len(month_data[month_data['카테고리'] == cat])
            monthly_counts[month][cat] = cat_count
    
    # DataFrame 변환
    count_df = pd.DataFrame(monthly_counts).T
    
    print(f"집계 완료: {len(months)}개월 x {len(categories)}개 카테고리")
    
    # 3. 바이럴 지수 계산
    print("\n🔥 바이럴 지수 계산 중...")
    print("방법: MoM(50%) + MA편차(30%) + Z-Score(20%)")
    
    # 3-1. MoM (Month-over-Month) 증가율
    mom_growth = count_df.pct_change() * 100
    
    # 3-2. 3개월 이동평균 대비 증가율
    ma3 = count_df.rolling(window=3, min_periods=1).mean()
    ma_deviation = ((count_df - ma3) / ma3) * 100
    
    # 3-3. Z-Score (표준화)
    z_scores = (count_df - count_df.mean()) / count_df.std()
    
    # 3-4. 종합 바이럴 지수 (가중 평균)
    viral_index = (
        mom_growth.fillna(0) * 0.5 +      # MoM 50%
        ma_deviation.fillna(0) * 0.3 +    # MA편차 30%
        z_scores.fillna(0) * 20 * 0.2     # Z-score 20% (스케일 조정)
    )
    
    print("✅ 바이럴 지수 계산 완료!")
    
    # 4. 월별 TOP 3 추출
    print("\n🏆 월별 TOP 3 바이럴 카테고리 추출 중...")
    
    viral_results = []
    
    for month in months:
        if month not in viral_index.index:
            continue
        
        month_viral = viral_index.loc[month].sort_values(ascending=False)
        month_counts = count_df.loc[month]
        
        # TOP 3만 추출
        for rank, (cat, viral_score) in enumerate(month_viral.head(3).items(), 1):
            actual_count = month_counts[cat]
            
            # 전월 대비 계산
            month_idx = months.index(month)
            if month_idx > 0:
                prev_month = months[month_idx - 1]
                prev_count = count_df.loc[prev_month, cat]
                mom_change = ((actual_count - prev_count) / prev_count * 100) if prev_count > 0 else 0
                mom_str = f"{mom_change:+.1f}%"
            else:
                mom_str = "N/A"
            
            viral_results.append({
                '년월': month,
                '순위': rank,
                '카테고리': cat,
                '바이럴지수': round(viral_score, 1),
                '실제기사수': int(actual_count),
                '전월대비': mom_str,
                'MoM증가율': round(mom_growth.loc[month, cat], 1) if month in mom_growth.index else None,
                'MA대비편차': round(ma_deviation.loc[month, cat], 1) if month in ma_deviation.index else None,
                'Z-Score': round(z_scores.loc[month, cat], 2) if month in z_scores.index else None
            })
    
    viral_top3_df = pd.DataFrame(viral_results)
    
    # 5. CSV 저장
    print("\n💾 CSV 파일 저장 중...")
    
    top3_path = f"{output_dir}/2025_viral_index_top3.csv"
    matrix_path = f"{output_dir}/2025_viral_index_matrix.csv"
    
    viral_top3_df.to_csv(top3_path, index=False, encoding='utf-8-sig')
    viral_index.to_csv(matrix_path, encoding='utf-8-sig')
    
    print(f"✅ 저장 완료:")
    print(f"   - {top3_path}")
    print(f"   - {matrix_path}")
    
    # 6. 요약 통계
    print("\n" + "="*100)
    print("📊 바이럴 지수 요약")
    print("="*100)
    
    # 가장 자주 TOP3에 든 카테고리
    category_frequency = viral_top3_df['카테고리'].value_counts()
    print(f"\n🔥 가장 자주 '핫'했던 카테고리 (TOP3 진입 횟수):")
    for i, (cat, count) in enumerate(category_frequency.head(5).items(), 1):
        print(f"  {i}. {cat}: {count}회")
    
    # 최고 바이럴 지수
    max_viral = viral_top3_df.loc[viral_top3_df['바이럴지수'].idxmax()]
    print(f"\n🚀 역대 최고 바이럴 지수:")
    print(f"  {max_viral['년월']} - {max_viral['카테고리']}: {max_viral['바이럴지수']}")
    
    # 최저 바이럴 지수
    all_viral_values = []
    for month in viral_index.index:
        for cat in viral_index.columns:
            all_viral_values.append({
                '년월': month,
                '카테고리': cat,
                '바이럴지수': viral_index.loc[month, cat]
            })
    all_viral_df = pd.DataFrame(all_viral_values)
    min_viral = all_viral_df.loc[all_viral_df['바이럴지수'].idxmin()]
    print(f"\n❄️ 역대 최저 바이럴 지수:")
    print(f"  {min_viral['년월']} - {min_viral['카테고리']}: {min_viral['바이럴지수']:.1f}")
    
    print("\n" + "="*100)
    print("✅ 완료!")
    print("="*100)
    
    return viral_top3_df, viral_index


def main():
    """
    메인 실행 함수
    
    사용 예시:
    python calculate_viral_index.py
    """
    
    # 입력 파일 경로 (실제 경로로 수정 필요)
    input_file = 'news_2025_full_categorized.xlsx'
    
    # 출력 디렉토리
    output_dir = './'
    
    try:
        viral_top3, viral_matrix = calculate_viral_index(input_file, output_dir)
        
        # 결과 미리보기
        print("\n📋 결과 미리보기 (TOP 3):")
        print(viral_top3.head(10))
        
    except FileNotFoundError:
        print(f"❌ 오류: '{input_file}' 파일을 찾을 수 없습니다.")
        print("파일 경로를 확인해주세요.")
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
