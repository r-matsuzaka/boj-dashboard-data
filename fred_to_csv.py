import pandas as pd
import requests
import os

def get_fred_data(series_id, api_key):
    """FRED APIから指定されたシリーズIDのデータを取得する"""
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json'
    
    response = requests.get(url)
    data = response.json()
    
    # DataFrameに変換
    df = pd.DataFrame(data['observations'])
    
    # 日付をdatetime型に、valueを数値型に変換（可能な場合）
    df['date'] = pd.to_datetime(df['date'])
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    return df

def process_data(df, frequency='quarterly'):
    """データを処理し、日付をYYYYMMDD形式に変換する"""
    # 元のデータを変更しないためにコピーを作成
    output_df = df.copy()
    
    # 日付をYYYYMMDD形式にフォーマット
    if frequency == 'quarterly':
        # 四半期データの場合、四半期の初日を使用
        output_df['formatted_date'] = output_df['date'].dt.strftime('%Y%m%d')
    elif frequency == 'annual':
        # 年次データの場合、その年の1月1日を使用
        output_df['formatted_date'] = output_df['date'].dt.strftime('%Y0101')
    
    # 必要な列のみを選択
    result = output_df[['formatted_date', 'value']].copy()
    
    # 欠損値がある行を削除（必要に応じて）
    result = result.dropna(subset=['value'])
    
    return result

def main():
    # 環境変数からAPIキーを取得
    api_key = os.environ.get('FRED_API_KEY')
    
    if not api_key:
        print("エラー: FRED_API_KEYが環境変数に設定されていません。")
        return
    
    # データディレクトリが存在しない場合は作成
    os.makedirs('data', exist_ok=True)
    
    # シリーズIDと対応する情報
    series_data = [
        {'id': 'GDP', 'name': 'us_gdp', 'frequency': 'quarterly'},
        {'id': 'JPNNGDP', 'name': 'japan_gdp', 'frequency': 'quarterly'},
        {'id': 'NYGDPMKTPCDWLD', 'name': 'world_gdp', 'frequency': 'annual'}
    ]
    
    # 各国・地域のデータを格納するためのディクショナリ
    all_data = {}
    
    # 各シリーズのデータを取得して処理
    for series in series_data:
        print(f"{series['name']}のデータを取得中...")
        
        try:
            df = get_fred_data(series['id'], api_key)
            processed_df = process_data(df, series['frequency'])
            
            # データをディクショナリに格納
            all_data[series['name']] = processed_df.set_index('formatted_date')['value']
            
        except Exception as e:
            print(f"{series['name']}の処理中にエラーが発生しました: {e}")
    
    # すべてのデータをマージして1つのDataFrameにする
    if all_data:
        # インデックスを基準にすべてのデータを結合
        merged_df = pd.DataFrame(all_data)
        
        # 列名を変更
        merged_df.columns = ['us_gdp', 'japan_gdp', 'world_gdp']
        
        # インデックス名を'date'に設定
        merged_df.index.name = 'date'
        
        # CSVに保存
        output_file = os.path.join('data', 'all_gdp_data.csv')
        merged_df.to_csv(output_file)
        print(f"すべてのGDPデータを{output_file}に保存しました")

if __name__ == "__main__":
    main()