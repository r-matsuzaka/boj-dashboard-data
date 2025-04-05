import pandas as pd
import requests
import os
import time

def download_boj_price_index():
    # スクリプトの場所を基準とした相対パスを作成
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    
    # データディレクトリが存在しない場合は作成
    os.makedirs(data_dir, exist_ok=True)
    
    # CSVファイル名とパスを設定
    csv_filename = os.path.join(data_dir, "boj_corporate_price_index.csv")
    
    # URLを指定
    url = "https://www.stat-search.boj.or.jp/ssi/mtshtml/pr01_m_1.html"
    
    # テーブルデータを直接取得
    try:
        # テーブルを読み込む（Shift-JISエンコーディングを試す）
        print(f"URLからデータを取得中: {url}")
        tables = pd.read_html(url, encoding="shift-jis")
        
        if len(tables) > 0:
            # メインのデータテーブルを取得
            main_table = tables[0]
            
            # CSVとして保存
            main_table.to_csv(csv_filename, encoding='utf-8')
            print(f"データを保存しました: {csv_filename}")
            
            return main_table
        else:
            print("テーブルが見つかりませんでした")
    except Exception as e:
        print(f"テーブル取得エラー: {e}")
    
    # 代替手段: ダウンロードボタンをシミュレート
    try:
        print("代替手段を試行中: ダウンロードボタンをシミュレート")
        
        session = requests.Session()
        
        # Step 1: メインページにアクセス
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = session.get(url, headers=headers)
        
        # Step 2: CSVダウンロード用のURLを構築
        # 無担保コールレートと同様の構造と仮定
        download_url = "https://www.stat-search.boj.or.jp/ssi/cgi-bin/famecgi2?cgi=$nme_a000&lstSelection=PR01&exec=download&csv=pr01_m_1"
        
        # Step 3: CSVをダウンロード
        response = session.get(download_url, headers=headers)
        
        if response.status_code == 200:
            # バイナリとして保存
            with open(csv_filename, 'wb') as f:
                f.write(response.content)
            print(f"CSVファイルを保存しました: {csv_filename}")
            
            # ファイルサイズを確認
            file_size = os.path.getsize(csv_filename)
            print(f"ファイルサイズ: {file_size} バイト")
            
            # エンコーディングを推測してDataFrameとして読み込む
            encodings_to_try = ['shift-jis', 'cp932', 'euc-jp', 'utf-8']
            
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(csv_filename, encoding=encoding)
                    print(f"{encoding}エンコーディングで成功しました")
                    return df
                except Exception as e:
                    print(f"{encoding}エンコーディングでの読み込みに失敗: {e}")
            
            # すべてのエンコーディングで失敗した場合
            print("すべてのエンコーディングでファイルの解析に失敗しました")
            # ファイルの中身を確認（最初の100バイト）
            with open(csv_filename, 'rb') as f:
                print(f"ファイル先頭部分: {f.read(100)}")
        else:
            print(f"ダウンロード失敗: ステータスコード {response.status_code}")
                
    except Exception as e:
        print(f"代替手段も失敗しました: {e}")
    
    return None

if __name__ == "__main__":
    df = download_boj_price_index()
    if df is not None:
        print("\n取得したデータ（先頭5行）:")
        print(df.head())
        
        # データの形状も表示
        print(f"\nデータの形状: {df.shape}")
        print(f"列名: {df.columns.tolist()}")
    else:
        print("データが取得できませんでした。")