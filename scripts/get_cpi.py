import requests
from bs4 import BeautifulSoup
import os
import re
import pandas as pd
from datetime import datetime, timedelta
import time
import urllib.parse
import sys

# ベースURL
base_url = 'https://www.e-stat.go.jp'

# プロジェクトのルートディレクトリとデータディレクトリの設定
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
data_dir = os.path.join(project_root, "data")

# データディレクトリが存在しない場合は作成
os.makedirs(data_dir, exist_ok=True)

# 現在の年月から2か月前を計算する関数
def get_two_months_ago():
    today = datetime.now()
    first_day_of_month = today.replace(day=1)
    # 1か月前
    one_month_ago = first_day_of_month - timedelta(days=1)
    first_day_one_month_ago = one_month_ago.replace(day=1)
    # 2か月前
    two_months_ago = first_day_one_month_ago - timedelta(days=1)
    
    return two_months_ago.year, two_months_ago.month

# 特定の年月のURL生成関数
def generate_url(year, month):
    # e-Statの月コードは1月=11010301, 2月=11010302, ...
    month_code = f'1101030{month}'
    return f'https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00200573&tstat=000001150147&cycle=1&year={year}0&month={month_code}&tclass1=000001150149&result_back=1&tclass2val=0'

# ファイルをダウンロードする関数
def download_file(url, filename):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    file_path = os.path.join(data_dir, filename)
    with open(file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    return file_path

# 複数のシートをそれぞれCSVに変換する関数
def convert_excel_to_csv(excel_file, base_name):
    try:
        # Excelファイルを読み込む
        print(f"Excelファイルを読み込んでいます: {excel_file}")
        
        # 各シートを読み込んでCSVに変換
        xl = pd.ExcelFile(excel_file, engine='openpyxl')
        sheet_names = xl.sheet_names
        print(f"シート名一覧: {sheet_names}")
        
        csv_files = []
        
        for i, sheet_name in enumerate(sheet_names):
            print(f"シート '{sheet_name}' を処理中...")
            
            # シート読み込み
            df = xl.parse(sheet_name)
            
            # CSVファイル名を作成
            if len(sheet_names) > 1:
                sheet_suffix = f"_sheet{i+1}"
                if i == 0:
                    sheet_suffix = "_指数"  # 1枚目のシート（指数）
                elif i == 1:
                    sheet_suffix = "_前月比"  # 2枚目のシート（前月比）
                elif i == 2: 
                    sheet_suffix = "_前年同月比"  # 3枚目のシート（前年同月比）
            else:
                sheet_suffix = ""
                
            csv_filename = f"{base_name}{sheet_suffix}.csv"
            csv_path = os.path.join(data_dir, csv_filename)
            
            # CSVに変換して保存
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"CSVに変換しました: {csv_path}")
            
            csv_files.append(csv_path)
        
        return csv_files
        
    except Exception as e:
        print(f"Excel→CSV変換中にエラーが発生しました: {e}")
        return []

# メイン関数
def download_cpi_data(year=None, month=None):
    # 年月が指定されていない場合は2か月前を使用
    if year is None or month is None:
        year, month = get_two_months_ago()
    
    print(f"{year}年{month}月の消費者物価指数データ（中分類指数/全国/月次）を取得しています...")
    
    # URLを生成
    target_url = generate_url(year, month)
    print(f"アクセスするURL: {target_url}")
    
    # ターゲットページの内容を取得（最大3回試行）
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = requests.get(target_url, timeout=30)
            response.raise_for_status()
            break
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            if attempt < max_attempts - 1:
                wait_time = 5 * (attempt + 1)
                print(f"接続エラー: {e} - {wait_time}秒後に再試行します（{attempt+1}/{max_attempts}）")
                time.sleep(wait_time)
            else:
                print(f"接続エラー: {e} - 最大試行回数に達しました")
                return None
    
    # HTMLを解析
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Excelダウンロードリンクを検索（複数の方法）
    excel_url = None
    
    # 方法1: 表1-1の中分類指数を探す
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            # 表番号1-1を探す
            table_num = row.find('td', class_='stat-table_number')
            if table_num and '1-1' in table_num.text.strip():
                # Excelリンクを探す
                excel_links = row.find_all('a', class_='stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
                if excel_links:
                    excel_url = base_url + excel_links[0]['href']
                    print(f"表1-1からExcelリンクを見つけました")
                    break
        if excel_url:
            break
    
    # 方法2: すべてのExcelリンクから探す
    if not excel_url:
        # すべてのExcelダウンロードリンクを検索
        excel_links = soup.find_all('a', class_='stat-dl_icon stat-icon_0 stat-icon_format js-dl stat-download_icon_left')
        
        if excel_links:
            # 中分類指数のリンクを探す（テキストや親要素から判断）
            for link in excel_links:
                parent_row = link.find_parent('tr')
                if parent_row and ('中分類指数' in parent_row.text or '1-1' in parent_row.text):
                    excel_url = base_url + link['href']
                    print(f"中分類指数のExcelリンクを見つけました")
                    break
            
            # それでも見つからなければ、最初のリンクを使用
            if not excel_url and excel_links:
                excel_url = base_url + excel_links[0]['href']
                print(f"最初のExcelリンクを使用します")
    
    # 方法3: 2025年2月で動作した直接URL（最終手段）
    if not excel_url:
        # 月に基づいてIDを予測する
        base_id = "000040254992"  # 2025年2月の基準ID
        month_diff = month - 2  # 2月からの差分
        
        if month_diff != 0:
            print(f"直接リンクが見つからないため、月差分 {month_diff} を使用してIDを予測します")
            predicted_id = str(int(base_id) + month_diff).zfill(len(base_id))
        else:
            predicted_id = base_id
        
        excel_url = f"{base_url}/stat-search/file-download?statInfId={predicted_id}&fileKind=0"
        print(f"予測されたURL: {excel_url}")
    
    if excel_url:
        # ファイル名を設定
        excel_filename = f"CPI_{year}_{month:02d}_中分類指数_全国_月次.xlsx"
        base_csv_name = f"CPI_{year}_{month:02d}_中分類指数_全国_月次"
        excel_path = os.path.join(data_dir, excel_filename)
        
        print(f"Excelファイルをダウンロードしています: {excel_filename}")
        print(f"URL: {excel_url}")
        
        try:
            # Excelファイルをダウンロード
            excel_file = download_file(excel_url, excel_filename)
            print(f"ダウンロード完了: {excel_file}")
            
            # 複数のシートをCSVに変換
            print(f"すべてのシートをCSVに変換しています...")
            csv_files = convert_excel_to_csv(excel_file, base_csv_name)
            
            if csv_files:
                print(f"CSV変換完了。{len(csv_files)}個のCSVファイルを作成しました:")
                for csv_file in csv_files:
                    print(f"- {csv_file}")
                
                # 元のExcelファイルを削除（オプション）
                if len(csv_files) > 0:
                    try:
                        os.remove(excel_file)
                        print(f"元のExcelファイルを削除しました")
                    except Exception as e:
                        print(f"Excelファイル削除時にエラー: {e}")
                
                return csv_files
            else:
                print("CSV変換に失敗しました。Excelファイルをそのまま保持します。")
                return [excel_file]
        except Exception as e:
            print(f"ダウンロード中にエラーが発生しました: {e}")
    else:
        print("ダウンロードリンクが見つかりませんでした")
    
    return None

if __name__ == "__main__":
    # 引数の処理（オプションで年月を指定可能）
    target_year = None
    target_month = None
    
    if len(sys.argv) > 2:
        try:
            target_year = int(sys.argv[1])
            target_month = int(sys.argv[2])
            print(f"指定された年月: {target_year}年{target_month}月")
        except ValueError:
            print("引数の形式が正しくありません。整数の年と月を指定してください。")
            print("例: python get_cpi.py 2025 2")
            sys.exit(1)
    
    # データ取得
    file_paths = download_cpi_data(target_year, target_month)
    
    if file_paths:
        print(f"消費者物価指数データを正常に取得しました:")
        for file_path in file_paths:
            print(f"- {file_path}")
        
        # 前月比のCSVを強調表示
        for file_path in file_paths:
            if "_前月比" in file_path:
                print(f"\n★ 前月比のデータ: {file_path}")
    else:
        print("データの取得に失敗しました")