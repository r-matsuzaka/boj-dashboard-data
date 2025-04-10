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

# ファイルをダウンロードする関数
def download_file(url, filename):
    """指定されたURLからファイルをダウンロードする関数"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.e-stat.go.jp/'
    }
    
    # セッションを作成
    session = requests.Session()
    
    # ファイルをダウンロード（最大3回まで試行）
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = session.get(url, headers=headers, stream=True, timeout=60)
            response.raise_for_status()
            
            file_path = os.path.join(data_dir, filename)
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    
            # ファイルサイズを確認
            file_size = os.path.getsize(file_path)
            if file_size < 1000:  # 1KB未満は不正なファイルの可能性
                print(f"警告: ダウンロードされたファイルのサイズが小さすぎます ({file_size} bytes)")
                if attempt < max_attempts - 1:
                    print(f"再試行します ({attempt+1}/{max_attempts})...")
                    time.sleep(5)
                    continue
                else:
                    raise Exception("ダウンロードされたファイルが不正です")
                    
            return file_path
            
        except Exception as e:
            if attempt < max_attempts - 1:
                wait_time = 5 * (attempt + 1)
                print(f"ダウンロードエラー: {e} - {wait_time}秒後に再試行します（{attempt+1}/{max_attempts}）")
                time.sleep(wait_time)
            else:
                print(f"ダウンロードエラー: {e} - 最大試行回数に達しました")
                raise
    
    return None

# 毎月勤労統計調査のデータをダウンロードする関数
def download_payroll_data():
    """e-Statから毎月勤労統計調査の長期時系列データをダウンロードする関数"""
    print("毎月勤労統計調査データのダウンロードを開始します...")
    
    # 統計表示ページのURL（長期時系列表）
    url = "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00450071&tstat=000001011791&cycle=7&tclass1=000001035519&stat_infid=000032189715"
    print(f"統計表示ページにアクセス: {url}")
    
    # ターゲットページの内容を取得（最大3回試行）
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            session = requests.Session()
            response = session.get(url, headers=headers, timeout=30)
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
    
    # Excelダウンロードリンクを検索
    excel_url = None
    
    # 方法1: クラス名で検索
    excel_links = soup.find_all('a', class_=['stat-dl_icon', 'download-button', 'excel'])
    
    # 方法2: URLパターンで検索
    if not excel_links:
        excel_links = soup.find_all('a', href=lambda href: href and 'file-download' in href)
    
    # 方法3: 画像タグで検索
    if not excel_links:
        excel_imgs = soup.find_all('img', alt=['EXCEL閲覧用', 'Excel閲覧用'])
        excel_links = [img.parent for img in excel_imgs if img.parent.name == 'a']
    
    if excel_links:
        # 最初に見つかったリンクを使用
        link = excel_links[0]
        href = link.get('href')
        if href:
            if href.startswith('/'):
                excel_url = base_url + href
            elif href.startswith('http'):
                excel_url = href
            else:
                excel_url = base_url + '/' + href
            print(f"ダウンロードリンクを見つけました: {excel_url}")
    
    # 方法4: URLから統計IDを抽出してダウンロードURLを構築
    if not excel_url:
        matches = url.split('stat_infid=')
        if len(matches) > 1:
            stat_infid = matches[1].split('&')[0]
            
            # 複数のfileKindを試す
            for file_kind in [0, 1, 4]:
                test_url = f"{base_url}/stat-search/file-download?statInfId={stat_infid}&fileKind={file_kind}"
                try:
                    test_resp = session.head(test_url, timeout=10)
                    if test_resp.status_code == 200 and int(test_resp.headers.get('Content-Length', 0)) > 1000:
                        excel_url = test_url
                        print(f"fileKind={file_kind} で有効なURLを見つけました")
                        break
                except:
                    continue
    
    if excel_url:
        # ファイル名を設定
        excel_filename = "毎月勤労統計調査.xlsx"
        excel_path = os.path.join(data_dir, excel_filename)
        
        print(f"Excelファイルをダウンロードしています: {excel_filename}")
        print(f"URL: {excel_url}")
        
        try:
            # Excelファイルをダウンロード
            excel_file = download_file(excel_url, excel_filename)
            print(f"ダウンロード完了: {excel_file}")
            return excel_file
            
        except Exception as e:
            print(f"ダウンロード中にエラーが発生しました: {e}")
            
            # 代替方法: 厚生労働省からの直接ダウンロードを試みる
            try:
                print("代替ソースからのダウンロードを試みます...")
                # 厚生労働省の毎月勤労統計調査長期時系列表のURL
                mhlw_url = "https://www.mhlw.go.jp/toukei/itiran/roudou/monthly/32/dl/30-1-lts.xls"
                excel_file = download_file(mhlw_url, excel_filename)
                print(f"代替ソースからダウンロード完了: {excel_file}")
                return excel_file
            except Exception as e2:
                print(f"代替ソースからのダウンロードも失敗しました: {e2}")
    else:
        print("ダウンロードリンクが見つかりませんでした")
        
        # 代替方法: 厚生労働省からの直接ダウンロードを試みる
        try:
            print("代替ソースからのダウンロードを試みます...")
            # 厚生労働省の毎月勤労統計調査長期時系列表のURL
            mhlw_url = "https://www.mhlw.go.jp/toukei/itiran/roudou/monthly/32/dl/30-1-lts.xls"
            excel_filename = "毎月勤労統計調査.xlsx"
            excel_file = download_file(mhlw_url, excel_filename)
            print(f"代替ソースからダウンロード完了: {excel_file}")
            return excel_file
        except Exception as e:
            print(f"代替ソースからのダウンロードも失敗しました: {e}")
    
    return None

# メイン実行部分
if __name__ == "__main__":
    try:
        # データをダウンロード
        excel_file = download_payroll_data()
        
        if excel_file:
            print(f"毎月勤労統計調査データを正常にダウンロードしました: {excel_file}")
        else:
            print("データの取得に失敗しました")
    except Exception as e:
        print(f"実行中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()