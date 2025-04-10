import requests
import os
import urllib.parse

def download_estat_excel(url, output_dir=None, filename=None, max_attempts=3):
    """
    e-Statから統計データのExcelファイルをダウンロードする関数
    
    Args:
        url (str): e-Statの統計表示ページまたは直接ダウンロードリンクのURL
        output_dir (str, optional): 出力ディレクトリ
        filename (str, optional): 保存するファイル名
        max_attempts (int, optional): 最大試行回数
    
    Returns:
        str: ダウンロードされたファイルパス、失敗した場合はNone
    """
    # 出力ディレクトリの設定
    if output_dir is None:
        output_dir = os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    
    # URLが直接のダウンロードリンクか確認
    if "file-download" in url:
        download_url = url
        print(f"直接ダウンロードURLを使用: {download_url}")
    else:
        # 通常のURLから統計IDを抽出してダウンロードURLを構築
        print(f"統計表示ページURL: {url}")
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        # 統計IDを抽出
        stat_infid = None
        for key in ['stat_infid', 'statinfid', 'statInfId']:
            if key in query_params:
                stat_infid = query_params[key][0]
                print(f"URLから統計ID（{key}）を検出: {stat_infid}")
                break
        
        if not stat_infid:
            print("統計IDが見つかりませんでした。ダウンロードできません。")
            return None
        
        # 直接のダウンロードURLを構築 (fileKind=4がExcelファイルを指定)
        download_url = f"https://www.e-stat.go.jp/stat-search/file-download?statInfId={stat_infid}&fileKind=4"
    
    # ファイル名を設定
    if not filename:
        filename = "e-stat_data.xlsx"
    file_path = os.path.join(output_dir, filename)
    
    # リクエストヘッダの設定
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
        'Referer': 'https://www.e-stat.go.jp/',
    }
    
    # Excelファイルをダウンロード
    print(f"ダウンロードを開始: {download_url}")
    for attempt in range(max_attempts):
        try:
            session = requests.Session()
            response = session.get(download_url, headers=headers, stream=True, timeout=60)
            response.raise_for_status()
            
            # ファイルサイズを確認
            content_length = int(response.headers.get('Content-Length', 0))
            content_type = response.headers.get('Content-Type', '')
            content_disp = response.headers.get('Content-Disposition', '')
            
            print(f"Content-Type: {content_type}")
            print(f"Content-Disposition: {content_disp}")
            print(f"ファイルサイズ: {content_length / 1024:.1f} KB")
            
            if content_length > 1000:
                # ファイルを保存
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                print(f"ダウンロード完了: {file_path}")
                
                # ファイルサイズを確認
                file_size = os.path.getsize(file_path)
                if file_size > 1000:
                    print(f"保存されたファイルサイズ: {file_size / 1024:.1f} KB")
                    return file_path
                else:
                    print(f"警告: 保存されたファイルが小さすぎます ({file_size} bytes)")
                    continue
            else:
                print(f"警告: ダウンロードされるファイルが小さすぎます。次の試行へ進みます。")
                continue
                
        except Exception as e:
            if attempt < max_attempts - 1:
                wait_time = 5 * (attempt + 1)
                print(f"ダウンロードエラー: {e} - {wait_time}秒後に再試行します（{attempt+1}/{max_attempts}）")
                import time
                time.sleep(wait_time)
            else:
                print(f"ダウンロードエラー: {e} - 最大試行回数に達しました")
    
    print("ファイルのダウンロードに失敗しました。")
    return None

# メイン実行部分
if __name__ == "__main__":
    # 指定されたURL - 直接のダウンロードリンクを使用
    download_url = "https://www.e-stat.go.jp/stat-search/file-download?statInfId=000032189715&fileKind=4"
    
    # ダウンロード先ディレクトリ
    output_directory = os.path.join(os.getcwd(), "data")
    
    # ファイル名
    output_filename = "毎月勤労統計調査.xlsx"
    
    # ダウンロードを実行
    downloaded_file = download_estat_excel(
        url=download_url, 
        output_dir=output_directory, 
        filename=output_filename
    )
    
    if downloaded_file:
        print(f"ファイルが正常にダウンロードされました: {downloaded_file}")
    else:
        print("ファイルのダウンロードに失敗しました。")