#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
get_di.py - 内閣府ESRIウェブサイトからハイライトされたExcelファイルをダウンロードし、
データディレクトリに保存するスクリプト
"""

import os
import requests
from bs4 import BeautifulSoup
import re
import pathlib
import time

def main():
    # ベースURL
    base_url = "https://www.esri.cao.go.jp/jp/stat/di/di.html"
    
    # リクエスト用のセッションを作成
    session = requests.Session()
    # ユーザーエージェントを設定（一部のサイトではこれが必要）
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    print("内閣府ESRIウェブサイトにアクセスしています...")
    
    # メインページを取得
    response = session.get(base_url, headers=headers)
    response.encoding = 'utf-8'  # 日本語テキストを適切に処理するためにエンコーディングを設定
    
    if response.status_code != 200:
        print(f"ウェブサイトへのアクセスに失敗しました。ステータスコード: {response.status_code}")
        return
    
    # HTMLを解析
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # ターゲットファイルは「長期系列(CI指数, DI指数, DI景気指標)(Excel形式:58KB)」
    print("目的のExcelファイルを検索しています...")
    
    # すべてのリンクをデバッグ用に出力
    print("ページ上の全てのリンクを確認中:")
    excel_links = []
    for i, link in enumerate(soup.find_all('a')):
        link_text = link.get_text().strip()
        if 'Excel' in link_text and '長期系列' in link_text:
            excel_links.append((link_text, link.get('href')))
            print(f"{i+1}. 「{link_text}」-> {link.get('href')}")
    
    # リンクが見つからない場合
    if not excel_links:
        print("目的のExcelファイルがページ上で見つかりませんでした。")
        
        # 代替手段として、すべてのリンクを検索
        print("代わりに、すべてのExcelファイルリンクを検索します:")
        for i, link in enumerate(soup.find_all('a')):
            link_text = link.get_text().strip()
            href = link.get('href')
            if href and ('xls' in href.lower() or 'xlsx' in href.lower() or 'Excel' in link_text):
                print(f"{i+1}. 「{link_text}」-> {href}")
        return
    
    # 最初に見つかったリンクを使用
    target_text, target_link = excel_links[0]
    print(f"ターゲットリンクを見つけました: 「{target_text}」")
    
    # 相対URLを絶対URLに変換（必要な場合）
    if target_link.startswith('/'):
        target_link = f"https://www.esri.cao.go.jp{target_link}"
    elif not target_link.startswith('http'):
        # 元のURLからベースディレクトリを抽出
        base_dir = '/'.join(base_url.split('/')[:-1])
        target_link = f"{base_dir}/{target_link}"
    
    print(f"ファイルをダウンロードしています: {target_link}")
    
    # ファイルをダウンロード
    file_response = session.get(target_link, headers=headers, stream=True)
    
    if file_response.status_code != 200:
        print(f"ファイルのダウンロードに失敗しました。ステータスコード: {file_response.status_code}")
        return
    
    # Content-Dispositionヘッダーからファイル名を取得するか、デフォルト名を使用
    filename = "長期系列_CI指数_DI指数_DI景気指標.xlsx"
    if 'Content-Disposition' in file_response.headers:
        content_disposition = file_response.headers['Content-Disposition']
        matches = re.findall('filename="(.+)"', content_disposition)
        if matches:
            filename = matches[0]
    
    # dataディレクトリが存在することを確認
    data_dir = pathlib.Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # ファイルをdataディレクトリに保存
    file_path = data_dir / filename
    with open(file_path, 'wb') as f:
        for chunk in file_response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"ファイルを '{file_path}' に正常にダウンロードしました")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()