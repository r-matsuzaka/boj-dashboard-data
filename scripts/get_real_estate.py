import requests
from bs4 import BeautifulSoup
import os
import re
from pathlib import Path


def download_commercial_real_estate_index():
    """
    Download the commercial real estate price index Excel file from MLIT website.
    The file will be saved in the 'data' directory in the project root.
    """
    # URL of the webpage containing the Excel file link
    base_url = "https://www.mlit.go.jp"
    page_url = "https://www.mlit.go.jp/totikensangyo/totikensangyo_tk5_000085.html"
    
    # Get the project root directory and create the data path
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data"
    
    # Create the data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    output_file = data_dir / "commercial_real_estate_price_index.xlsx"
    
    try:
        # Step 1: Get the webpage content
        print(f"Accessing the webpage: {page_url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(page_url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Set the encoding to handle Japanese characters
        response.encoding = 'utf-8'
        
        # Step 2: Parse the HTML to find the Excel link for commercial real estate
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for table rows that contain "不動産価格指数（商業用不動産）"
        target_text = "不動産価格指数（商業用不動産）"
        excel_link = None
        
        # Find all table rows
        rows = soup.find_all('tr')
        for row in rows:
            # Check if this row contains the target text
            if target_text in row.text:
                # Look for Excel link in this row
                excel_anchor = row.find('a', string='Excel')
                if excel_anchor and excel_anchor.get('href'):
                    excel_link = excel_anchor.get('href')
                    break
        
        if not excel_link:
            print("Could not find the Excel link for commercial real estate price index.")
            return False
        
        # Step 3: Construct the full URL if it's a relative path
        if excel_link.startswith('/'):
            excel_url = base_url + excel_link
        elif excel_link.startswith('http'):
            excel_url = excel_link
        else:
            excel_url = base_url + '/' + excel_link
        
        print(f"Found Excel link: {excel_url}")
        
        # Step 4: Download the Excel file
        print(f"Downloading the Excel file...")
        file_response = requests.get(excel_url, headers=headers, stream=True)
        file_response.raise_for_status()
        
        # Step 5: Save the file
        with open(output_file, 'wb') as f:
            for chunk in file_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        file_size = os.path.getsize(output_file) / 1024  # Size in KB
        print(f"Successfully downloaded to {output_file} ({file_size:.2f} KB)")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False


def download_by_direct_url():
    """
    Alternative method to download the commercial real estate price index 
    by trying direct URLs if the main method fails.
    """
    # Get the project root directory and create the data path
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data"
    
    # Create the data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Based on the pattern observed from similar government websites
    possible_urls = [
        "https://www.mlit.go.jp/totikensangyo/totikensangyo_content/commercial_real_estate_index.xlsx",
        "https://www.mlit.go.jp/common/001285728.xlsx",
        "https://www.mlit.go.jp/totikensangyo/content/001285728.xlsx"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    for url in possible_urls:
        try:
            print(f"Trying direct URL: {url}")
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code == 200:
                # Check if it's actually an Excel file
                content_type = response.headers.get('Content-Type', '')
                if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or \
                   'application/vnd.ms-excel' in content_type or \
                   'application/octet-stream' in content_type:
                    
                    filename = data_dir / "commercial_real_estate_index_direct.xlsx"
                    with open(filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    print(f"Successfully downloaded to {filename}")
                    return True
                else:
                    print(f"URL returned non-Excel content: {content_type}")
            else:
                print(f"URL returned status code: {response.status_code}")
                
        except Exception as e:
            print(f"Error with URL {url}: {e}")
    
    return False


def main():
    """Main function to execute the download process."""
    print("Attempting to download the commercial real estate price index Excel file...")
    
    # Try the main method first
    if not download_commercial_real_estate_index():
        print("\nMain method failed. Trying alternative method...")
        if not download_by_direct_url():
            print("\nAll download attempts failed. Please check the website manually and update the script accordingly.")
        else:
            print("\nDownload successful using the alternative method!")
    else:
        print("\nDownload successful using the main method!")


if __name__ == "__main__":
    main()