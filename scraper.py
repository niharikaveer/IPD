import requests
from bs4 import BeautifulSoup
import sys
import re
import time
import html2text
import os
import zlib
import gzip

# optional brotli support
try:
    import brotli
    _HAS_BROTLI = True
except Exception:
    _HAS_BROTLI = False

# --- Configuration ---
base_url = "https://indiankanoon.org"
search_query = "divorce civil appeal doctypes: bombay"
headers = {
    'Cookie': 'sessionid=ae9b0w1kb6145m4i57yrpib8qnp5pw3g',
    'User-Agent': 'PostmanRuntime/7.37.3',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Host': 'indiankanoon.org'
}
start_page = 0
end_page = 5


def get_decoded_html(response):
    """
    Return decoded HTML (str) from a requests.Response object.
    Prefer response.text (requests' own decoding). Fall back to manual
    gzip/deflate/br handling if needed.
    """
    # Fast path: requests already decoded it
    try:
        txt = response.text
        if isinstance(txt, str) and ('<' in txt and '>' in txt):
            return txt
    except Exception:
        pass

    # Fallback: examine bytes
    content = response.content
    ce = (response.headers.get('Content-Encoding') or '').lower()

    # brotli
    if 'br' in ce:
        if not _HAS_BROTLI:
            raise RuntimeError("Server used brotli (br). Install 'brotli' (pip install brotli).")
        try:
            return brotli.decompress(content).decode(response.encoding or 'utf-8', errors='replace')
        except Exception:
            pass

    # gzip
    if 'gzip' in ce:
        try:
            return gzip.decompress(content).decode(response.encoding or 'utf-8', errors='replace')
        except Exception:
            pass

    # deflate (try zlib wrapper and raw)
    if 'deflate' in ce or True:
        for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
            try:
                decompressed = zlib.decompress(content, wbits)
                return decompressed.decode(response.encoding or 'utf-8', errors='replace')
            except zlib.error:
                continue

    # last resort: decode bytes directly forgiving errors
    return content.decode(response.encoding or 'utf-8', errors='replace')


def extract_numbers_from_page(page_num):
    """
    Extracts unique numbers from URLs on a given page of the search results.

    Args:
        page_num (int): The page number to fetch (0-indexed).

    Returns:
        list: A list of unique numbers extracted from the URLs, or an empty list on error.
    """
    target_url = f"{base_url}/search/?formInput={search_query}&pagenum={page_num}"
    print(f"Attempting to fetch URL: {target_url}")
    try:
        response = requests.get(target_url, headers=headers, timeout=10)
        response.raise_for_status()
        print("Successfully fetched HTML content.")
        html_content = get_decoded_html(response)
        soup = BeautifulSoup(html_content, 'html.parser')
        results_divs = soup.find_all('div', class_='result')
        if not results_divs:
            print(html_content)
            print(
                "No search results found on the page (could not find divs with class='result').")
            return []
        extracted_numbers = []
        print(f"\nExtracting unique numbers from URLs on Page {page_num}...")
        for result_div in results_divs[:10]:  # Limit to the top 10 results per page
            title_div = result_div.find('div', class_='result_title')
            if title_div:
                link_tag = title_div.find('a')
                if link_tag and link_tag.has_attr('href'):
                    full_url = base_url + link_tag['href']
                    match = re.search(r'/docfragment/(\d+)/', full_url)
                    if match:
                        unique_number = match.group(1)
                        extracted_numbers.append(unique_number)
        return extracted_numbers
    except requests.exceptions.Timeout as e:
        print(f"\nError: The request timed out: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"\nError: HTTP Error fetching URL: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"\nError: Could not connect to the URL: {e}")
    except requests.exceptions.RequestException as e:
        print(f"\nError: An error occurred during the request: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    return []


def html_to_text(url, output_folder="scrappedText"):
    """
    Fetches the HTML content from the given URL, converts it to plain text,
    and saves the text to a file in the specified folder.

    Args:
        url (str): The URL to fetch.
        output_folder (str, optional): The name of the folder to save the text file in.
            Defaults to "scrappedText".
    """
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html_content = get_decoded_html(response)
        h = html2text.HTML2Text()
        h.ignore_links = True
        text = h.handle(html_content)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        filename = f"{url.split('/')[-2]}.txt"
        filepath = os.path.join(output_folder, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Successfully saved text from {url} to {filepath}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
    except OSError as e:
        print(f"Error creating directory or writing file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    for page_num in range(start_page, end_page + 1):
        doc_ids = extract_numbers_from_page(page_num)
        if doc_ids:
            for doc_id in doc_ids:
                target_url = f"https://indiankanoon.org/doc/{doc_id}/"
                print(f"Processing URL: {target_url}")
                html_to_text(target_url)
                time.sleep(2)
            time.sleep(20)
        else:
            print(f"No document IDs found on page {page_num}.")
