from bs4 import BeautifulSoup
from requests_html import HTMLSession
import json
import logging
import os
import time
import urllib.request

# Set the logging level for the 'websockets' logger
logging.getLogger('websockets').setLevel(logging.WARNING)

# If you need to configure logging for requests-html as well
logging.getLogger('requests_html').setLevel(logging.WARNING)


# Setup Constant 
USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'

BASE_URL = "https://investors.sgx.com/_security-types/stocks/"
ALT_BASE_URL_1 = "https://investors.sgx.com/_security-types/reits/"
ALT_BASE_URL_2 = "https://investors.sgx.com/_security-types/businesstrusts/"

# Make the list of possible link
LINK_ARR = {
  "BASE_URL": BASE_URL, 
  "ALT_BASE_URL_1" : ALT_BASE_URL_1, 
  "ALT_BASE_URL_2" : ALT_BASE_URL_2
}
SYMBOL_LIST_MAP = {
  "C70" : "C09", # City Development
  '5TY' : "WJ9",  # Advanced System Automation
  "S51" : "5E2",  # Seatrium Ltd
  # "5WJ" : "5WJ",  # Supposed to be ada
  # "AXB" : "AXB",
  # "TCPD" : "", # T CP ALL TH SDR
  # "TATD" : "", # Airports of Thailand
  # "TEPD" : "", # PTT Exploration Production PCL DRC
  # "9G2" : "9G2", # Sam Holding
  # "OXMU" : "OXMU", # Prime US REIT
  "QSD" : "V7R" #Resources Global
}

def get_url(base_url: str, symbol: str) -> str:
  return f"{base_url}{symbol}"

def read_page(url: str):
  try:
    session = HTMLSession()
    response = session.get(url)
    response.html.render(sleep=1, timeout=5)

    soup = BeautifulSoup(response.html.html, "html.parser")
    return soup
  except Exception as e:
    print(f"Failed to open {url}: {e}")
    return None
  finally:
    session.close()
    print(f"Session in {url} is closed")

def scrap_stock_page(base_url, symbol: str, new_symbol: str):
  url = get_url(base_url, new_symbol)
  soup = read_page(url)

  industry = None
  sub_industry = None

  if (soup is not None):
    # Get Industry
    try:
      industry = soup.find("span", {"class": "widget-security-details-general-industry"}).get_text()
      if (industry is not None and len(industry) > 0): # Handling empty string or not found
        industry = industry.replace("Industry: ", "")
        industries = industry.split(",")
        industry = industries[0]
        sub_industry = industries[1]
      else:
        industry = None
    except:
      print(f"Failed to get Industry data from {url}")
      industry = None
    

    if (industry is not None and sub_industry is not None):
      print(f"Successfully scrap from {symbol} stock page")
    else:
      if (industry is None):
        print(f"Detected None type for Industry variable from {symbol} stock page")
      else: # sub_industry is None
        print(f"Detected None type for SubIndustry variable from {symbol} stock page")
    
    stock_data = dict()
    stock_data['symbol'] = symbol
    stock_data['sector'] = industry
    stock_data['sub_sector'] = sub_industry

    return stock_data
  else:
    print(f"None type of BeautifulSoup")
    stock_data = {
        "symbol" : symbol,
        "sector" : None,
        "sub_sector" : None,
      }
    return stock_data

def scrap_function_sg(symbol_list, process_idx):
  print(f"==> Start scraping from process P{process_idx}")
  all_data = []
  cwd = os.getcwd()
  start_idx = 0
  count = 0

  # Iterate in symbol list
  for i in range(start_idx, len(symbol_list)):
    attempt_count = 1
    symbol = symbol_list[i]

    # Check if symbol is in SYMBOL_LIST_MAP
    if (symbol in SYMBOL_LIST_MAP):
      new_symbol = SYMBOL_LIST_MAP[symbol]
    else:
      new_symbol = symbol

    if (symbol is not None):
      scrapped_data = {
        "symbol" : symbol,
        "sector" : None,
        "sub_sector" : None,
      }

      # Handling for page that returns None although it should not
      while (scrapped_data['sector'] is None and scrapped_data['sub_sector'] is None and attempt_count <= 3):
        
        # Iterate among possible URLs
        for key, base in LINK_ARR.items():
          print(f"Try scraping {symbol} using {key}")
          scrapped_data = scrap_stock_page(base, symbol, new_symbol)

          if (scrapped_data['sector'] is not None and scrapped_data['sub_sector'] is not None):
            break

        if (scrapped_data['sector'] is None and scrapped_data['sub_sector'] is None):
          print(f"Data not found! Retrying.. Attempt: {attempt_count}")
        attempt_count += 1

        if (attempt_count == 3 and scrapped_data['sector'] is None):
          print(f"Data for {symbol} is still None after all attempts!")

      all_data.append(scrapped_data)

    if (i % 10 == 0 and count != 0):
      print(f"CHECKPOINT || P{process_idx} {i} Data")
    
    count += 1
    time.sleep(0.2)
  
  # Save last
  filename = f"P{process_idx}_data_sgx.json"
  print(f"==> Finished data is exported in {filename}")
  file_path = os.path.join(cwd, "data", filename)

  # Save to JSON
  with open(file_path, "w") as output_file:
    json.dump(all_data, output_file, indent=2)

  return all_data


ADDITIONAL_BASE_URL = "https://www.tradingview.com/symbols/SGX-"

def scrap_stock_page_additional( symbol : str) -> dict :
  url = get_url(ADDITIONAL_BASE_URL, symbol)
  soup = read_page(url)

  data_dict = {
    "symbol" : symbol,
    "sector" : None,
    "sub_sector" : None
  }

  if (soup is not None):
    try:
      container = soup.find("div", {"data-container-name" : "company-info-id"})
      needed_data = container.findAll("a")
      sector = None
      sub_sector = None
      if (len(needed_data) > 1):
        sector = needed_data[0].get_text().replace(u'\xa0', u' ')
        sub_sector = needed_data[1].get_text().replace(u'\xa0', u' ')
      else:
        print(f"There is at least 2 data needed on {url}")
      
      data_dict['sector'] = sector
      data_dict['sub_sector'] = sub_sector

      return data_dict
    except:
      print(f"Failed to get data from {url}")
      return data_dict
  else:
    print(f"Detected None type for Beautifulsoup for {url}")
    return data_dict

def scrap_null_data_sg():
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  data_file_path = [os.path.join(data_dir,f'P{i}_data_sgx.json') for i in range(1,5)]


  # Iterate each file
  file_idx = 0
  for file_path in data_file_path:
    file_idx += 1

    f = open(file_path)
    all_data_list = json.load(f)
    null_list = []


    for i in range(len(all_data_list)):
      data = all_data_list[i]
      if (data['sector'] is None or data['sub_sector'] is None):
        null_list.append({"idx" : i, "data" : data})

    for null_data in null_list:
      symbol = null_data['data']['symbol']

      attempt = 1
      while ( attempt <= 3):
        data_dict = scrap_stock_page_additional(symbol)
        null_data['data'] = data_dict

        if (data_dict['sector'] is not None and data_dict['sub_sector'] is not None):
          print(f"Successfully get data for stock {symbol}")
          break
        else:
          print(f"Failed to get data from {symbol} on attempt {attempt}. Retrying...")
        attempt +=1

    # After done with each file
    for null_data in null_list:
      all_data_list[null_data['idx']] = null_data['data']
    
    # Save last
    filename = f"P{file_idx}_data_sgx.json"
    print(f"==> Finished data is exported in {filename}")
    file_path = os.path.join(cwd, "data", filename)

    # Save to JSON
    with open(file_path, "w") as output_file:
      json.dump(all_data_list, output_file, indent=2)
