"""
A standalone script to download and parse edgar 10k and 10q MDA sections
"""
import argparse
import pandas as pd
import itertools
import os
import os.path
import time
import re
import unicodedata
from collections import namedtuple
from functools import wraps
import glob
from sec_edgar_downloader import Downloader
import requests
from bs4 import BeautifulSoup

#Iterate through list of S&P 500 tickers
df = pd.read_csv('sp500_test.csv')
drop_column = df.columns[0]
df.drop(columns = drop_column, inplace = True)
company_index = df.copy()
mypath = (os.getcwd())

        item7={}
        item7[1]="item 7\. managements discussion and analysis"
        item7[2]="item 7\.managements discussion and analysis"
        item7[3]="item7\. managements discussion and analysis"
        item7[4]="item7\.managements discussion and analysis"
        item7[5]="item 7\. management discussion and analysis"
        item7[6]="item 7\.management discussion and analysis"
        item7[7]="item7\. management discussion and analysis"
        item7[8]="item7\.management discussion and analysis"
        item7[9]="item 7 managements discussion and analysis"
        item7[10]="item 7managements discussion and analysis"
        item7[11]="item7 managements discussion and analysis"
        item7[12]="item7managements discussion and analysis"
        item7[13]="item 7 management discussion and analysis"
        item7[14]="item 7management discussion and analysis"
        item7[15]="item7 management discussion and analysis"
        item7[16]="item7management discussion and analysis"
        item7[17]="item 7: managements discussion and analysis"
        item7[18]="item 7:managements discussion and analysis"
        item7[19]="item7: managements discussion and analysis"
        item7[20]="item7:managements discussion and analysis"
        item7[21]="item 7: management discussion and analysis"
        item7[22]="item 7:management discussion and analysis"
        item7[23]="item7: management discussion and analysis"
        item7[24]="item7:management discussion and analysis"

def create_parser():
    """Argument Parser"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--back_to_year', type=str, required=True,
                        help="year to start downloading from")
    parser.add_argument('-d', '--data_dir', type=str,
                        default="./data", help="path to save data")

    return parser


def main():
    
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args()

    # Download forms
    download_forms(company_index, args.back_to_year)

    # Parse HTML, normalize text, extract MD&A from 10Ks into df
    df_mda_text_10k = parse_mda_10k(company_index)
    
    # Parse HTML, normalize text, extract MD&A from 10Ks into df
    df_mda_text_10q = parse_mda_10q(company_index)
    
    df_doc_index = pd.concat([df_mda_text_10k, df_mda_text_10q], ignore_index=True)
    df_doc_index.reset_index(drop=True, inplace=True)
    
    # Pickle text dataframe
    df_doc_index.to_pickle('md&a_text_doc_index.pkl')
    
def timeit(f):
    @wraps(f)
    def wrapper(*args, **kw):
        start_time = time.time()
        result = f(*args, **kw)
        end_time = time.time()
        print("{} took {:.2f} seconds."
              .format(f.__name__, end_time-start_time))
        return result
    return wrapper

@timeit
def download_forms(company_index, year: str):
    """ Reads index file and downloads 10Ks and 10Qs 
    """
    dl = Downloader(mypath+'/data/')
    
    for i in company_index.index:
        
        dl.get("10-K", company_index.TICKER[i], after_date=year+'0101', include_amends=False)
        dl.get("10-Q", company_index.TICKER[i], after_date=year+'0101', include_amends=False)

def normalize_text(text):
    """Normalize Text
    """
    text = unicodedata.normalize("NFKD", text)  # Normalize
    text = '\n'.join(text.splitlines())  # Unicode break lines

    # Convert to upper
    text = text.upper()  # Convert to upper

    # Take care of breaklines & whitespaces combinations due to beautifulsoup parsing
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # To find MDA section, reformat item headers
    text = text.replace('\n.\n', '.\n')  # Move Period to beginning

    text = text.replace('\nI\nTEM', '\nITEM')
    text = text.replace('\nITEM\n', '\nITEM ')
    text = text.replace('\nITEM  ', '\nITEM ')

    text = text.replace(':\n', '.\n')

    # Math symbols for clearer looks
    text = text.replace('$\n', '$')
    text = text.replace('\n%', '%')

    # Reformat
    text = text.replace('\n', '\n\n')  # Reformat by additional breakline

    return text

def find_mda_from_10k(text, start=0):
    """Find MDA section from normalized text
    Args:
        text (str)s
    """
    mda = ""
    end = 0

    # Define start & end signal for parsing
    item7_begins = ['\nITEM 7.', '\nITEM 7 –', '\nITEM 7:', '\nITEM 7 ', '\nITEM 7\n'
                    '\nITEM7']
    item7_ends = ['\nITEM 7A']
    if start != 0:
        item7_ends.append('\nITEM 7')  # Case: ITEM 7A does not exist
    item8_begins = ['\nITEM 8']
    """
    Parsing code section
    """
    text = text[start:]

    # Get begin
    for item7 in item7_begins:
        begin = text.find(item7)
        if begin != -1:
            break

    if begin != -1:  # Begin found
        for item7A in item7_ends:
            end = text.find(item7A, begin + 1)
            if end != -1:
                break

        if end == -1:  # ITEM 7A does not exist
            for item8 in item8_begins:
                end = text.find(item8, begin + 1)
                if end != -1:
                    break

        # Get MDA
        if end > begin:
            mda = text[begin:end].strip()
        else:
            end = 0

    return mda, end

def find_mda_from_10q(text, start=0):
    """Find MDA section from normalized text
    Args:
        text (str)s
    """
    mda = ""
    end = 0

    # Define start & end signal for parsing
    item2_begins = [
        '\nITEM 2.', '\nITEM 2 –', '\nITEM 2:', '\nITEM 2 ', '\nITEM 2\n'
    ]
    item2_ends = ['\nITEM 2A']
    if start != 0:
        item2_ends.append('\nITEM 2')  # Case: ITEM 2A does not exist
    item3_begins = ['\nITEM 3']
    """
    Parsing code section
    """
    text = text[start:]

    # Get begin
    for item2 in item2_begins:
        begin = text.find(item2)
        if begin != -1:
            break

    if begin != -1:  # Begin found
        for item2A in item2_ends:
            end = text.find(item2A, begin + 1)
            if end != -1:
                break

        if end == -1:  # ITEM 2A does not exist
            for item3 in item3_begins:
                end = text.find(item3, begin + 1)
                if end != -1:
                    break

        # Get MDA
        if end > begin:
            mda = text[begin:end].strip()
        else:
            end = 0

    return mda, end

def parse_mda_10q(company_index):
    """ Parses text from html from 10Qs with BeautifulSoup
    """
    df_mda_text_10q = pd.DataFrame()
    for i in company_index.index:
        path = mypath+f'/dl/sec_edgar_filings/{company_index.TICKER[i]}/10-Q/'
        file_list_Q = glob.glob(mypath+f'/data/sec_edgar_filings/{company_index.TICKER[i]}/10-Q/*.txt')
        for file_name in file_list_Q:
            with open(file_name) as html_file:
                soup = BeautifulSoup(open(file_name), 'lxml')
                text = soup.get_text()
                
                # Normalize text
                text = normalize_text(text)
                
                # Parse MDA
                mda, end = find_mda_from_10q(text)
                
                # Parse second time if first parse results in index
                if mda and len(mda.encode('utf-8')) < 1000:
                    mda, _ = find_mda_from_10q(text, start=end)
                
                date_string = ''
                
                for text_line in text:
                    if 'FILED AS OF DATE' in text_line:
                        date_string = text_line
                        break
                
                #filename = f'{company_index.TICKER[i]}-10Q-MD&A-{date_string}.txt'

                #with open(os.path.join(path, filename), 'w+', encoding='utf-8') as f:
                    #f.write(filing_string)

                index_row = {'TICKER':company_index.TICKER[i], 'FILING_TYPE':'10Q', 'FILING_DATE':date_string,
                             'MD&A_PATH':'', 'MD&A_TEXT':mda}
                df_mda_text_10q = df_mda_text_10q.append(index_row, ignore_index=True)
                
    return df_mda_text_10q
    

def parse_mda_10k(company_index):
    """ Parses text from html from 10Ks with BeautifulSoup
    """
    df_mda_text_10k = pd.DataFrame()
    for i in company_index.index:
        path = mypath+f'/dl/sec_edgar_filings/{company_index.TICKER[i]}/10-K/'
        file_list_K = glob.glob(mypath+f'/data/sec_edgar_filings/{company_index.TICKER[i]}/10-K/*.txt')
        for file_name in file_list_K:
            with open(file_name) as html_file:
                soup = BeautifulSoup(open(file_name), 'lxml')
                text = soup.get_text()
                
                # Normalize text
                text = normalize_text(text)
                
                # Parse MDA
                mda, end = find_mda_from_10k(text)
                
                # Parse second time if first parse results in index
                if mda and len(mda.encode('utf-8')) < 1000:
                    mda, _ = find_mda_from_10k(text, start=end)
                
                date_string = ''
                
                for text_line in text:
                    if 'FILED AS OF DATE' in text_line:
                        date_string = text_line
                        break
                
                #filename = f'{company_index.TICKER[i]}-10K-MD&A-{date_string}.txt'

                #with open(os.path.join(path, filename), 'w+', encoding='utf-8') as f:
                    #f.write(filing_string)
                
                index_row = {'TICKER':company_index.TICKER[i], 'FILING_TYPE':'10K', 'FILING_DATE':date_string,
                             'MD&A_PATH':'', 'MD&A_TEXT':mda}
                df_mda_text_10k = df_mda_text_10k.append(index_row, ignore_index=True)
    return df_mda_text_10k

if __name__ == "__main__":
    main()
