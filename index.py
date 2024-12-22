from datetime import datetime, timedelta
import http.client as httplib
import json
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests
from requests import RequestException

app = Flask(__name__)
app.json.sort_keys = False

TRUE_BOOL_VALUES = ('true', '1', 't', 'y', 'yes', 's', 'sim')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

FUNDAMENTUS_SOURCE = 'fundamentus'
INVESTIDOR10_SOURCE = 'investidor10'

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    #print(f'Response from {url} : {response}')

    return response

def get_substring(text, start_text, end_text, replace_by_paterns=[], should_remove_tags=False):
    start_index = text.find(start_text)
    new_text = text[start_index:]

    end_index = new_text[len(start_text):].find(end_text) + len(start_text)
    cutted_text = new_text[len(start_text):end_index]

    if not cutted_text:
        return None

    clean_text = cutted_text.replace('\n', '').replace('\t', '')

    no_tags_text = re.sub(r'<[^>]*>', '', clean_text) if should_remove_tags else clean_text

    final_text = no_tags_text
    for pattern in replace_by_paterns:
        final_text = final_text.replace(pattern, '')

    return final_text.strip()

def text_to_number(text, should_convert_thousand_decimal_separators=True, convert_percent_to_decimal=False):
    try:
        if not text:
            raise Exception()
    
        if not isinstance(text, str):
            return text

        text = text.strip()

        if not text.strip():
            raise Exception()

        if should_convert_thousand_decimal_separators:
            text = text.replace('.','').replace(',','.')

        if '%' in text:
            return float(text.replace('%', '').strip()) / (100 if convert_percent_to_decimal else 1)

        return float(text.strip())
    except:
        return 0

def delete_cache():
    if os.path.exists(CACHE_FILE):
        #print('Deleting cache')
        os.remove(CACHE_FILE)
        #print('Deleted')

def clear_cache(ticker):
    #print(f'Cleaning cache')
    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(ticker):
                cache_file.write(line)
   #print(f'Cleaned')

def read_cache(ticker, should_clear_cache):
    if not os.path.exists(CACHE_FILE):
        return None, None

    if should_clear_cache:
        clear_cache(ticker)
        return None, None

    control_clean_cache = False

    #print(f'Reading cache')
    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(ticker):
                continue

            _, cached_datetime, data = line.strip().split('#@#')

            cached_date = datetime.strptime(cached_datetime, '%Y-%m-%d %H:%M:%S')

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                #print(f'Finished read')
                return json.loads(data.replace("'", '"')), cached_date

            control_clean_cache = True
            break

    if control_clean_cache:
        clear_cache(ticker)

    return None, None

def write_to_cache(ticker, data):
    #print(f'Writing cache')
    with open(CACHE_FILE, 'a') as cache_file:
        cache_file.write(f'{ticker}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n')
    #print(f'Writed')

def convert_fundamentus_data(data):
    patterns_to_remove = [
        '</span>',
        '<span class="txt">',
        '<span class="oscil">',
        '</td>',
        '<td class="data">',
        '<td class="data w1">',
        '<td class="data w2">',
        '<td class="data w3">',
        '<td class="data destaque w3">',
        '<a href="resultado.php?segmento=',
        '<font color="#306EFF">',
        '<font color="#F75D59">'
    ]

    all_patterns_and_type_info = patterns_to_remove + [ get_substring(data, 'Tipo</span>', '</span>', patterns_to_remove) ]

    return {
        'name': get_substring(data, 'Empresa</span>', '</span>', all_patterns_and_type_info),
        'sector': get_substring(data, 'Subsetor</span>', '</a>', patterns_to_remove).split('>')[1],
        'link': None,
        'price': text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': text_to_number(get_substring(data, 'Nro. Ações</span>', '</span>', patterns_to_remove)),
        'enterprise_value': text_to_number(get_substring(data, 'Valor da firma</span>', '</span>', patterns_to_remove)),
        'equity_value': text_to_number(get_substring(data, 'Patrim. Líq</span>', '</span>', patterns_to_remove)),
        'net_revenue': text_to_number(get_substring(data, 'Receita Líquida</span>', '</span>', patterns_to_remove)),
        'net_profit': text_to_number(get_substring(data, 'Lucro Líquido</span>', '</span>', patterns_to_remove)),
        'net_margin': text_to_number(get_substring(data, 'Marg. Líquida</span>', '</span>', patterns_to_remove)),
        'gross_margin': text_to_number(get_substring(data, 'Marg. Bruta</span>', '</span>', patterns_to_remove)),
        'CAGR_revenue': None,
        'CAGR_profit': None,
        'debit': text_to_number(get_substring(data, 'Dív. Líquida</span>', '</span>', patterns_to_remove)),
        'EBIT': text_to_number(get_substring(data, '>EBIT</span>', '</span>', patterns_to_remove)),
        'variation_12M': text_to_number(get_substring(data, '12 meses</span>', '</font>', patterns_to_remove)),
        'variation_30D': text_to_number(get_substring(data, '30 dias</span>', '</font>', patterns_to_remove)),
        'min_52_weeks': text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        'PVP': text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'DY': text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'latests_dividends': None,
        'AVG_annual_dividends': None,
        'assets_value': text_to_number(get_substring(data, 'Ativo</span>', '</span>', patterns_to_remove)),
        'market_value': text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'PL': text_to_number(get_substring(data, 'P/L</span>', '</span>', patterns_to_remove)),
        'ROE': text_to_number(get_substring(data, 'ROE</span>', '</span>', patterns_to_remove)),
    }

def get_data_from_fundamentus_by(ticker):
    try:
        url = f'https://fundamentus.com.br/detalhes.php?papel={ticker}'
    
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://fundamentus.com.br/index.php',
            'Origin': 'https://fundamentus.com.br/index.php',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
        }
    
        response = request_get(url, headers)
        html_page = response.text

        #print(f"Converted Fundamentus data: {convert_fundamentus_data(html_page)}")

        return convert_fundamentus_data(html_page)
    except Exception as error:
        #print(f"Error on get Fundamentus data: {repr(error)}")
        return None

def calculate_AVG_dividends_annual(dividends):
    return sum(dividend['price'] for dividend in dividends) / len(dividends)

def get_leatests_dividends(dividends):
    current_year = datetime.now().year
    return next((dividend['price'] for dividend in dividends if dividend['created_at'] == current_year), None)

def convert_investidor10_ticker_data(html_page, json_data):
    patterns_to_remove = [
        '<span>',
        '<span class="value">',
        '</div>',
        '<div>',
        '<div class="_card-body">',
        '<div class="value d-flex justify-content-between align-items-center"',
        'style="margin-top: 10px; width: 100%; padding-right: 0px">',
        '<td class="column-value">'
    ]

    get_detailed_value = lambda text: text_to_number(get_substring(text, 'detail-value">', '</div>'))
    
    #sector_subsector = get_substring(html_page, '.br/setores/', '"', patterns_to_remove)
    #subsector = sector_subsector.split('/').replace('-', ' ')

    return {
        'name': get_substring(html_page, 'name-company">', '<', patterns_to_remove),
        'sector':  get_substring(html_page, 'Segmento</span>', '</span>', patterns_to_remove),
        'link': None,
        'price': text_to_number(get_substring(html_page, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': get_detailed_value(get_substring(html_page, 'Liquidez Média Diária</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': get_detailed_value(get_substring(html_page, 'Nº total de papeis</span>', '</span>', patterns_to_remove)),
        'enterprise_value': get_detailed_value(get_substring(html_page, 'Valor de firma</span>', '</span>', patterns_to_remove)),
        'equity_value': get_detailed_value(get_substring(html_page, 'Patrimônio Líquido</span>', '</span>', patterns_to_remove)),
        'net_revenue': get_detailed_value(get_substring(html_page, 'Receita Líquida - (R$)</td>', '</tr>', patterns_to_remove)),
        'net_profit': get_detailed_value(get_substring(html_page, 'Lucro Líquido - (R$)</td>', '</tr>', patterns_to_remove)),
        'net_margin': get_detailed_value(get_substring(html_page, 'Margem Líquida - (%)</td>', '</td>', patterns_to_remove)),
        'gross_margin': get_detailed_value(get_substring(html_page, 'Margem Bruta - (%)</td>', '</td>', patterns_to_remove)),
        'CAGR_revenue': text_to_number(get_substring(html_page, 'período de cinco anos atrás.&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'CAGR_profit': text_to_number(get_substring(html_page, 'período equivalente de cinco anos atrás.&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'debit': get_detailed_value(get_substring(html_page, 'Dívida Líquida</span>', '</span>', patterns_to_remove)),
        'EBIT': get_detailed_value(get_substring(html_page, 'EBIT - (R$)</td>', '</tr>', patterns_to_remove)),
        'variation_12M': text_to_number(get_substring(html_page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30D': None,
        'min_52_weeks': None,
        'max_52_weeks': None,
        'PVP': text_to_number(get_substring(html_page, 'P/VP</span>', '</span>', patterns_to_remove)),
        'DY': text_to_number(get_substring(html_page, 'DY</span>', '</span>', patterns_to_remove)),
        'latests_dividends': get_leatests_dividends(json_data),
        'AVG_annual_dividends': calculate_AVG_dividends_annual(json_data),
        'assets_value': get_detailed_value(get_substring(html_page, 'Ativos</span>', '</span>', patterns_to_remove)),
        'market_value': get_detailed_value(get_substring(html_page, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'PL': text_to_number(get_substring(html_page, 'P/L</span>', '</span>', patterns_to_remove)),
        'ROE': get_detailed_value(get_substring(html_page, 'ROE - (%)</td>', '</td>', patterns_to_remove)),
    }

def get_data_from_investidor10_by(ticker):
    try:
        headers = {
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/acoes/cmig4/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        }
    
        url = f'https://investidor10.com.br/acoes/{ticker}'
        response = request_get(url, headers)
    
        half_html_page = response.text[15898:]
    
        url = f'https://investidor10.com.br/api/dividendos/chart/{ticker}/3650/ano'
        response = request_get(url, headers)
        json_data = response.json()

        print(f"Converted Investidor 10 data: {convert_investidor10_ticker_data(half_html_page, json_data)}")
        return convert_investidor10_ticker_data(half_html_page, json_data)
    except Exception as error:
        print(f"Error on get Investidor 10 data: {traceback.format_exc()}")
        return None

def get_data_from_all_by(ticker):
    data_fundamentus = get_data_from_fundamentus_by(ticker)
    data_investidor10 = get_data_from_investidor10_by(ticker)

    if not data_fundamentus:
        return data_investidor10

    if not data_investidor10:
        return data_fundamentus

    data_merge = {}

    for key, value in data_fundamentus.items():
        if key in data_investidor10 and not value:
            data_merge[key] = data_fundsexplorer[key]
            continue

        data_merge[key] = value

    return data_merge

def request_shares_by(ticker, source):
    if source == FUNDAMENTUS_SOURCE:
        return get_data_from_fundamentus_by(ticker)
    elif source == INVESTIDOR10_SOURCE:
        return get_data_from_investidor10_by(ticker)

    return get_data_from_all_by(ticker)

@app.route('/acao/<ticker>', methods=['GET'])
def get_acao_data_by(ticker):
    should_delete_cache = request.args.get('should_delete_cache', '0').lower() in TRUE_BOOL_VALUES
    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in TRUE_BOOL_VALUES
    should_use_cache = request.args.get('should_use_cache', '1').lower() in TRUE_BOOL_VALUES

    source = request.args.get('source', 'all').lower()

    #print(f'Delete cache? {should_delete_cache}, Clear cache? {should_clear_cache}, Use cache? {should_use_cache}')
    #print(f'Ticker: {ticker}, Source: {source}')

    if should_delete_cache:
        delete_cache()

    if should_use_cache and not should_delete_cache:
        cached_data , cache_date = read_cache(ticker, should_clear_cache)

        if cached_data:
            #print(f'Data from Cache: {cached_data}')
            return jsonify({'data': cached_data, 'source': 'cache', 'date': cache_date.strftime("%d/%m/%Y, %H:%M")}), 200

    data = request_shares_by(ticker, source)
    #print(f'Data from Source: {data}')

    if should_use_cache and not should_delete_cache and not should_clear_cache:
        write_to_cache(ticker, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    is_debug = os.getenv('IS_DEBUG', False)
    app.run(debug=is_debug)
