import ast
from datetime import datetime, timedelta
from hashlib import sha512
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

DEBUG_LOG_LEVEL = 'DEBUG'
ERROR_LOG_LEVEL = 'ERROR'
INFO_LOG_LEVEL = 'INFO'
LOG_LEVEL = os.environ.get('LOG_LEVEL', ERROR_LOG_LEVEL)

SEPARATOR = '#@#'

VALID_SOURCES = {
    'ALL_SOURCE': 'all',
    'CVM_SOURCE': 'cvm',
    'FUNDAMENTUS_SOURCE': 'fundamentus',
    'INVESTIDOR10_SOURCE': 'investidor10'
}

VALID_INFOS = [
    'assets_value',
    'avg_annual_dividends',
    'avg_price',
    'cagr_profit',
    'cagr_revenue',
    'debit',
    'dy',
    'ebit',
    'enterprise_value',
    'equity_value',
    'gross_margin',
    'latests_dividends',
    'latest_net_profit',
    'link',
    'liquidity',
    'market_value',
    'max_52_weeks',
    'mayer_multiple',
    'min_52_weeks',
    'name',
    'net_margin',
    'net_profit',
    'net_revenue',
    'payout',
    'pl',
    'price',
    'pvp',
    'roe',
    'roic',
    'sector',
    'total_issued_shares',
    'variation_12m',
    'variation_30d'
]

investidor_10_preloaded_data = (None, None)

app = Flask(__name__)
app.json.sort_keys = False

def log_error(message):
    if LOG_LEVEL == ERROR_LOG_LEVEL or LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {ERROR_LOG_LEVEL} - {message}')

def log_info(message):
    if LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {INFO_LOG_LEVEL} - {message}')

def log_debug(message):
    if LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {DEBUG_LOG_LEVEL} - {message}')

def cache_exists():
    if os.path.exists(CACHE_FILE):
        return True

    log_info('No cache file found')
    return False

def upsert_cache(id, data):
    lines = []
    updated = False

    if cache_exists():
        with open(CACHE_FILE, 'r') as cache_file:
            lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(id):
                cache_file.write(line)
                continue

            _, old_cached_date_as_text, old_data_as_text = line.strip().split(SEPARATOR)
            old_data = ast.literal_eval(old_data_as_text)

            combined_data = { **old_data, **data }
            updated_line = f'{id}{SEPARATOR}{old_cached_date_as_text}{SEPARATOR}{combined_data}\n'
            cache_file.write(updated_line)
            updated = True

        if not updated:
            new_line = f'{id}{SEPARATOR}{datetime.now().strftime(DATE_FORMAT)}{SEPARATOR}{data}\n'
            cache_file.write(new_line)
            log_info(f'New cache entry created for "{id}"')

    if updated:
        log_info(f'Cache updated for "{id}"')

def clear_cache(id):
    if not cache_exists():
        return

    log_debug('Cleaning cache')

    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        cache_file.writelines(line for line in lines if not line.startswith(id))

    log_info(f'Cache cleaning completed for "{id}"')

def read_cache(id):
    if not cache_exists():
        return None

    log_debug('Reading cache')

    clear_cache_control = False

    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(id):
                continue

            _, cached_date_as_text, data = line.strip().split(SEPARATOR)
            cached_date = datetime.strptime(cached_date_as_text, DATE_FORMAT)

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                log_debug(f'Cache hit for "{id}" (Date: {cached_date_as_text})')
                return ast.literal_eval(data)

            log_debug(f'Cache expired for "{id}" (Date: {cached_date_as_text})')
            clear_cache_control = True
            break

    if clear_cache_control:
        clear_cache(id)

    log_info(f'No cache entry found for "{id}"')
    return None

def delete_cache():
    if not cache_exists():
        return

    log_debug('Deleting cache')

    os.remove(CACHE_FILE)

    log_info('Cache deletion completed')

def preprocess_cache(id, should_delete_all_cache, should_clear_cached_data, should_use_cache):
    if should_delete_all_cache:
        delete_cache()
    elif should_clear_cached_data:
        clear_cache(id)

    can_use_cache = should_use_cache and not (should_delete_all_cache or should_clear_cached_data)

    return can_use_cache

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

        if 'R$' in text:
            text = text.replace('R$', '')

        return float(text.strip())
    except:
        return 0

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    log_debug(f'Response from {url} : {response}')

    return response

def convert_cvm_data(data, info_names):
    cvm_code = get_substring(data, 'dlCiasCdCVM$_ctl1$Linkbutton5&#39;,&#39;&#39;)">', '</a>')

    ALL_INFO = {
        'assets_value': lambda: None,
        'avg_annual_dividends': lambda: None,
        'avg_price': lambda: None,
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: None,
        'dy': lambda: None,
        'ebit': lambda: None,
        'enterprise_value': lambda: None,
        'equity_value': lambda: None,
        'gross_margin': lambda: None,
        'latests_dividends': lambda: None,
        'latest_net_profit': lambda: None,
        'link': lambda: f'https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx?tipoconsulta=CVM&codigoCVM={cvm_code}',
        'liquidity': lambda: None,
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: None,
        'net_margin': lambda: None,
        'net_profit': lambda: None,
        'net_revenue': lambda: None,
        'payout': lambda: None,
        'pl': lambda: None,
        'price': lambda: None,
        'pvp': lambda: None,
        'roe': lambda: None,
        'roic': lambda: None,
        'sector': lambda: None,
        'total_issued_shares': lambda: None,
        'variation_12m': lambda: None,
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_data_from_cvmweb(cnpj):
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 OPR/125.0.0.0'
        }

        response = request_get(f'https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CiaAb/ResultBuscaParticCiaAb.aspx?CNPJNome={cnpj}&TipoConsult=C', headers)
        html_body = response.text

        return html_body
    except:
        log_error(f'Error fetching CVM Code on CVM Web for "{cnpj}": {traceback.format_exc()}')
        return None

def get_cnpj_from_investidor10(ticker):
    global investidor_10_preloaded_data

    patterns_to_remove = [ '</td>', '<td class=\'value\'>' ]

    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,ko;q=0.7,es;q=0.6,fr;q=0.5',
            'Referer': 'https://investidor10.com.br/acoes/BBAS3',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 OPR/125.0.0.0'
        }

        response = request_get(f'https://investidor10.com.br/acoes/{ticker}', headers)
        html_cropped_body = response.text[15898:]

        cnpj = get_substring(html_cropped_body, 'CNPJ:', '</tr>', patterns_to_remove)

        if cnpj:
          investidor_10_preloaded_data = (ticker, html_cropped_body)

        return cnpj
    except:
        investidor_10_preloaded_data = (None, None)
        log_error(f'Error fetching CNPJ on Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def get_data_from_cvm(ticker, info_names):
    try:
        cnpj = get_cnpj_from_investidor10(ticker)

        if not cnpj:
            log_error(f'No CNPJ found for "{ticker}"')
            return None

        data = get_data_from_cvmweb(cnpj)

        converted_data = convert_cvm_data(data, info_names)
        log_debug(f'Converted CVM data: {converted_data}')
        return converted_data
    except:
        log_error(f'Error fetching data on CVM for "{ticker}": {traceback.format_exc()}')
        return None

def convert_fundamentus_data(data, historical_prices, info_names):
    patterns_to_remove = [
      '<span class="txt">',
      '<span class="oscil">',
      '<font color="#F75D59">',
      '<font color="#306EFF">',
      '</td>',
      '<td class="data">',
      '<td class="data w1">',
      '<td class="data w2">',
      '<td class="data w3">',
      '<td class="data destaque w3">',
      '<a href="resultado.php?segmento='
    ]

    prices = [ price[1] for price in historical_prices[-200:] ]
    avg_price = sum(prices) / len(prices)
    last_price = historical_prices[-1][1]

    def get_revenue():
        if 'Receita Líquida' in data:
            return text_to_number(get_substring(data, 'Receita Líquida</span>', '</span>', patterns_to_remove))
        return text_to_number(get_substring(data, 'Rec Serviços</span>', '</span>', patterns_to_remove)) + text_to_number(get_substring(data, 'Result Int Financ</span>', '</span>', patterns_to_remove))

    ALL_INFO = {
        'assets_value': lambda: text_to_number(get_substring(data, 'Ativo</span>', '</span>', patterns_to_remove)),
        'avg_annual_dividends': lambda: None,
        'avg_price': lambda: avg_price,
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: text_to_number(get_substring(data, 'Dív. Líquida</span>', '</span>', patterns_to_remove)),
        'dy': lambda: text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'ebit': lambda: text_to_number(get_substring(data, '>EBIT</span>', '</span>', patterns_to_remove)),
        'enterprise_value': lambda: text_to_number(get_substring(data, 'Valor da firma</span>', '</span>', patterns_to_remove)),
        'equity_value': lambda: text_to_number(get_substring(data, 'Patrim. Líq</span>', '</span>', patterns_to_remove)),
        'gross_margin': lambda: text_to_number(get_substring(data, 'Marg. Bruta</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: None,
        'latest_net_profit': lambda: None,
        'link': lambda: 'https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx',
        'liquidity': lambda: text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'market_value': lambda: text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': lambda: text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        #'max_52_weeks': lambda: max(prices),
        'mayer_multiple': lambda: last_price / avg_price,
        'min_52_weeks': lambda: text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        #'min_52_weeks': lambda: min(prices),
        'name': lambda: get_substring(data, 'Empresa</span>', '</span>', patterns_to_remove + [ get_substring(data, 'Tipo</span>', '</span>', patterns_to_remove) ]),
        'net_margin': lambda: text_to_number(get_substring(data, 'Marg. Líquida</span>', '</span>', patterns_to_remove)),
        'net_profit': lambda: text_to_number(get_substring(data, 'Lucro Líquido</span>', '</span>', patterns_to_remove)),
        'net_revenue': get_revenue,
        'payout': lambda: None,
        'pl': lambda: text_to_number(get_substring(data, 'P/L</span>', '</span>', patterns_to_remove)),
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        #'price': lambda: last_price,
        'pvp': lambda: text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'roe': lambda: text_to_number(get_substring(data, 'ROE</span>', '</span>', patterns_to_remove)),
        'roic': lambda: text_to_number(get_substring(data, 'ROIC</span>', '</span>', patterns_to_remove)),
        'sector': lambda: get_substring(data, 'Subsetor</span>', '</a>', patterns_to_remove).split('>')[1],
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'Nro. Ações</span>', '</span>', patterns_to_remove)),
        'variation_12m': lambda: text_to_number(get_substring(data, '12 meses</span>', '</font>', patterns_to_remove)),
        'variation_30d': lambda: text_to_number(get_substring(data, '30 dias</span>', '</font>', patterns_to_remove))
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_fundamentus(ticker, info_names):
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://fundamentus.com.br/index.php',
            'Origin': 'https://fundamentus.com.br/index.php',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0'
        }

        response = request_get(f'https://fundamentus.com.br/detalhes.php?papel={ticker}', headers)
        html_page = response.text

        response = request_get(f'https://www.fundamentus.com.br/amline/cot_hist.php?papel={ticker}', headers)
        historical_prices = response.json()

        converted_data = convert_fundamentus_data(html_page, historical_prices, info_names)
        log_debug(f'Converted Fundamentus data: {converted_data}')
        return converted_data
    except:
        log_debug(f'Error fetching data on Fundamentus for "{ticker}": {traceback.format_exc()}')
        return None

def convert_investidor10_data(page, dividends, historical_net_profit, info_names):
    patterns_to_remove = [
        '<div>',
        '</div>',
        '<div class="_card-body">',
        '<div class="value d-flex justify-content-between align-items-center"',
        '<p class="indicator-card-value">',
        '<span>',
        '<span class="value">',
        '<span id="company-average-value">',
        'style="margin-top: 10px; width: 100%; padding-right: 0px">'
    ]

    current_year = datetime.now().year
    dividends_has_current_year = any(dividend['created_at'] == current_year for dividend in dividends)

    get_detailed_value = lambda text: text_to_number(get_substring(text, 'detail-value">', '</div>')) if text else None

    def filter_historical_net_profit():
        years = sorted((int(year) for year in historical_net_profit.keys() if year.isdigit()))

        latest_years = years[-5:]

        latest_net_profit = { year: historical_net_profit[str(year)]["net_profit"] for year in latest_years }

        return latest_net_profit

    ALL_INFO = {
        'assets_value': lambda: get_detailed_value(get_substring(page, 'Ativos</span>', '</span>')),
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in dividends if dividend['created_at'] != current_year) / (len(dividends) -1 if dividends_has_current_year else len(dividends))) if dividends else None,
        'avg_price': lambda: None,
        'cagr_profit': lambda: text_to_number(get_substring(page, 'período equivalente de cinco anos atrás.&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'cagr_revenue': lambda: text_to_number(get_substring(page, 'período de cinco anos atrás.&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'debit': lambda: get_detailed_value(get_substring(page, 'Dívida Líquida</span>', '</span>')),
        'dy': lambda: text_to_number(get_substring(page, 'DY</span>', '</span>', patterns_to_remove)),
        'ebit':  lambda: None,
        'enterprise_value': lambda: get_detailed_value(get_substring(page, 'Valor de firma</span>', '</span>')),
        'equity_value': lambda: get_detailed_value(get_substring(page, 'Patrimônio Líquido</span>', '</span>')),
        'gross_margin': lambda: text_to_number(get_substring(page, 'lucro bruto / receita líquida&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: next((dividend['price'] for dividend in dividends if dividend['created_at'] == (current_year if dividends_has_current_year else current_year -1)), None) if dividends else None,
        'latest_net_profit': filter_historical_net_profit,
        'link': lambda: None,
        'liquidity': lambda: get_detailed_value(get_substring(page, 'Liquidez Média Diária</span>', '</span>')),
        'market_value': lambda: get_detailed_value(get_substring(page, 'Valor de mercado</span>', '</span>')),
        'max_52_weeks': lambda: None,
        'mayer_multiple': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: get_substring(page, 'name-company">', '<', patterns_to_remove),
        'net_margin': lambda: text_to_number(get_substring(page, 'lucro líquido / receita líquida&lt;/b&gt;&lt;br&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'net_profit': lambda: None,
        'net_revenue': lambda: None,
        'payout': lambda: text_to_number(get_substring(page, 'prov. pagos / lucro líquido&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'pl': lambda: text_to_number(get_substring(page, 'P/L</span>', '</span>', patterns_to_remove)),
        'price': lambda: text_to_number(get_substring(page, 'Cotação</span>', '</span>', patterns_to_remove)),
        'pvp': lambda: text_to_number(get_substring(page, 'P/VP</span>', '</span>', patterns_to_remove)),
        'roe': lambda: text_to_number(get_substring(page, 'lucro líquido / patrimônio líquido&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'roic': lambda: text_to_number(get_substring(page, 'EBIT / capital investido&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'sector':  lambda: get_substring(page, 'Segmento</span>', '</span>', patterns_to_remove),
        'total_issued_shares': lambda: get_detailed_value(get_substring(page, 'Nº total de papeis</span>', '</span>')),
        'variation_12m': lambda: text_to_number(get_substring(page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_investidor10(ticker, info_names):
    global investidor_10_preloaded_data

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://investidor10.com.br/acoes/cmig4/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    def get_investidor10_html_page():
        if investidor_10_preloaded_data[1] and ticker == investidor_10_preloaded_data[0]:
            log_debug(f'Using preloaded Investidor 10 data')
            return investidor_10_preloaded_data[1]

        url = f'https://investidor10.com.br/acoes/{ticker}'
        response = request_get(url, headers)
        html_page = response.text[15898:]

        log_debug(f'Using fresh Investidor 10 data')
        return html_page

    def get_investidor10_dividends():
        dividends = {}

        if 'latests_dividends' in info_names or 'avg_annual_dividends' in info_names:
          url = f'https://investidor10.com.br/api/dividendos/chart/{ticker}/3650/ano'
          response = request_get(url, headers)
          dividends = response.json()

        return dividends

    def get_investidor10_historical_prices():
        response = request_get(f'https://investidor10.com.br/api/cotacao-lucro/{ticker}/adjusted', headers)
        historical_net_profit = response.json()
        return historical_net_profit

    try:
        converted_data = convert_investidor10_data(get_investidor10_html_page(), get_investidor10_dividends(), get_investidor10_historical_prices(), info_names)
        log_debug(f'Converted Investidor 10 data: {converted_data}')
        return converted_data
    except:
        log_debug(f'Error fetching data on Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def filter_remaining_infos(data, info_names, default_info_names=None):
    if not data:
        return info_names

    missing_info = [ info for info in info_names if info in data and data[info] is None ]

    return missing_info if missing_info else default_info_names

def combine_data(first_dict, second_dict, info_names):
    if first_dict and second_dict:
        combined_dict = {**first_dict, **second_dict}
        log_debug(f'Data from combined Frist and Second Dictionaries: {combined_dict}')
    elif first_dict:
        combined_dict = first_dict
        log_debug(f'Data from First Dictionary only: {combined_dict}')
    elif second_dict:
        combined_dict = second_dict
        log_debug(f'Data from Second Dictionary only: {combined_dict}')
    else:
        combined_dict = {}
        log_debug('No combined data')

    missing_combined_infos = filter_remaining_infos(combined_dict, info_names)
    log_debug(f'Missing info from Combined data: {missing_combined_infos}')
    return combined_dict, missing_combined_infos

def get_data_from_all_sources(ticker, info_names):
    data_cvm = get_data_from_cvm(ticker, info_names)
    log_info(f'Data from CVM: {data_cvm}')

    missing_cvm_infos = filter_remaining_infos(data_cvm, info_names)
    log_debug(f'Missing info from CVM: {missing_cvm_infos}')

    if data_cvm and not missing_cvm_infos:
        return data_cvm

    data_fundamentus = get_data_from_fundamentus(ticker, missing_cvm_infos or info_names)
    log_info(f'Data from Fundamentus: {data_fundamentus}')

    combined_data, missing_combined_infos = combine_data(data_cvm, data_fundamentus, info_names)
    log_debug(f'Missing info from BM & FBovespa or Fundamentus: {missing_combined_infos}')

    if combined_data and not missing_combined_infos:
        return combined_data

    data_investidor_10 = get_data_from_investidor10(ticker, missing_combined_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return combined_data

    return { **combined_data, **data_investidor_10 }

def get_data_from_sources(ticker, source, info_names):
    SOURCES = {
        VALID_SOURCES['FUNDAMENTUS_SOURCE']: get_data_from_fundamentus,
        VALID_SOURCES['CVM_SOURCE']: get_data_from_cvm,
        VALID_SOURCES['INVESTIDOR10_SOURCE']: get_data_from_investidor10
    }

    fetch_function = SOURCES.get(source, get_data_from_all_sources)
    return fetch_function(ticker, info_names)

def get_data_from_cache(ticker, info_names, can_use_cache):
    if not can_use_cache:
        return None

    cached_data = read_cache(ticker)
    if not cached_data:
        return None

    filtered_data = { key: cached_data[key] for key in info_names if key in cached_data }
    log_info(f'Data from Cache: {filtered_data}')

    return filtered_data

def get_data(ticker, source, info_names, can_use_cache):
    cached_data = get_data_from_cache(ticker, info_names, can_use_cache)

    SHOULD_UPDATE_CACHE = True

    if not can_use_cache:
        return not SHOULD_UPDATE_CACHE, get_data_from_sources(ticker, source, info_names)

    missing_cache_info_names = filter_remaining_infos(cached_data, info_names)

    if not missing_cache_info_names:
        return not SHOULD_UPDATE_CACHE, cached_data

    source_data = get_data_from_sources(ticker, source, missing_cache_info_names)

    if cached_data and source_data:
        return SHOULD_UPDATE_CACHE, { **cached_data, **source_data }
    elif cached_data and not source_data:
        return not SHOULD_UPDATE_CACHE, cached_data
    elif not cached_data and source_data:
        return SHOULD_UPDATE_CACHE, source_data

    return not SHOULD_UPDATE_CACHE, None

def get_parameter_info(params, name, default=None):
    return params.get(name, default).replace(' ', '').lower()

def get_cache_parameter_info(params, name, default='0'):
    return get_parameter_info(params, name, default) in { '1', 's', 'sim', 't', 'true', 'y', 'yes' }

@app.route('/acao/<ticker>', methods=['GET'])
def get_acao_data(ticker):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_SOURCES.values() else VALID_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_data(ticker, source, info_names, can_use_cache)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, data)

    return jsonify(data), 200

if __name__ == '__main__':
    log_debug('Starting acaoCrawler API')
    app.run(debug=LOG_LEVEL == 'DEBUG')
