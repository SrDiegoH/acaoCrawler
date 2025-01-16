import ast
from datetime import datetime, timedelta
from hashlib import sha512
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests
from requests import RequestException

app = Flask(__name__)
app.json.sort_keys = False

TRUE_BOOL_VALUES = ('1', 's', 'sim', 'y', 'yes', 't', 'true')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

FUNDAMENTUS_SOURCE = 'fundamentus'
INVESTIDOR10_SOURCE = 'investidor10'

VALID_INFOS = ['name','sector','link','price','liquidity','total_issued_shares','enterprise_value','equity_value','net_revenue','net_profit','net_margin','gross_margin','cagr_revenue','cagr_profit','debit','ebit','variation_12m','variation_30d','min_52_weeks','max_52_weeks','pvp','dy','latests_dividends','avg_annual_dividends','assets_value','market_value','pl','roe','payout']

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

        if 'R$' in text:
            text = text.replace('R$', '')

        return float(text.strip())
    except:
        return 0

def delete_cache():
    if os.path.exists(CACHE_FILE):
        #print('Deleting cache')
        os.remove(CACHE_FILE)
        #print('Deleted')

def clear_cache(hash_id):
    #print('Cleaning cache')
    with open(CACHE_FILE, 'w+') as cache_file:
        lines = cache_file.readlines()

        for line in lines:
            if not line.startswith(hash_id):
                cache_file.write(line)
   #print('Cleaned')

def read_cache(hash_id, should_clear_cache):
    if not os.path.exists(CACHE_FILE):
        return None, None

    if should_clear_cache:
        clear_cache(hash_id)
        return None, None

    control_clean_cache = False

    print('Reading cache')
    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(hash_id):
                continue

            _, cached_datetime, data = line.strip().split('#@#')

            cached_date = datetime.strptime(cached_datetime, '%Y-%m-%d %H:%M:%S')

            print(f'Found value: Date: {cached_datetime} - Data: {data}')
            if datetime.now() - cached_date <= CACHE_EXPIRY:
                print('Finished read')
                return ast.literal_eval(data), cached_date

            control_clean_cache = True
            break

    if control_clean_cache:
        clear_cache(hash_id)

    return None, None

def write_to_cache(hash_id, data):
    print('Writing cache')
    with open(CACHE_FILE, 'a') as cache_file:
        print(f'Writed value: {f'{hash_id}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n'}')
        cache_file.write(f'{hash_id}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n')
    print('Writed')

def convert_fundamentus_data(data, info_names):
    patterns_to_remove = [
      '<span class="txt">',
      '<span class="oscil">',
      '<font color="#F75D59">',
      '</td>',
      '<td class="data">',
      '<td class="data w1">',
      '<td class="data w2">',
      '<td class="data w3">',
      '<td class="data destaque w3">',
      '<a href="resultado.php?segmento='
    ]

    ALL_INFO = {
        'name': lambda: get_substring(data, 'Empresa</span>', '</span>', patterns_to_remove + [ get_substring(data, 'Tipo</span>', '</span>', patterns_to_remove) ]),
        'sector': lambda: get_substring(data, 'Subsetor</span>', '</a>', patterns_to_remove).split('>')[1],
        'link': lambda: 'https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx',
        'price': lambda: text_to_number(get_substring(data, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': lambda: text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>', patterns_to_remove)),
        'total_issued_shares': lambda: text_to_number(get_substring(data, 'Nro. Ações</span>', '</span>', patterns_to_remove)),
        'enterprise_value': lambda: text_to_number(get_substring(data, 'Valor da firma</span>', '</span>', patterns_to_remove)),
        'equity_value': lambda: text_to_number(get_substring(data, 'Patrim. Líq</span>', '</span>', patterns_to_remove)),
        'net_revenue': lambda: text_to_number(get_substring(data, 'Receita Líquida</span>', '</span>', patterns_to_remove)),
        'net_profit': lambda: text_to_number(get_substring(data, 'Lucro Líquido</span>', '</span>', patterns_to_remove)),
        'net_margin': lambda: text_to_number(get_substring(data, 'Marg. Líquida</span>', '</span>', patterns_to_remove)),
        'gross_margin': lambda: text_to_number(get_substring(data, 'Marg. Bruta</span>', '</span>', patterns_to_remove)),
        'cagr_revenue': lambda: None,
        'cagr_profit': lambda: None,
        'debit': lambda: text_to_number(get_substring(data, 'Dív. Líquida</span>', '</span>', patterns_to_remove)),
        'ebit': lambda: text_to_number(get_substring(data, '>EBIT</span>', '</span>', patterns_to_remove)),
        'variation_12m': lambda: text_to_number(get_substring(data, '12 meses</span>', '</font>', patterns_to_remove)),
        'variation_30d': lambda: text_to_number(get_substring(data, '30 dias</span>', '</font>', patterns_to_remove)),
        'min_52_weeks': lambda: text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>', patterns_to_remove)),
        'max_52_weeks': lambda: text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>', patterns_to_remove)),
        'pvp': lambda: text_to_number(get_substring(data, 'P/VP</span>', '</span>', patterns_to_remove)),
        'dy': lambda: text_to_number(get_substring(data, 'Div. Yield</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: None,
        'avg_annual_dividends': lambda: None,
        'assets_value': lambda: text_to_number(get_substring(data, 'Ativo</span>', '</span>', patterns_to_remove)),
        'market_value': lambda: text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>', patterns_to_remove)),
        'pl': lambda: text_to_number(get_substring(data, 'P/L</span>', '</span>', patterns_to_remove)),
        'roe': lambda: text_to_number(get_substring(data, 'ROE</span>', '</span>', patterns_to_remove)),
        'payout': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_fundamentus(ticker, info_names):
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

        #print(f'Converted Fundamentus data: {convert_fundamentus_data(html_page, info_names)}')
        return convert_fundamentus_data(html_page, info_names)
    except Exception as error:
        #print(f'Error on get Fundamentus data: {traceback.format_exc()}')
        return None

def convert_investidor10_ticker_data(html_page, json_dividends, info_names):
    def get_leatests_dividends(dividends):
        get_leatest_dividend = lambda dividends, year: next((dividend['price'] for dividend in dividends if dividend['created_at'] == year), None)

        current_year = datetime.now().year

        value = get_leatest_dividend(dividends, current_year)

        return value if value else get_leatest_dividend(dividends, current_year -1)

    get_detailed_value = lambda text: text_to_number(get_substring(text, 'detail-value">', '</div>')) if text else None

    patterns_to_remove = [
        '<div>',
        '</div>',
        '<div class="_card-body">',
        '<div class="value d-flex justify-content-between align-items-center"',
        '<span>',
        '<span class="value">',
        'style="margin-top: 10px; width: 100%; padding-right: 0px">'
    ]

    ALL_INFO = {
        'name': lambda: get_substring(html_page, 'name-company">', '<', patterns_to_remove),
        'sector':  lambda: get_substring(html_page, 'Segmento</span>', '</span>', patterns_to_remove),
        'link': lambda: None,
        'price': lambda: text_to_number(get_substring(html_page, 'Cotação</span>', '</span>', patterns_to_remove)),
        'liquidity': lambda: get_detailed_value(get_substring(html_page, 'Liquidez Média Diária</span>', '</span>')),
        'total_issued_shares': lambda: get_detailed_value(get_substring(html_page, 'Nº total de papeis</span>', '</span>')),
        'enterprise_value': lambda: get_detailed_value(get_substring(html_page, 'Valor de firma</span>', '</span>')),
        'equity_value': lambda: get_detailed_value(get_substring(html_page, 'Patrimônio Líquido</span>', '</span>')),
        'net_revenue': lambda: None,
        'net_profit': lambda: None,
        'net_margin': lambda: text_to_number(get_substring(html_page, 'lucro líquido / receita líquida&lt;/b&gt;&lt;br&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'gross_margin': lambda: text_to_number(get_substring(html_page, 'lucro bruto / receita líquida&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'cagr_revenue': lambda: text_to_number(get_substring(html_page, 'período de cinco anos atrás.&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'cagr_profit': lambda: text_to_number(get_substring(html_page, 'período equivalente de cinco anos atrás.&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'debit': lambda: get_detailed_value(get_substring(html_page, 'Dívida Líquida</span>', '</span>')),
        'ebit':  lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: None,
        'min_52_weeks': lambda: None,
        'max_52_weeks': lambda: None,
        'pvp': lambda: text_to_number(get_substring(html_page, 'P/VP</span>', '</span>', patterns_to_remove)),
        'dy': lambda: text_to_number(get_substring(html_page, 'DY</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: get_leatests_dividends(json_dividends),
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends) / len(json_dividends)) if json_dividends else None,
        'assets_value': lambda: get_detailed_value(get_substring(html_page, 'Ativos</span>', '</span>')),
        'market_value': lambda: get_detailed_value(get_substring(html_page, 'Valor de mercado</span>', '</span>')),
        'pl': lambda: text_to_number(get_substring(html_page, 'P/L</span>', '</span>', patterns_to_remove)),
        'roe': lambda: text_to_number(get_substring(html_page, 'lucro líquido / patrimônio líquido&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove)),
        'payout': lambda: text_to_number(get_substring(html_page, 'prov. pagos / lucro líquido&lt;/b&gt;&lt;/p&gt;"></i></span>', '</span>', patterns_to_remove))
    }

    final_data = { info: ALL_INFO[info]() for info in info_names}

    return final_data

def get_data_from_investidor10(ticker, info_names):
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

        json_dividends_data = {}
        if 'latests_dividends' in info_names or 'AVG_annual_dividends' in info_names:
          url = f'https://investidor10.com.br/api/dividendos/chart/{ticker}/3650/ano'
          response = request_get(url, headers)
          json_dividends_data = response.json()

        #print(f'Converted Investidor 10 data: {convert_investidor10_ticker_data(half_html_page, json_dividends_data)}')
        return convert_investidor10_ticker_data(half_html_page, json_dividends_data, info_names)
    except Exception as error:
        #print(f'Error on get Investidor 10 data: {traceback.format_exc()}')
        return None

def get_data_from_all_sources(ticker, info_names):
    data_fundamentus = get_data_from_fundamentus(ticker, info_names)
    #print(f'Data from Fundamentus: {data_fundamentus}')

    blank_fundamentus_info_names = [ info for info in info_names if not data_fundamentus.get(info, False) ]
    #print(f'Info names: {blank_fundamentus_info_names}')

    if data_fundamentus and not blank_fundamentus_info_names:
        return data_fundamentus

    data_investidor10 = get_data_from_investidor10(ticker, blank_fundamentus_info_names if blank_fundamentus_info_names else info_names)
    #print(f'Data from Investidor 10: {data_investidor10}')

    if not data_investidor10:
        return data_fundamentus

    return { **data_fundamentus, **data_investidor10 }

def request_shares(ticker, source, info_names):
    if source == FUNDAMENTUS_SOURCE:
        return get_data_from_fundamentus(ticker, info_names)
    elif source == INVESTIDOR10_SOURCE:
        return get_data_from_investidor10(ticker, info_names)

    return get_data_from_all_sources(ticker, info_names)

@app.route('/acao/<ticker>', methods=['GET'])
def get_acao_data(ticker):
    should_delete_cache = request.args.get('should_delete_cache', '0').lower() in TRUE_BOOL_VALUES
    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in TRUE_BOOL_VALUES
    should_use_cache = request.args.get('should_use_cache', '1').lower() in TRUE_BOOL_VALUES

    source = request.args.get('source', '').lower()

    info_names = request.args.get('info_names', '').lower().replace(' ', '').split(',')
    info_names = [ info for info in info_names if info in VALID_INFOS ]
    info_names = info_names if len(info_names) else VALID_INFOS

    print(f'Delete cache? {should_delete_cache}, Clear cache? {should_clear_cache}, Use cache? {should_use_cache}')
    print(f'Ticker: {ticker}, Source: {source}, Info names: {info_names}')

    if should_delete_cache:
        delete_cache()

    should_use_and_not_delete_cache = should_use_cache and not should_delete_cache

    if should_use_and_not_delete_cache:
        id = f'{ticker}{source}{info_names.sort()}'.encode('utf-8')
        hash_id = sha512().hexdigest()
        print(f'Cache Hash ID: {hash_id}, From values: {id}')

        cached_data, cache_date = read_cache(hash_id, should_clear_cache)

        if cached_data:
            print(f'Data from Cache: {cached_data}')
            return jsonify({'data': cached_data, 'source': 'cache', 'date': cache_date.strftime("%d/%m/%Y, %H:%M")}), 200

    data = request_shares(ticker, source, info_names)
    #print(f'Data from Source: {data}')

    if should_use_and_not_delete_cache and not should_clear_cache:
        write_to_cache(hash_id, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    is_debug = os.getenv('IS_DEBUG', False)
    app.run(debug=is_debug)
