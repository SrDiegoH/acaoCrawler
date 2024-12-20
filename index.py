import http.client as httplib
import json
import os
import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
import requests
from requests import RequestException

app = Flask(__name__)
app.json.sort_keys = False

TRUE_BOOL_VALUES = ('true', '1', 't', 'y', 'yes', 's', 'sim')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

FIIS_SOURCE = 'fiis'
FUNDAMENTUS_SOURCE = 'fundamentus'
FUNDSEXPLORER_SOURCE = 'fundsexplorer'

def request_fii_by(ticker, source):
    if source == FUNDAMENTUS_SOURCE:
        return get_data_from_fundamentus_by(ticker)
    elif source == FUNDSEXPLORER_SOURCE:
        return get_data_from_fundsexplorer_by(ticker)

    return get_data_from_all_by(ticker)

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
    
        return convert_fundamentus_data(response)
    except:
        return None

def convert_fundamentus_data(data):
    if not data:
        return None

    def generate_link(cnpj):
        return f'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo={cnpj}'

    cash = get_substring(data, 'Caixa\'', '}', False)
    vacancy = get_substring(data, 'Vacância Média</span>', '</span>')

    return {
        'nome': get_substring(data, 'Nome</span>', '</span>'),
        'gestao': get_substring(data, 'Gestão</span>', '</span>'),
        'tipo': None,
        'segmento': get_substring(data, 'Mandato</span>', '</span>'),
        'atuacao': None,
        'valor_caixa': get_substring(cash, '[', ']', False) if cash else None,
        'valor_ativos': text_to_number(get_substring(data, '>Ativos</span>', '</span>')),
        'valor_mercado': text_to_number(get_substring(data, 'Valor de mercado</span>', '</span>')),
        'valor_patrimonio_liquido': text_to_number(get_substring(data, 'Patrim Líquido</span>', '</span>')),
        'valor_cotacao': text_to_number(get_substring(data, 'Cotação</span>', '</span>')),
        'liquidez': text_to_number(get_substring(data, 'Vol $ méd (2m)</span>', '</span>')),
        'pvp': text_to_number(get_substring(data, 'P/VP</span>', '</span>')),
        'ffoy': text_to_number(get_substring(data, 'FFO Yield</span>', '</span>')),
        'dy': text_to_number(get_substring(data, 'Div. Yield</span>', '</span>')),
        'dividendos_12_meses': None,
        'ultimo_dividendo': text_to_number(get_substring(data, 'Dividendo/cota</span>', '</span>')),
        'valorizacao_12_meses': text_to_number(get_substring(data, '12 meses</span>', '</span>')),
        'valorizacao_ultimo_mes': text_to_number(get_substring(data, 'Mês</span>', '</span>')),
        'min_52_semanas': text_to_number(get_substring(data, 'Min 52 sem</span>', '</span>')),
        'max_52_semanas': text_to_number(get_substring(data, 'Max 52 sem</span>', '</span>')),
        'qnt_imoveis': text_to_number(get_substring(data, 'Qtd imóveis</span>', '</span>')),
        'vacancia': text_to_number(vacancy.replace('-', '')) if vacancy else None,
        'total_cotas_emitidas': text_to_number(get_substring(data, 'Nro. Cotas</span>', '</span>')),
        'data_inicio': None,
        'publico_alvo': None,
        'prazo': None,
        'link': generate_link(get_substring(data, 'abrirGerenciadorDocumentosCVM?cnpjFundo=', '">Pesquisar Documentos', False)),
        'vp_cota': text_to_number(get_substring(data, 'VP/Cota</span>', '</span>'))
    }

def get_data_from_fundsexplorer_by(ticker):
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'DNT': '1',
            'Priority': 'u=0, i',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0'
        }
    
        response = request_get(f'https://www.fundsexplorer.com.br/funds/{ticker}', headers)
    
        data_as_text = get_substring(response, 'var dataLayer_content', 'dataLayer.push')
    
        if not data_as_text:
            return None
    
        data_as_json = json.loads(data_as_text.strip(';= '))['pagePostTerms']['meta']
    
        return convert_fundsexplorer_data(data_as_json)
    except:
        return None

def convert_fundsexplorer_data(data):
    if not data:
        return None

    return {
        'nome': data['name'],
        'gestao': data['gestao'],
        'tipo': data['setor_atuacao'],
        'segmento': data['segmento_ambima'],
        'atuacao': data['segmento_atuacao'],
        'valor_caixa': data['valor_caixa'],
        'valor_mercado': data['valormercado'],
        'valor_patrimonio_liquido': data['patrimonio'],
        'valor_cotacao': data['valor'],
        'liquidez': data['liquidezmediadiaria'],
        'pvp': data['pvp'],
        'ffoy': None,
        'dy': data['dy'],
        'dividendos_12_meses': data['dividendos_12_meses'],
        'ultimo_dividendo': data['lastdividend'],
        'valorizacao_12_meses': data['valorizacao_12_meses'],
        'valorizacao_ultimo_mes': data['valorizacao_mes'],
        'min_52_semanas': data['min_52_semanas'],
        'max_52_semanas': data['max_52_semanas'],
        'qnt_imoveis': data['assets_number'],
        'vacancia': data['vacancia'],
        'total_cotas_emitidas': data['numero_cotas'],
        'data_inicio': data['firstdate'],
        'publico_alvo': data['publicoalvo'],
        'prazo': data['prazoduracao'],
        'link': None,
        'vp_cota': data['valorpatrimonialcota']
    }

def get_data_from_all_by(ticker):
    data_fundamentus = get_data_from_fundamentus_by(ticker)
    data_fundsexplorer = get_data_from_fundsexplorer_by(ticker)

    if not data_fundamentus:
        return data_fundsexplorer

    if not data_fundsexplorer:
        return data_fundamentus

    data_merge = {}

    for key, value in data_fundamentus.items():
        if key in data_fundsexplorer and not value:
            data_merge[key] = data_fundsexplorer[key]
            continue

        data_merge[key] = value

    return data_merge

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    #print(f'Response: {response}')

    return response.text

def get_substring(text, start_text, end_text, should_remove_tags=True, replace_by_paterns=[]):
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
    if not text:
        return 0

    try:
        if not isinstance(text, str):
            return text

        if should_convert_thousand_decimal_separators:
            text = text.replace('.','').replace(',','.')

        if '%' in text:
            return float(text.replace('%', '').strip()) / (100 if convert_percent_to_decimal else 1)

        return float(text.strip())
    except:
        return 0
    
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

def clear_cache(ticker):
    #print(f'Cleaning cache')
    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(ticker):
                cache_file.write(line)
   #print(f'Cleaned')

def write_to_cache(ticker, data):
    with open(CACHE_FILE, 'a') as cache_file:
        cache_file.write(f'{ticker}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n')

def delete_cache():
    if os.path.exists(CACHE_FILE):
        #print('Deleting cache')
        os.remove(CACHE_FILE)
        #print('Deleted')

@app.route('/fii/<ticker>', methods=['GET'])
def get_fii_data_by(ticker):
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

    data = request_fii_by(ticker, source)
    #print(f'Data from Source: {data}')

    if should_use_cache and not should_delete_cache and not should_clear_cache:
        write_to_cache(ticker, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    is_debug = os.getenv('IS_DEBUG', False)
    app.run(debug=is_debug)
