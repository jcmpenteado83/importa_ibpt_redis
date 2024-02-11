import csv
import json
import redis
import os
import glob
import re
import codecs


def extrair_versao_do_arquivo(nome_base_arquivo):
    # Use regex para extrair a versão
    padrao_versao = re.compile(r'TabelaIBPTax\w+([0-9]+\.[0-9]+\.[A-Za-z])\.csv')
    match = padrao_versao.search(nome_base_arquivo)
    
    if match:
        return match.group(1)
    else:
        return None

def converter_para_numero(valor):
    if valor:
        try:
            return float(valor.replace(',', '.'))  # Converter ',' para '.' e, em seguida, para float
        except ValueError:
            return valor  # Se a conversão falhar, retornar o valor original
    else:
        return 0  # Se estiver vazio, retornar 0

def processar_arquivo_csv(arquivo_csv, redis_conn, versao):
    mapeamento_campos = {
        'codigo': 'Codigo',
        'UF': 'UF',
        'ex': 'EX',
        'descricao': 'Descricao',
        'nacionalfederal': 'Nacional',
        'estadual': 'Estadual',
        'importadosfederal': 'Importado',
        'municipal': 'Municipal',
        'tipo': 'Tipo',
        'vigenciainicio': 'VigenciaInicio',
        'vigenciafim': 'VigenciaFim',
        'chave': 'Chave',
        'versao': 'Versao',
        'fonte': 'Fonte'
    }

    # Extrair a UF do nome do arquivo
    nome_base_arquivo = os.path.basename(arquivo_csv)
    uf_do_arquivo = nome_base_arquivo[len('TabelaIBPTax'):len('TabelaIBPTax')+2]

    with codecs.open(arquivo_csv, 'r', encoding='latin-1') as arquivo_csv:
        leitor_csv = csv.DictReader(arquivo_csv, delimiter=';')
        for linha in leitor_csv:
            linha['UF'] = uf_do_arquivo


            # Converter os campos para números
            campos_numericos = ['estadual', 'ex', 'importadosfederal', 'municipal', 'nacionalfederal']
            for campo in campos_numericos:
                linha[campo] = converter_para_numero(linha[campo])

            novo_dicionario = {json_nome: linha[csv_nome] for csv_nome, json_nome in mapeamento_campos.items()}
            chave_redis = f"{linha['codigo']}_{uf_do_arquivo}"
            json_data = json.dumps(novo_dicionario)
            print(json_data)
            redis_conn.set(chave_redis, json_data)

# Caminho do diretório onde estão os arquivos CSV
pasta_projetos = os.path.join(os.path.dirname(__file__), '')

# Padrão do nome dos arquivos CSV
padrao_nome_arquivo = os.path.join(pasta_projetos, 'TabelaIBPTax*.csv')

# Obter lista de arquivos correspondentes ao padrão
arquivos_csv = glob.glob(padrao_nome_arquivo)

# Conectar ao servidor Redis
redis_conn = redis.StrictRedis(host='172.17.0.2', port='6379', db=0)

# Iterar sobre os arquivos CSV
for arquivo_csv in arquivos_csv:
    nome_base_arquivo = os.path.basename(arquivo_csv)
    
    # Extrair a versão do nome do arquivo
    versao = extrair_versao_do_arquivo(nome_base_arquivo)
    
    if versao:
        processar_arquivo_csv(arquivo_csv, redis_conn, versao)
    else:
        print(f'Erro ao extrair a versão do arquivo: {arquivo_csv}')
