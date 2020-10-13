import sys
import argparse
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import csv
import glob
from pandas_profiling import ProfileReport
import re
import string
import numpy as np

##############################################################################################################################################################
################################################################# Portal Dados Abertos - CVM #################################################################
#################################################################  http://dados.cvm.gov.br/  #################################################################
"""
CKAN é a maior plataforma para portal de dados em software livre do mundo.

CKAN é uma solução completa e pronta para usar que torna os dados acessíveis e utilizáveis – ao prover ferramentas para simplificar a publicação, 
o compartilhamento, o encontro e a utilização dos dados (incluindo o armazenamento de dados e o provimento de robustas APIs de dados). CKAN está direcionado 
a publicadores de dados (governos nacionais e regionais, companhias e organizações) que querem tornar seus dados abertos e disponíveis.

CKAN é usado por governos e grupos de usuários em todo o mundo e impulsiona vários portais oficiais e da comunidade, incluindo portais governamentais locais, 
nacionais e internacionais, tais como o data.gov.uk do Reino Unido, o publicdata.eu da União Europeia, o dados.gov.br do Brasil, o portal do governo da 
Holanda, assim como sítios de cidades e municípios nos EUA, Reino Unido, Argentina, Finlândia e em outros lugares.

CKAN: http://ckan.org/ | http://ckan.org/tour/
Visão geral das funcionalidades: http://ckan.org/features/
"""
##############################################################################################################################################################
# use este comando para limpar o ambiente: os.system('cls' if os.name == 'nt' else 'clear')

def f_extract(df_fi, set_wd, file_load):
    """
    Extrai os dados da fonte pública - Comissão de Valores Mobiliários (CVM).
    """
    for path in range(0, 2):
        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])
        # Registra em memória as informações dos arquivos a partir da página >html< da fonte pública.
        df_fi_csv = []
        df_fi_data = requests.get(df_fi[path]).content
        df_fi_soup = BeautifulSoup(df_fi_data, 'html.parser')
        with open(file_load[path]+'.html', 'w', encoding='utf-8') as file:
            file.write(str(df_fi_soup))
        # Encontra e registra em lista o nome dos arquivos.
        for link in df_fi_soup.find_all('a'):
            df_fi_csv.append(link.get('href'))
        inf_cadastral_fi = [fi_file for fi_file in df_fi_csv if 'csv' in fi_file]
        # Salva todos os arquivos em seus respectivos diretórios.
        for files in range(0, len(inf_cadastral_fi)):
            df_fi_csv_url = df_fi[path]+inf_cadastral_fi[files]
            url_response = requests.get(df_fi_csv_url)
            url_content = url_response.content
            df_fi_csv_file = open(inf_cadastral_fi[files], 'wb')
            df_fi_csv_file.write(url_content)
            df_fi_csv_file.close()
            os.system('dir *csv* /b')
        os.system('dir *csv* /t > filelist.txt')

def f_transform(set_wd):
    """
    Aplica uma série de transformações aos dados extraídos para gerar o fluxo que será carregado.
    """
    # Define as variáveis do cadastro dos fundos de investimentos.
    var_list = ['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'CONDOM', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF']
    for path in range(0, 2):
        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])
        # Cria uma lista com os names dos arquivos com extenção CSV.
        files_list = glob.glob('*.csv')
        # Define um limite de 50MB para leitura dos arquivos da fonte pública.
        csv.field_size_limit(500000)
        # Lê cada arquivo da lista removendo as variáveis desnecessárias:
        for files in range(0, len(files_list)):
            if set_wd[path] == set_wd[0]:
                files_sample = dd.read_csv(files_list[files], sep=';', engine='python', quotechar='"', usecols=var_list, error_bad_lines=False)
                files_sample = files_sample.compute()
                files_sample.drop(files_sample[files_sample.SIT == 'CANCELADA'].index, inplace=True)
                files_sample.drop(files_sample[files_sample.SIT == 'FASE PRÉ-OPERACIONAL'].index, inplace=True)
                files_sample = files_sample.drop(columns=['SIT'])    
            else:
                files_sample = dd.read_csv(files_list[files], sep=';', engine='python', quotechar='"', error_bad_lines=False)
                files_sample = files_sample.compute()
                files_sample = files_sample.drop(columns=['VL_TOTAL','CAPTC_DIA', 'RESG_DIA'])
                files_sample['VL_QUOTA'] = files_sample['VL_QUOTA'].astype('float16')
                files_sample['VL_PATRIM_LIQ'] = files_sample['VL_PATRIM_LIQ'].astype('float32')
                files_sample['NR_COTST'] = files_sample['NR_COTST'].astype(np.uint16)
            # Remove campos vazios de cada variável.
            files_sample = files_sample.dropna(how='any', axis=0)
            # Remove os espaços em brancos da base de dados.
            files_sample = files_sample.applymap(lambda x: x.strip() if type(x)==str else x)
            # Salva o arquivo da base de dados, transformado e limpo, em seu respectivo diretório.
            files_sample.to_csv(files_list[files], sep=';', index=False, encoding='utf-8-sig')

def f_load(set_wd, file_load, file_pattern):
    """
    Carrega a base de dados transformada.
    """
    Client()
    for path in range(0, 2):
        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])
        for step in range(0, 5):
            if set_wd[path] == set_wd[0]:
                # Lê e concatena todos os arquivos CSV do diretório - fi_cad.
                files_list = glob.glob('*'+file_pattern[step]+'*.csv')
                var_list = ['CNPJ_FUNDO', 'DENOM_SOCIAL', 'CLASSE', 'CONDOM', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF']
                fi_cad = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list) 
                                    for files in files_list])
                # Remove linhas repetidas.
                fi_cad = fi_cad.drop_duplicates('CNPJ_FUNDO')
                fi_cad = fi_cad.compute()
                # Salva os arquivos concatenados em seu respectivo diretório.
                fi_cad.to_csv(file_load[path]+'_'+file_pattern[step]+'.csv', sep=';', index=False, encoding='utf-8-sig')
            else:
                # Lê e concatena todos os arquivos CSV do diretório - fi_diario.
                files_list = glob.glob('*'+file_pattern[step]+'*.csv')
                var_list = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']
                fi_diario = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', 
                                       usecols=var_list).astype({'VL_QUOTA': 'float16', 'VL_PATRIM_LIQ': 'float32', 'NR_COTST': np.uint16})
                                       for files in files_list])
                fi_diario = fi_diario.compute()
                # Salva os arquivos concatenados em seu respectivo diretório.
                fi_diario.to_csv(file_load[path]+'_'+file_pattern[step]+'.csv', sep=';', index=False, encoding='utf-8-sig')

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-extract', dest='extract', action='store_const', const=True, help='Call the f_extract function')
    parser.add_argument('-transform', dest='transform', action='store_const', const=True, help='Call the f_transform function')
    parser.add_argument('-load', dest='load', action='store_const', const=True, help='Call the f_load function')
    cmd_args = parser.parse_args()
    # Lista de constantes como parâmetros de entrada.
    df_fi = ['http://dados.cvm.gov.br/dados/FI/CAD/DADOS/', 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    file_load = ['fi_cad', 'fi_diario']
    file_pattern = ['2017', '2018', '2019', '2020', 'inf']
    # Define os argumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.extract: 
        f_extract(df_fi, set_wd, file_load)
    if cmd_args.transform: 
        f_transform(set_wd)
    if cmd_args.load:
        f_load(set_wd, file_load, file_pattern)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
