import sys
import argparse
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import dask.dataframe as dd
import glob
import csv
import numpy as np
from pandas_profiling import ProfileReport

def f_extract(df_fi, set_wd, len_count):
    """
    Extrai os dados da fonte pública - Comissão de Valores Mobiliários (CVM).
    """
    for path in range(0, 2):

        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        # Registra em memória as informações dos arquivos a partir da página html da fonte pública.
        df_fi_csv = []
        df_fi_data = requests.get(df_fi[path]).content
        df_fi_soup = BeautifulSoup(df_fi_data, 'html.parser')

        # Encontra e registra em lista o nome dos arquivos.
        for link in df_fi_soup.find_all('a'):
            df_fi_csv.append(link.get('href'))
        inf_cadastral_fi = df_fi_csv[len(df_fi_csv)-(len_count[path]):(len(df_fi_csv)-2)]

        # Salva todos os arquivos em seus respectivos diretórios.
        for files in range(0, len(inf_cadastral_fi)):
            df_fi_csv_url = df_fi[path]+inf_cadastral_fi[files]
            url_response = requests.get(df_fi_csv_url)
            url_content = url_response.content
            df_fi_csv_file = open(inf_cadastral_fi[files], 'wb')
            df_fi_csv_file.write(url_content)
            df_fi_csv_file.close()

def f_transform(set_wd, year):
    """
    Aplica uma série de transformações aos dados extraídos para gerar o fluxo que será carregado.
    """
    for path in range(0, 2):

        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        # Cria uma lista com os names dos arquivos com extenção CSV.
        files_list = glob.glob('*'+year[path]+'*.csv')

        # Define um limite de 50MB para leitura dos arquivos da fonte pública.
        csv.field_size_limit(500000)

        for files in range(0, len(files_list)):
            # Lê cada arquivo da lista removendo as variáveis desnecessárias.
            if set_wd[path] == set_wd[0]:
                files_sample = dd.read_csv(files_list[files], sep=';', engine='python', quotechar='"', error_bad_lines=False)
                files_sample = files_sample.drop(columns=['DT_REG', 'DT_CONST', 'DT_CANCEL', 'DT_INI_SIT', 'DT_INI_ATIV', 'DT_INI_CLASSE', 'RENTAB_FUNDO',
                                                          'TRIB_LPRAZO', 'TAXA_PERFM', 'VL_PATRIM_LIQ', 'DT_PATRIM_LIQ', 'DIRETOR', 'CNPJ_ADMIN', 'ADMIN',
                                                          'PF_PJ_GESTOR', 'CPF_CNPJ_GESTOR', 'GESTOR'])
                files_sample = files_sample.compute()

                # Remove subitens desnecessário considerando particularidades de algumas variável.
                files_sample.drop(files_sample[files_sample.SIT == 'CANCELADA'].index, inplace=True)
                files_sample.drop(files_sample[files_sample.SIT == 'FASE PRÉ-OPERACIONAL'].index, inplace=True)
                files_sample.drop(files_sample[files_sample.CONDOM == 'Fechado'].index, inplace=True)
                files_sample.drop(files_sample[files_sample.FUNDO_EXCLUSIVO == 'S'].index, inplace=True)
                files_sample.drop(files_sample[files_sample.INVEST_QUALIF == 'S'].index, inplace=True)
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

            # Salva o arquivo transformado e limpo em seu respectivo diretório.
            files_sample.to_csv(files_list[files], sep=';', index=False, encoding='utf-8-sig')

def f_load(set_wd, file_load, year):
    """
    Carrega a base de dados transformada.
    """
    var_list = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST']
    for path in range(0, 2):

        # Determina o diretório dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        for step_year in range(0, 2):
            # Lê e concatena todos os arquivos CSV do diretório - fi_cad.
            if set_wd[path] == set_wd[0]:
                fi_cad = pd.concat([pd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig') 
                                  for files in glob.glob('*'+year[step_year]+'*.csv')], 
                                  ignore_index=True)
                
                # Remove linhas repetidas.
                fi_cad = fi_cad.drop_duplicates('CNPJ_FUNDO')

                # Salva os arquivos concatenados em seu respectivo diretório.
                fi_cad.to_csv(file_load[path]+'.csv', sep=';', index=False, encoding='utf-8-sig')

                # Validação dos dados.
                print(fi_cad, fi_cad.dtypes, fi_cad.columns, fi_cad.count(), fi_cad.isnull().sum(), fi_cad.nunique(), fi_cad.shape)

            # Lê e concatena todos os arquivos CSV do diretório - fi_diario.
            if set_wd[path] == set_wd[1]:
                fi_diario = pd.concat([pd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list).astype({'VL_QUOTA': 'float16',
                                                                                                                                    'VL_PATRIM_LIQ': 'float32',
                                                                                                                                    'NR_COTST': np.uint16})
                                      for files in glob.glob('*'+year[step_year]+'*.csv')],
                                      ignore_index=True)

                # Salva os arquivos concatenados em seu respectivo diretório.
                fi_diario.to_csv(file_load[path]+'.csv', sep=';', index=False, encoding='utf-8-sig')

                # Validação dos dados.
                print(fi_diario, fi_diario.dtypes, fi_diario.columns, fi_diario.count(), fi_diario.isnull().sum(), fi_diario.nunique(), fi_diario.shape)

    fi_df = fi_diario.merge(fi_cad, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO')

    # Validação dos dados.
    print(fi_df, fi_df.dtypes, fi_df.columns, fi_df.count(), fi_df.isnull().sum(), fi_df.nunique(), fi_df.shape)

    # Determina o diretório da base de dados transformada.
    os.chdir(set_wd[2])

    # Salva os a base de dados transformada em seu respectivo diretório.
    fi_df.to_csv('fi_geral.csv', sep=';', index=False, encoding='utf-8-sig')

def f_exploratory_data(set_wd, file_load):
    """
    Gera os relatórios das análises exploratórias de dados para cada base de dados.
    """
    # Determina o diretório da base de dados transformada.
    os.chdir(set_wd[2])

    # Lê a base de dados geral no diretório - fi_df.
    var_list = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST', 'DENOM_SOCIAL', 'SIT', 'CLASSE',
                'CONDOM', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF']
    fi_profile = pd.read_csv('fi_geral.csv', sep=';', engine='python', encoding='utf-8-sig', usecols=var_list).astype({'VL_QUOTA': 'float16',
                                                                                                                       'VL_PATRIM_LIQ': 'float32',
                                                                                                                       'NR_COTST': np.uint16})

    # Relatório das análises exploratórias de dados.
    fi_profile = ProfileReport(fi_profile, minimal=True)
    fi_profile.to_file('fi_geral_profile.html')

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
    parser.add_argument('-exploratory_data', dest='exploratory_data', action='store_const', const=True, help='Call the f_exploratory_data')
    cmd_args = parser.parse_args()

    # lista de urls para cada ano do cadastro geral de fundos de investimentos.
    df_fi = ['http://dados.cvm.gov.br/dados/FI/CAD/DADOS/', 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_df']
    len_count = [807, 46]
    file_load = ['fi_cad', 'fi_diario']
    year = ['2017', '2017']

    # Define os arqgumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.extract: 
        f_extract(df_fi, set_wd, len_count)
    if cmd_args.transform: 
        f_transform(set_wd, year)
    if cmd_args.load:
        f_load(set_wd, file_load, year)
    if cmd_args.exploratory_data:
        f_exploratory_data(set_wd, file_load)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
        