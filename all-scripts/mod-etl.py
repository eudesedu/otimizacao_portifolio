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
from sklearn import linear_model
import statsmodels.api as sm

##############################################################################################################################################################
################################################################# Portal Dados Abertos - CVM #################################################################
#################################################################  http://dados.cvm.gov.br/  #################################################################
"""
CKAN é a maior plataforma para portal de dados em software livre do mundo.

CKAN é uma solução completa e pronta para usar que torna os dados acessíveis e utilizáveis – ao prover ferramentas para simplificar a publicação,
o compartilhamento, o encontro e a utilização dos dados (incluindo o armazenamento de dados e o provimento de robustas APIs de dados). CKAN está
direcionado a publicadores de dados (governos nacionais e regionais, companhias e organizações) que querem tornar seus dados abertos e disponíveis.

CKAN é usado por governos e grupos de usuários em todo o mundo e impulsiona vários portais oficiais e da comunidade, incluindo portais governamentais
locais, nacionais e internacionais, tais como o data.gov.uk do Reino Unido, o publicdata.eu da União Europeia, o dados.gov.br do Brasil, o portal do
governo da Holanda, assim como sítios de cidades e municípios nos EUA, Reino Unido, Argentina, Finlândia e em outros lugares.

CKAN: http://ckan.org/ | http://ckan.org/tour/
Visão geral das funcionalidades: http://ckan.org/features/
"""
##############################################################################################################################################################

def f_extract(df_fi, set_wd, len_count):
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
            os.system('dir *csv* /b')
        os.system('dir *csv* /T:W')

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

def f_regression_model(set_wd, year):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    # Lista as variáveis da base de dados.
    var_list = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST', 'DENOM_SOCIAL', 'SIT', 'CLASSE',
                'CONDOM', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF']

    fi_geral_modelo = pd.DataFrame()
    for step_year in range(0, 4):

        # Determina o diretório da base de dados transformada - fi_df.
        os.chdir(set_wd[2]+'\\'+year[step_year])

        fi_geral = pd.read_csv('fi_geral.csv', sep=';', engine='python', encoding='utf-8-sig', usecols=var_list).astype({'VL_QUOTA': 'float16',
                                                                                                                         'VL_PATRIM_LIQ': 'float32',
                                                                                                                         'NR_COTST': np.uint16})
        for chunk in fi_geral:
            chunk = fi_geral.loc[fi_geral['CNPJ_FUNDO'] == '11.052.478/0001-81']
            fi_geral_modelo = pd.concat([fi_geral_modelo, chunk], ignore_index=True)

    # Salva os a base de dados transformada em seu respectivo diretório.
    fi_geral_modelo.to_csv(set_wd[2]+'\\fi_geral_modelo.csv', sep=';', index=False, encoding='utf-8-sig')

    # Prepara a base para os cálculos do modelo.
    fi_geral_modelo = fi_geral_modelo.drop(columns=['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'CONDOM', 'FUNDO_COTAS',
                                                    'FUNDO_EXCLUSIVO', 'INVEST_QUALIF'])

    x = fi_geral_modelo[['VL_PATRIM_LIQ', 'NR_COTST']]
    y = fi_geral_modelo['VL_QUOTA']

    # Regressão múltipla utilizando a biblioteca sklearn
    regression = linear_model.LinearRegression().fit(x, y)
    print('Intercept: ', regression.intercept_, '| Coefficients: ', regression.coef_)

    # Previsões:
    patrimonio_liquido = 60240396.0
    cotistas = 385
    print ('Resultado: ', regression.predict([[patrimonio_liquido, cotistas]]))

    patrimonio_liquido = 665332160.0
    cotistas = 13841
    print ('Resultado: ', regression.predict([[patrimonio_liquido, cotistas]]))

    # Regressão múltipla utilizando a biblioteca statsmodels.
    x = sm.add_constant(x)
    regression_model = sm.OLS(y, x).fit()
    regression_model.predict(x) 
    regression_model = regression_model.summary()
    print(regression_model)

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
    parser.add_argument('-regression_model', dest='regression_model', action='store_const', const=True, help='Call the f_regression_model')
    cmd_args = parser.parse_args()

    # Lista de constantes como parâmetros de entrada.
    df_fi = ['http://dados.cvm.gov.br/dados/FI/CAD/DADOS/', 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_df']
    len_count = [817, 46]
    file_load = ['fi_cad', 'fi_diario']
    year = ['2017', '2017', '2018', '2018', '2019', '2019', '2020', '2020']
    #year = ['2017', '2018', '2019', '2020']

    # Define os argumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.extract: 
        f_extract(df_fi, set_wd, len_count)
    if cmd_args.transform: 
        f_transform(set_wd)
    if cmd_args.load:
        f_load(set_wd, file_load, year)
    if cmd_args.exploratory_data:
        f_exploratory_data(set_wd, file_load)
    if cmd_args.regression_model:
        f_regression_model(set_wd, year)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
