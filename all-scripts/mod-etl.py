import sys
import argparse
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import dask.dataframe as dd
import glob
import csv
from pandas_profiling import ProfileReport

def f_extract(fi_cad, set_wd, len_count):
    """
    Extrai os dados da fonte pública - Comissão de Valores Mobiliários (CVM).
    """
    for path in range(0, 2):

        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        # Registra em memória as informações dos arquivos a partir da página html da fonte pública.
        fi_cad_csv = []
        fi_cad_data = requests.get(fi_cad[path]).content
        fi_cad_soup = BeautifulSoup(fi_cad_data, 'html.parser')

        # Encontra e registra em lista o nome dos arquivos.
        for link in fi_cad_soup.find_all('a'):
            fi_cad_csv.append(link.get('href'))
        inf_cadastral_fi = fi_cad_csv[len(fi_cad_csv)-(len_count[path]):(len(fi_cad_csv)-2)]

        # Salva todos os arquivos em seus respectivos diretórios.
        for files in range(0, len(inf_cadastral_fi)):
            fi_cad_csv_url = fi_cad[path]+inf_cadastral_fi[files]
            url_response = requests.get(fi_cad_csv_url)
            url_content = url_response.content
            fi_cad_csv_file = open(inf_cadastral_fi[files], 'wb')
            fi_cad_csv_file.write(url_content)
            fi_cad_csv_file.close()

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
                files_sample = files_sample.drop(columns=['CAPTC_DIA', 'RESG_DIA'])

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
    for path in range(0, 2):

        # Determina o diretório dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        for step_year in range(0, 2):
            # Lê e concatena todos os arquivos CSV do diretório.
            fi_cad = pd.concat([pd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig') 
                               for files in glob.glob('*'+year[step_year]+'*.csv')], 
                               ignore_index=True)

            # Salva os arquivos concatenados em seu respectivo diretório.
            fi_cad.to_csv(file_load[path]+'_'+year[step_year]+'.csv', sep=';', index=False, encoding='utf-8-sig')

            # Validação dos dados.
            print(fi_cad, fi_cad.dtypes, fi_cad.columns, fi_cad.count(), fi_cad.isnull().sum(), fi_cad.nunique(), fi_cad.shape)

def f_exploratory_data(set_wd, file_load):
    """
    Gera os relatórios das análises exploratórias de dados para cada base de dados.
    """
    for path in range(0, 2):

        # Determina o diretório dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        # Cria uma lista com os names dos arquivos com extenção CSV.
        files_list = glob.glob('*'+file_load[path]+'*.csv')

        for files in range(0, len(files_list)):
            # Lê e concatena todos os arquivos CSV do diretório.
            fi_cad = pd.read_csv(files_list[files], sep=';', engine='python', encoding='utf-8-sig')

            # Relatório das análises exploratórias de dados.
            profile = ProfileReport(fi_cad, minimal=True)

            profile.to_file('profiling_'+files_list[files][0:11]+'.html')

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
    fi_cad = ['http://dados.cvm.gov.br/dados/FI/CAD/DADOS/', 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario']
    len_count = [807, 46]
    file_load = ['fi_cad', 'fi_diario']
    year = ['2017', '2017']

    # Define os arqgumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.extract: 
        f_extract(fi_cad, set_wd, len_count)
    if cmd_args.transform: 
        f_transform(set_wd, year)
    if cmd_args.load:
        f_load(set_wd, file_load, year)
    if cmd_args.exploratory_data:
        f_exploratory_data(set_wd, file_load)

    # test=fi_diario.merge(fi_cad, left_index=True, right_index=True)
    # CNPJ_FUNDO=pd.Series(list(set(a).intersection(set(b))))

if __name__ == '__main__':
    f_main()
    sys.exit(0)
        