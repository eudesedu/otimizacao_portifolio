import sys
import argparse
import os
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from pandas_profiling import ProfileReport
import numpy as np
import re
import string
import numpy as np
import yfinance as yf
import glob

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

def f_exploratory_data_cvm(set_wd, file_load, file_pattern, tickers):
    """
    Gera os relatórios das análises exploratórias de dados para cada base de dados da CVM.
    """
    Client()
    for step in range(0, 4):
        for path in range(0, 2):
            # Determina o diretório da base de dados transformada.
            os.chdir(set_wd[path])
            if set_wd[path] == set_wd[0]:
                # Lê a base de dado.
                fi_cad = dd.read_csv(file_load[path]+'_'+file_pattern[step]+'.csv', sep=';', engine='python', encoding='utf-8-sig')
                fi_cad = fi_cad.compute()
                # Troca o nome das variáveis.
                fi_cad = fi_cad.rename(columns={'CNPJ_FUNDO': 'CNPJ', 'DENOM_SOCIAL': 'NOME', 'CONDOM': 'CONDICAO', 'FUNDO_COTAS': 'COTAS',
                                                'FUNDO_EXCLUSIVO': 'EXCLUSIVO', 'INVEST_QUALIF': 'QUALIFICADO'})
                # Relatório das análises exploratórias de dados.
                fi_profile = ProfileReport(fi_cad, title='Profiling Report')
                fi_profile.to_file(set_wd[2]+'\\'+file_load[path]+'_'+file_pattern[step]+'.html')
            else:
                # Lê a base de dado.
                fi_diario = dd.read_csv(file_load[path]+'_'+file_pattern[step]+'.csv', sep=';', engine='python',
                                        encoding='utf-8-sig').astype({'VL_QUOTA': 'float16', 'VL_PATRIM_LIQ': 'float32', 'NR_COTST': np.uint16})
                fi_diario = fi_diario.compute()
                # Troca o nome das variáveis.
                fi_diario = fi_diario.rename(columns={'CNPJ_FUNDO': 'CNPJ', 'DT_COMPTC': 'DATA', 'VL_QUOTA': 'QUOTA', 'VL_PATRIM_LIQ': 'PATRIMONIO',
                                                      'NR_COTST': 'COTISTAS'})
                # Salva os arquivos concatenados em seu respectivo diretório.
                fi_diario = fi_diario.merge(fi_cad, left_on='CNPJ', right_on='CNPJ')
                fi_diario.to_csv(set_wd[2]+'\\merged_file_'+file_pattern[step]+'.csv', sep=';', index=False, encoding='utf-8-sig')

def f_exploratory_data_index(set_wd, file_load, file_pattern, tickers):
    """
    Gera os relatórios das análises exploratórias de dados para cada base de dados dos índices.
    """
    Client()
    os.chdir(set_wd[2])
    regex_punctuation = r"[{}]".format(string.punctuation)
    for step in range(0, 4):
        # Lê a base de dado.
        fi_diario = dd.read_csv('merged_file_'+file_pattern[step]+'.csv', sep=';', engine='python',
                                encoding='utf-8-sig').astype({'QUOTA': 'float16', 'PATRIMONIO': 'float32', 'COTISTAS': np.uint16})
        fi_diario = fi_diario.compute()
        fi_diario = fi_diario.loc[fi_diario['CLASSE'] == 'Fundo de Ações']
        cnpj_fi_unique = fi_diario.CNPJ.to_frame().drop_duplicates('CNPJ')
        cnpj_fi_unique = cnpj_fi_unique.head(2)
        cnpj_list = cnpj_fi_unique['CNPJ'].tolist()
        for cnpj in range(0, len(cnpj_list)):
            fi_cnpj = fi_diario.set_index('CNPJ').filter(regex=cnpj_list[cnpj], axis=0).reset_index()
            for index in range(0, len(tickers)):
                ticker = yf.download(tickers[index], start=file_pattern[step]+'-01-01', end=file_pattern[step]+'-12-31').reset_index()
                ticker = ticker.drop(columns=['Open', 'High', 'Low', 'Close'])
                ticker = ticker.astype({'Date': 'str', 'Adj Close': 'float32', 'Volume': np.uint32})
                ticker = ticker.rename(columns={'Date': 'DATA', 'Adj Close': tickers[index]+'_FECHAMENTO', 'Volume': tickers[index]+'_VOLUME'})
                fi_cnpj = fi_cnpj.merge(ticker, left_on='DATA', right_on='DATA')
            # Relatório das análises exploratórias de dados.
            fi_cnpj.to_csv(file_load[2]+'_'+file_pattern[step]+'_'+re.sub(regex_punctuation, "", cnpj_list[cnpj])+'.csv', sep=';', index=False, encoding='utf-8-sig')

def f_exploratory_data_obv(set_wd, file_load, file_pattern, tickers, volume_index, obv):
    """
    Gera os relatórios das análises exploratórias de dados para cada base de dados considerando o On-Balance Volume (OBV).
    """
    Client()
    os.chdir(set_wd[2])
    for step in range(0, 4):
        # Define as variáveis do cadastro dos fundos de investimentos.
        var_list = ['CNPJ', 'DATA', 'QUOTA', 'PATRIMONIO', 'COTISTAS', 'NOME', 'CLASSE', 'CONDICAO', 'COTAS', 'EXCLUSIVO', 'QUALIFICADO', '^BVSP_FECHAMENTO',
                    '^BVSP_VOLUME', '^GSPC_FECHAMENTO', '^GSPC_VOLUME', '^IXIC_FECHAMENTO', '^IXIC_VOLUME', '^TNX_FECHAMENTO', '000001.SS_FECHAMENTO',
                    '000001.SS_VOLUME', '^N225_FECHAMENTO', '^N225_VOLUME', '^VIX_FECHAMENTO', 'BRL=X_FECHAMENTO', 'GC=F_FECHAMENTO', 'GC=F_VOLUME',
                    'CL=F_FECHAMENTO', 'CL=F_VOLUME']
        # Cria uma lista com os names dos arquivos com extenção CSV.
        files_list = glob.glob('*'+file_load[2]+'_'+file_pattern[step]+'*')
        # Lê cada arquivo da lista removendo as variáveis desnecessárias:
        for files in range(0, len(files_list)):
            fi_cnpj = dd.read_csv(files_list[files], sep=';', engine='python', usecols=var_list, encoding='utf-8-sig')
            fi_cnpj = fi_cnpj.compute()
            fi_cnpj = fi_cnpj.rename(columns={'^BVSP_FECHAMENTO' : 'IBOV', '^BVSP_VOLUME' : 'IBOV_VOLUME', '^GSPC_FECHAMENTO' : 'SP500', 
                                              '^GSPC_VOLUME' : 'SP500_VOLUME', '^IXIC_FECHAMENTO' : 'NASDAQ', '^IXIC_VOLUME' : 'NASDAQ_VOLUME',
                                              '^TNX_FECHAMENTO' : 'BOND10Y', '000001.SS_FECHAMENTO' : 'SHANGHAI', '000001.SS_VOLUME' : 'SHANGHAI_VOLUME', 
                                              '^N225_FECHAMENTO' : 'NIKKEI', '^N225_VOLUME' : 'NIKKEI_VOLUME', '^VIX_FECHAMENTO' : 'VOLATILITY', 
                                              'BRL=X_FECHAMENTO' : 'DOLLAR', 'GC=F_FECHAMENTO' : 'GOLD', 'GC=F_VOLUME' : 'GOLD_VOLUME', 'CL=F_FECHAMENTO' : 'OIL', 
                                              'CL=F_VOLUME' : 'OIL_VOLUME'})
            for volume_index_count in range(0, len(volume_index)):
                volume = fi_cnpj.loc[:, [volume_index[volume_index_count]]]
                volume = volume[volume_index[volume_index_count]].tolist()
                obv_list = []
                for volume_count in range(0, len(volume) - 1):
                    previous_volume_index = volume[volume_count]
                    current_volume_index = volume[volume_count + 1]
                    if current_volume_index == previous_volume_index:
                        obv_list.append(current_volume_index)
                    elif current_volume_index > previous_volume_index:
                        obv_list.append(previous_volume_index + current_volume_index)
                    elif current_volume_index < previous_volume_index:
                        obv_list.append(previous_volume_index - current_volume_index)
                obv_list.append(0)
                obv_data = pd.DataFrame(obv_list)
                obv_data = obv_data.rename(columns={0 : obv[volume_index_count]})
                fi_cnpj = pd.concat([fi_cnpj, obv_data], axis=1)
            fi_cnpj.drop(fi_cnpj.tail(1).index, inplace=True)
            # Relatório das análises exploratórias de dados.
            fi_cnpj.to_csv('obv_'+files_list[files], sep=';', index=False, encoding='utf-8-sig')
            fi_profile = ProfileReport(fi_cnpj, title='Profiling Report')
            fi_profile.to_file('obv_'+files_list[files][0:28]+'.html')

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-exploratory_data_cvm', dest='exploratory_data_cvm', action='store_const', const=True, help='Call the f_exploratory_data_cvm')
    parser.add_argument('-exploratory_data_index', dest='exploratory_data_index', action='store_const', const=True, help='Call the f_exploratory_data_index')
    parser.add_argument('-exploratory_data_obv', dest='exploratory_data_obv', action='store_const', const=True, help='Call the f_exploratory_data_obv')
    cmd_args = parser.parse_args()
    # Lista de constantes como parâmetros de entrada.
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    file_load = ['fi_cad', 'fi_diario', 'cnpj']
    file_pattern = ['2017', '2018', '2019', '2020', 'inf']
    tickers = ['^BVSP', '^GSPC', '^IXIC', '^TNX', '000001.SS', '^N225', '^VIX', 'BRL=X', 'GC=F', 'CL=F']
    volume_index = ['IBOV_VOLUME', 'SP500_VOLUME', 'NASDAQ_VOLUME', 'SHANGHAI_VOLUME', 'NIKKEI_VOLUME', 'GOLD_VOLUME', 'OIL_VOLUME']
    obv = ['IBOV_OBV', 'SP500_OBV', 'NASDAQ_OBV', 'SHANGHAI_OBV', 'NIKKEI_OBV', 'GOLD_OBV', 'OIL_OBV']
    # Define os argumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.exploratory_data_cvm:
        f_exploratory_data_cvm(set_wd, file_load, file_pattern, tickers)
    if cmd_args.exploratory_data_index:
        f_exploratory_data_index(set_wd, file_load, file_pattern, tickers)
    if cmd_args.exploratory_data_obv:
        f_exploratory_data_obv(set_wd, file_load, file_pattern, tickers, volume_index, obv)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
