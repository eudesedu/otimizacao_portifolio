import sys
import argparse
from dask.distributed import Client
import os
import glob
import dask.dataframe as dd
import pandas as pd

def f_multiple_regression(set_wd, file_pattern):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    Client()
    os.chdir(set_wd[2])
    files_list = glob.glob('*obv_cnpj*')
    var_list = ['QUOTA', 'PATRIMONIO', 'COTISTAS', 'IBOV', 'IBOV_VOLUME', 'SP500', 'SP500_VOLUME', 'NASDAQ', 'NASDAQ_VOLUME', 'BOND10Y', 'SHANGHAI', 'SHANGHAI_VOLUME', 'NIKKEI', 
                'NIKKEI_VOLUME', 'VOLATILITY', 'DOLLAR', 'GOLD', 'GOLD_VOLUME', 'OIL', 'OIL_VOLUME', 'IBOV_OBV', 'SP500_OBV', 'NASDAQ_OBV', 'SHANGHAI_OBV',
                'NIKKEI_OBV', 'GOLD_OBV', 'OIL_OBV']
    data_model = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list) for files in files_list])
    data_model = data_model.compute()
    data_model.to_csv('data_model.csv', sep=';', index=False, encoding='utf-8-sig')
    os.chdir(set_wd[3])
    os.system(r'gretlcli -r multiple_regression.inp')

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-multiple_regression', dest='multiple_regression', action='store_const', const=True, help='Call the f_multiple_regression')
    cmd_args = parser.parse_args()
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario', 
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda', 'C:\\Users\\eudes\\Documents\\github\\otimizacao_portifolio\\all-scripts\\grelt']
    if cmd_args.multiple_regression:
        f_multiple_regression(set_wd)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
