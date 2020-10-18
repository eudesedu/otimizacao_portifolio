import sys
import argparse
from dask.distributed import Client
import os
import glob
import dask.dataframe as dd
import pandas as pd

def f_regression_model(set_wd, file_pattern):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    Client()
    os.chdir(set_wd[2])
    files_list = glob.glob('*obv_cnpj*')
    data_model = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig') for files in files_list])
    data_model = data_model.compute()
    os.chdir(set_wd[3])
    os.system(r'gretlcli -r regression_model.inp')

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-regression_model', dest='regression_model', action='store_const', const=True, help='Call the f_regression_model')
    cmd_args = parser.parse_args()
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario', 
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_model']
    if cmd_args.regression_model:
        f_regression_model(set_wd)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
