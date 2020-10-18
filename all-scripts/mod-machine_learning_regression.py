import sys
import argparse
import os
import glob
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score

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

def f_machine_learning_regression(set_wd, file_pattern):
    """
    Modelo de regressão baseado na biblioteca de aprendizado de máquina "scikit-learn".
    """
    Client()
    # Determina o diretorio dos arquivos em cada enlace.
    os.chdir(set_wd[2])
    # Cria uma lista com os names dos arquivos com extenção CSV.
    files_list = glob.glob('*obv_cnpj*')
    # Lista as variáveis da base de dados.
    var_list = ['QUOTA', 'IBOV', 'BOND10Y', 'SHANGHAI', 'NIKKEI', 'DOLLAR', 'OIL']
    features = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list) for files in files_list])
    features = features.compute()
    features.to_csv('features_all_vars.csv', sep=';', index=False, encoding='utf-8-sig')
    # Teste e Treino.
    target = features.drop(columns=['IBOV', 'BOND10Y', 'SHANGHAI', 'NIKKEI', 'DOLLAR', 'OIL'])
    features = features.drop(columns=['QUOTA'])
    features_train, features_test, target_train, target_test = train_test_split(features, target, test_size = 0.2)
    linear_regression = LinearRegression()
    linear_regression.fit(features_train, target_train)
    target_pred = linear_regression.predict(features_test)
    target_pred = pd.DataFrame({'COTA': target_pred[:, 0]})
    print('Intercept: ', linear_regression.intercept_, '| Coefficients: ', linear_regression.coef_)
    # Métricas.
    r2_score(target_train, linear_regression.predict(features_train))
    r2_score(target_test, linear_regression.predict(features_test))

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-machine_learning_regression', dest='machine_learning_regression', action='store_const', const=True, help='Call the f_machine_learning_regression')
    cmd_args = parser.parse_args()
    # Lista de constantes como parâmetros de entrada.
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    file_pattern = ['2017', '2018', '2019', '2020']
    # Define os argumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.machine_learning_regression:
        f_machine_learning_regression(set_wd, file_pattern)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
