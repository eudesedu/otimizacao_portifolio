import sys
import argparse
import os
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
from sklearn import linear_model
import statsmodels.api as sm

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

def f_regression_model(set_wd, file_pattern):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    # Lista as variáveis da base de dados.
    var_list = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST', 'DENOM_SOCIAL', 'SIT', 'CLASSE',
                'CONDOM', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF']
    fi_geral_modelo = pd.DataFrame()
    for step in range(0, 4):
        # Determina o diretório da base de dados transformada - fi_df.
        os.chdir(set_wd[2]+'\\'+file_pattern[step])
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
    parser.add_argument('-regression_model', dest='regression_model', action='store_const', const=True, help='Call the f_regression_model')
    cmd_args = parser.parse_args()
    # Lista de constantes como parâmetros de entrada.
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    file_pattern = ['2017', '2018', '2019', '2020', 'inf']
    # Define os argumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.regression_model:
        f_regression_model(set_wd, file_pattern)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
