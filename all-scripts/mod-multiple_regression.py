import sys
import argparse
import os
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
from sklearn import linear_model
import statsmodels.api as sm

def f_regression_model(set_wd, file_pattern):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    var_list = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'NR_COTST', 'DENOM_SOCIAL', 'SIT', 'CLASSE',
                'CONDOM', 'FUNDO_COTAS', 'FUNDO_EXCLUSIVO', 'INVEST_QUALIF']
    fi_geral_modelo = pd.DataFrame()
    for step in range(0, 4):
        os.chdir(set_wd[2]+'\\'+file_pattern[step])
        fi_geral = pd.read_csv('fi_geral.csv', sep=';', engine='python', encoding='utf-8-sig', usecols=var_list).astype({'VL_QUOTA': 'float16',
                                                                                                                         'VL_PATRIM_LIQ': 'float32',
                                                                                                                         'NR_COTST': np.uint16})
        for chunk in fi_geral:
            chunk = fi_geral.loc[fi_geral['CNPJ_FUNDO'] == '11.052.478/0001-81']
            fi_geral_modelo = pd.concat([fi_geral_modelo, chunk], ignore_index=True)
    fi_geral_modelo.to_csv(set_wd[2]+'\\fi_geral_modelo.csv', sep=';', index=False, encoding='utf-8-sig')
    fi_geral_modelo = fi_geral_modelo.drop(columns=['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'CONDOM', 'FUNDO_COTAS',
                                                    'FUNDO_EXCLUSIVO', 'INVEST_QUALIF'])
    x = fi_geral_modelo[['VL_PATRIM_LIQ', 'NR_COTST']]
    y = fi_geral_modelo['VL_QUOTA']
    regression = linear_model.LinearRegression().fit(x, y)
    print('Intercept: ', regression.intercept_, '| Coefficients: ', regression.coef_)
    patrimonio_liquido = 60240396.0
    cotistas = 385
    print ('Resultado: ', regression.predict([[patrimonio_liquido, cotistas]]))
    patrimonio_liquido = 665332160.0
    cotistas = 13841
    print ('Resultado: ', regression.predict([[patrimonio_liquido, cotistas]]))
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
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    file_pattern = ['2017', '2018', '2019', '2020', 'inf']
    if cmd_args.regression_model:
        f_regression_model(set_wd, file_pattern)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
