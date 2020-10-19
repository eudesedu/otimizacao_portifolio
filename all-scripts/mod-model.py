import sys
import argparse
from dask.distributed import Client
import os
import glob
import dask.dataframe as dd
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.metrics import accuracy_score

def f_regression_models(set_wd):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    Client()
    os.chdir(set_wd[2])
    files_list = glob.glob('*obv_cnpj*')
    var_list = ['QUOTA', 'PATRIMONIO', 'COTISTAS', 'IBOV', 'IBOV_VOLUME', 'SP500', 'SP500_VOLUME', 'NASDAQ', 'NASDAQ_VOLUME', 'BOND10Y', 'SHANGHAI',
                'SHANGHAI_VOLUME', 'NIKKEI', 'NIKKEI_VOLUME', 'VOLATILITY', 'DOLLAR', 'GOLD', 'GOLD_VOLUME', 'OIL', 'OIL_VOLUME', 'IBOV_OBV', 'SP500_OBV',
                'NASDAQ_OBV', 'SHANGHAI_OBV', 'NIKKEI_OBV', 'GOLD_OBV', 'OIL_OBV']
    data_model = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list) for files in files_list])
    data_model = data_model.compute()
    data_model.to_csv('data_model.csv', sep=';', index=False, encoding='utf-8-sig')
    os.chdir(set_wd[3])
    os.system(r'gretlcli -r multiple_regression.inp')

    def f_machine_learning_regression():
        """
        Modelo de regressão baseado na biblioteca de aprendizado de máquina "scikit-learn".
        """
        features = pd.DataFrame(data_model, columns=['IBOV', 'SHANGHAI', 'NIKKEI', 'DOLLAR', 'OIL'])
        target = pd.DataFrame(data_model, columns=['QUOTA'])
        features_train, features_test, target_train, target_test = train_test_split(features, target, test_size = 0.2)
        linear_regression = LinearRegression()
        linear_regression.fit(features_train, target_train)
        target_pred = linear_regression.predict(features_test)
        target_pred = pd.DataFrame({'COTA': target_pred[:, 0]})
        print('Intercept: ', linear_regression.intercept_, '| Coefficients: ', linear_regression.coef_)
        print(r2_score(target_train, linear_regression.predict(features_train)))
        print(r2_score(target_test, linear_regression.predict(features_test)))
    f_machine_learning_regression()

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-regression_models', dest='regression_models', action='store_const', const=True, help='Call the f_regression_models')
    cmd_args = parser.parse_args()
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda', 'C:\\Users\\eudes\\Documents\\github\\otimizacao_portifolio\\all-scripts\\grelt']
    if cmd_args.regression_models:
        f_regression_models(set_wd)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
