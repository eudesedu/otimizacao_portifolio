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
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

def f_regression_models(set_wd):
    """
    Modelo baseado em regressão linear múltipla para variável resposta: Valor da Cota (VL_QUOTA).
    """
    Client()
    os.chdir(set_wd[2])
    list_files = glob.glob('*obv_cnpj*')
    list_varibles = ['QUOTA', 'DATA', 'PATRIMONIO', 'COTISTAS', 'IBOV', 'IBOV_VOLUME', 'SP500', 'SP500_VOLUME', 'NASDAQ', 'NASDAQ_VOLUME', 'BOND10Y',
                     'SHANGHAI', 'SHANGHAI_VOLUME', 'NIKKEI', 'NIKKEI_VOLUME', 'VOLATILITY', 'DOLLAR', 'GOLD', 'GOLD_VOLUME', 'OIL', 'OIL_VOLUME',
                     'IBOV_OBV', 'SP500_OBV', 'NASDAQ_OBV', 'SHANGHAI_OBV', 'NIKKEI_OBV', 'GOLD_OBV', 'OIL_OBV']
    df_regression_models = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=list_varibles) for files in list_files])
    df_regression_models = df_regression_models.compute()
    df_regression_models.to_csv('batch_regression_models.csv', sep=';', index=False, encoding='utf-8-sig')
    os.chdir(set_wd[3])
    os.system(r'gretlcli -r gretl_regression_models.inp')
    os.chdir(set_wd[2])

    def f_machine_learning_regression():
        """
        Modelo de regressão baseado na biblioteca de aprendizado de máquina "scikit-learn".
        """
        df_features = pd.DataFrame(df_regression_models, columns=['IBOV', 'SHANGHAI', 'NIKKEI', 'DOLLAR', 'OIL'])
        df_target = pd.DataFrame(df_regression_models, columns=['QUOTA'])
        df_features_train, df_features_test, df_target_train, df_target_test = train_test_split(df_features, df_target, test_size = 0.3)
        print(df_features_train, df_features_test, df_target_train, df_target_test)

        model_linear_regression = LinearRegression().fit(df_features_train, df_target_train)

        array_target_prediction = model_linear_regression.predict(df_features_test)
        df_target_prediction = pd.DataFrame({'COTA': array_target_prediction[:, 0]})
        print(df_target_prediction, 'Intercept: ', model_linear_regression.intercept_, '| Coefficients: ', model_linear_regression.coef_)
        print('R-squared train: ', r2_score(df_target_train, model_linear_regression.predict(df_features_train)))
        print('R-squared test : ', r2_score(df_target_test, model_linear_regression.predict(df_features_test)))

        df_features_prediction = pd.DataFrame({'Previsao_Cota': model_linear_regression.predict(df_features_train)[:, 0]})
        df_ml_target = pd.concat([df_target_train.reset_index(), df_features_prediction], axis=1).drop(columns=['index'])
        df_ml_target.to_csv('batch_ml_target.csv', sep=';', index=False, encoding='utf-8-sig')
        os.chdir(set_wd[3])
        os.system(r'gretlcli -r gretl_ml_regression_models.inp')
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
