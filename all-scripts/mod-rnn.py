import sys
import argparse
import os
import glob
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tensorflow import set_random_seed
from keras.layers import SimpleRNN
from numpy.random import seed
from keras.models import Sequential
import math
from sklearn.metrics import mean_squared_error

def f_recurrent_neural_networks(set_wd):
    """
    Modelo de regressão baseado na biblioteca de aprendizado de máquina "scikit-learn".
    """
    Client()
    seed(42)
    os.chdir(set_wd[2])
    files_list = glob.glob('*obv_cnpj*')
    var_list = ['DATA', 'QUOTA']
    target = dd.concat([dd.read_csv(files, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list) for files in files_list])
    target = target.compute()
    target = target.rename(columns={'QUOTA': 'Valor da Cota'})
    target = target.set_index('DATA')
    target.plot(legend=True)
    plt.title('Fundo de Ações - Valor da Cota')
    plt.show()

    target_train = target[target.index < '2019-08-05'].copy()
    target_test = target[target.index >= '2019-08-05'].copy()

    scaler = MinMaxScaler()
    scaler.fit(target_train)
    scaler.data_min_, scaler.data_max_

    target_train['Valor da Cota'] = scaler.fit_transform(target_train).reshape(-1)
    target_test['Valor da Cota'] = scaler.fit_transform(target_test).reshape(-1)

    # target_train['Valor da Cota'] = scaler.fit_transform(target_train['Valor da Cota'].values.reshape(-1, 1))
    # target_test['Valor da Cota'] = scaler.transform(target_test['Valor da Cota'].values.reshape(-1, 1))

    def gen_rnn_inputs(target, window_size):
        X, y = [], []
        cota = target['Valor da Cota'].values
    
        for i in range(window_size, len(target)):
            X.append(cota[i-window_size: i])
            y.append(cota[i])
        
        return np.array(X), np.array(y)
    
    X_train, y_train = gen_rnn_inputs(target_train, 30)
    X_train.shape, y_train.shape
    X_test, y_test = gen_rnn_inputs(target_test, 30)
    X_test.shape, y_test.shape

    model = Sequential()
    model.add(SimpleRNN(1, input_shape=(30, 1)))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.summary()

    model.fit(np.expand_dims(X_train, axis=-1), y_train, epochs=50, batch_size=8, verbose=2)
    y_pred = model.predict(np.expand_dims(X_test, axis=-1))
    y_pred.shape, y_test.shape

    target_test_preds = target_test.copy()
    target_test_preds['Valor da Cota'] = np.zeros(30).tolist() + scaler.inverse_transform(np.expand_dims(y_test, axis=-1)).reshape(-1).tolist()
    target_test_preds['Previsao'] = np.zeros(30).tolist() + scaler.inverse_transform(y_pred).reshape(-1).tolist()

    def return_rmse(test, predicted):
        rmse = math.sqrt(mean_squared_error(test, predicted))
        print("The root mean squared error is {}.".format(rmse))

    return_rmse(target_test['Valor da Cota'], target_test_preds['Previsao'])

    # target_test_preds['Valor da Cota'] = np.zeros(30).tolist() + scaler.inverse_transform(y_test.reshape(-1,1)).squeeze().tolist()
    # target_test_preds['Prediction'] = np.zeros(30).tolist() + scaler.inverse_transform(y_pred.reshape(-1,1)).squeeze().tolist()

    target_test_preds.plot(legend=True)
    plt.show()

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-recurrent_neural_networks', dest='recurrent_neural_networks', action='store_const', const=True, help='Call the f_recurrent_neural_networks')
    cmd_args = parser.parse_args()
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    if cmd_args.recurrent_neural_networks:
        f_recurrent_neural_networks(set_wd)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
