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
from keras.models import Sequential
from numpy.random import seed
from tensorflow import set_random_seed
from keras.layers import SimpleRNN

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

def f_recurrent_neural_networks(set_wd):
    """
    Modelo de regressão baseado na biblioteca de aprendizado de máquina "scikit-learn".
    """
    Client()
    # Determina o diretorio dos arquivos em cada enlace.
    os.chdir(set_wd[2])
    # Cria uma lista com os names dos arquivos com extenção CSV.
    files_list = glob.glob('test.csv')
    # Lista as variáveis da base de dados.
    var_list = ['QUOTA']
    features = dd.read_csv(files_list, sep=';', engine='python', encoding='utf-8-sig', usecols=var_list)
    features = features.compute()
    features.plot(legend=True)
    plt.title('Fundo de Ações - Valor da Cota')
    plt.show()

    features_train = features[features.index < 150].copy()
    features_test = features[features.index >= 150].copy()

    scaler = MinMaxScaler()
    features_train['QUOTA'] = scaler.fit_transform(features_train['QUOTA'].values.reshape(-1, 1))
    features_test['QUOTA'] = scaler.transform(features_test['QUOTA'].values.reshape(-1, 1))

    def gen_rnn_inputs(features, window_size):
        X, y = [], []
        cota = features['QUOTA'].values
    
        for i in range(window_size, len(features)):
            X.append(cota[i-window_size: i])
            y.append(cota[i])
        
        return np.array(X), np.array(y)
    
    X_train, y_train = gen_rnn_inputs(features_train, 30)
    X_test, y_test = gen_rnn_inputs(features_test, 30)

    model = Sequential()
    model.add(SimpleRNN(1, input_shape=(30, 1)))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.summary()

    model.fit(np.expand_dims(X_train, axis=-1), y_train, epochs=50, batch_size=8, verbose=2)
    y_pred = model.predict(np.expand_dims(X_test, axis=-1))

    features_test_preds = features_test.copy()
    features_test_preds['QUOTA'] = np.zeros(30).tolist() + scaler.inverse_transform(y_test.reshape(-1,1)).squeeze().tolist()
    features_test_preds['Prediction'] = np.zeros(30).tolist() + scaler.inverse_transform(y_pred.reshape(-1,1)).squeeze().tolist()

    features_test_preds.plot(legend=True)
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
    # Lista de constantes como parâmetros de entrada.
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad', 'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_eda']
    # Define os argumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.recurrent_neural_networks:
        f_recurrent_neural_networks(set_wd)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
