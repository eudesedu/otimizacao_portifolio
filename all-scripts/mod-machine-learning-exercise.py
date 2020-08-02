import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import argparse
import importlib
import os
import sys

def f_load(setwd):
    """
    Local dataset of credit cards canceled.
    """
    os.chdir(setwd)
    d_card = pd.read_csv('cartao-credito-cancelamento.csv').drop(columns=['ID'], axis=1)
    return print(d_card, d_card.dtypes, d_card.columns, d_card.corr(), d_card.isnull().sum(), d_card.nunique(), d_card.shape)

def f_transform(setwd):
    """
    Generate dummy and transform variables.
    """
    le = LabelEncoder()
    os.chdir(setwd)
    d_card = pd.read_csv('cartao-credito-cancelamento.csv').drop(columns=['ID'], axis=1)
    dummy_card = pd.get_dummies(d_card, columns=['PerfilEconomico', 'Sexo', 'PerfilCompra', 'RegiaodoPais'], drop_first=True)
    d_uf = pd.DataFrame(le.fit_transform(dummy_card['UF']), columns=['le_UF'])
    d_cid = pd.DataFrame(le.fit_transform(dummy_card['CidadeResidencia']), columns=['le_CidadeResidencia'])
    dummy_card = pd.merge(dummy_card, d_uf, left_index=True, right_index=True)
    dummy_card = pd.merge(dummy_card, d_cid, left_index=True, right_index=True)
    dummy_card.drop(columns=['UF', 'CidadeResidencia'], inplace=True)
    return print(d_uf, d_cid, dummy_card, dummy_card.dtypes, dummy_card.columns)

def f_main():
    """
    Import a module to generate the environment configuration and parser argument.
    """
    descr = """
        Define the parser argument to load a local dataset.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-load', dest='load', action='store_const', const=True, help='Call the f_load function')
    parser.add_argument('-transform', dest='transform', action='store_const', const=True, help='Call the f_transform function')
    cmd_args = parser.parse_args()
    importlib.import_module('mod-environment')
    if cmd_args.load: 
        f_load(os.environ.get('DIR_DATASET'))
    if cmd_args.transform: 
        f_transform(os.environ.get('DIR_DATASET'))

if __name__ == '__main__':
    f_main()
    sys.exit(0)
