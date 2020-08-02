import sys
import argparse
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import glob

def f_extract(fi_cad, set_wd, len_count):
    """
    Extrai os dados da fonte pública - Comissão de Valores Mobiliários (CVM).
    """
    for path in range(0, 2):

        # Determina o diretorio dos arquivos em cada enlace.
        os.chdir(set_wd[path])

        # Registra em memória as informações dos arquivos a partir da página html da fonte pública.
        fi_cad_csv = []
        fi_cad_data = requests.get(fi_cad[path]).content
        fi_cad_soup = BeautifulSoup(fi_cad_data, 'html.parser')

        # Encontre e registra em lista o nome dos arquivos.
        for link in fi_cad_soup.find_all('a'):
            fi_cad_csv.append(link.get('href'))
        inf_cadastral_fi = fi_cad_csv[len(fi_cad_csv)-(len_count[path]):(len(fi_cad_csv)-2)]
        
        # Salva todos os arquivos em seus respectivos diretórios.
        for files in range(0, len(inf_cadastral_fi)):
            fi_cad_csv_url = fi_cad[path]+inf_cadastral_fi[files]
            url_response = requests.get(fi_cad_csv_url)
            url_content = url_response.content
            fi_cad_csv_file = open(inf_cadastral_fi[files], 'wb')
            fi_cad_csv_file.write(url_content)
            fi_cad_csv_file.close()

def f_transform(set_wd):
    """
    Aplica uma série de transformações aos dados extraídos para gerar o fluxo que será carregado.
    """

    # Comente!
    os.chdir(set_wd[0])

    # Sample!
    fi_cad_list = [files for files in glob.glob('*.{}'.format('CSV'))]
    fi_cad_sample = pd.concat([pd.read_csv(files, sep=';', engine='python') for files in fi_cad_list[1:2]])

    # Verificação
    print(fi_cad_sample, 
          fi_cad_sample.dtypes, 
          fi_cad_sample.columns, 
          fi_cad_sample.count(), 
          fi_cad_sample.isnull().sum(), 
          fi_cad_sample.nunique(), 
          fi_cad_sample.shape)
    
    # Sample!
    fi_cad_sample = pd.read_csv(fi_cad_list[1], sep=';', engine='python').drop(columns=['DT_REG',
                                                                                        'DT_CONST',
                                                                                        'DT_CANCEL',
                                                                                        'DT_INI_SIT',
                                                                                        'DT_INI_ATIV',
                                                                                        'RENTAB_FUNDO',
                                                                                        'TRIB_LPRAZO',
                                                                                        'TAXA_PERFM',
                                                                                        'DT_PATRIM_LIQ',
                                                                                        'DIRETOR',
                                                                                        'ADMIN',
                                                                                        'PF_PJ_GESTOR',
                                                                                        'CPF_CNPJ_GESTOR',
                                                                                        'GESTOR'])
    
    # Sample!
    fi_cad_sample = fi_cad_sample.astype(str)
    fi_cad_sample.applymap(lambda x: x.strip() if type(x)==str else x)

    # Sample!
    fi_cad_sample.filter(items=['CNPJ_FUNDO']).head(50)
    fi_cad_sample.filter(items=['SIT']).head(50)
    fi_cad_sample.filter(items=['CLASSE']).head(50)   
    fi_cad_sample.filter(items=['DT_INI_CLASSE']).head(50)    
    fi_cad_sample.filter(items=['CONDOM']).head(50)       
    fi_cad_sample.filter(items=['FUNDO_COTAS']).head(50)     
    fi_cad_sample.filter(items=['FUNDO_EXCLUSIVO']).head(50) 
    fi_cad_sample.filter(items=['INVEST_QUALIF']).head(50)  
    fi_cad_sample.filter(items=['VL_PATRIM_LIQ']).head(50)
    fi_cad_sample.filter(items=['CNPJ_ADMIN']).head(50)
    
    # Sample!
    fi_cad_sample.filter(items=['CNPJ_FUNDO']).tail(50)
    fi_cad_sample.filter(items=['SIT']).tail(50)
    fi_cad_sample.filter(items=['CLASSE']).tail(50)   
    fi_cad_sample.filter(items=['DT_INI_CLASSE']).tail(50)    
    fi_cad_sample.filter(items=['CONDOM']).tail(50)       
    fi_cad_sample.filter(items=['FUNDO_COTAS']).tail(50)     
    fi_cad_sample.filter(items=['FUNDO_EXCLUSIVO']).tail(50) 
    fi_cad_sample.filter(items=['INVEST_QUALIF']).tail(50)  
    fi_cad_sample.filter(items=['VL_PATRIM_LIQ']).tail(50)
    fi_cad_sample.filter(items=['CNPJ_ADMIN']).tail(50)

    # Sample!
    fi_cad_sample[fi_cad_sample.CNPJ_FUNDO.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.SIT.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.CLASSE.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.DT_INI_CLASSE.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.CONDOM.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.FUNDO_COTAS.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.FUNDO_EXCLUSIVO.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.INVEST_QUALIF.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.VL_PATRIM_LIQ.str.contains('nan', regex=True, na=False)]
    fi_cad_sample[fi_cad_sample.CNPJ_ADMIN.str.contains('nan', regex=True, na=False)]

    # Remove 'vazios'!
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CNPJ_FUNDO == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.SIT == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CLASSE == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.DT_INI_CLASSE == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CONDOM == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.FUNDO_COTAS == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.FUNDO_EXCLUSIVO == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.INVEST_QUALIF == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.VL_PATRIM_LIQ == ''].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CNPJ_ADMIN == ''].index, inplace=True)

    # Remove 'nan'!
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.SIT == 'nan'].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CLASSE == 'nan'].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.VL_PATRIM_LIQ == 'nan'].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CNPJ_ADMIN == 'nan'].index, inplace=True)

    # Remove SIT!
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.SIT == 'CANCELADA'].index, inplace=True)
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.SIT == 'FASE PRÉ-OPERACIONAL'].index, inplace=True)

    # Remove CONDOM!
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.CONDOM == 'Fechado'].index, inplace=True)

    # Remove FUNDO_EXCLUSIVO!
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.FUNDO_EXCLUSIVO == 'S'].index, inplace=True)

    # Remove INVEST_QUALIF!
    fi_cad_sample.drop(fi_cad_sample[fi_cad_sample.INVEST_QUALIF == 'S'].index, inplace=True)

    # Remove FUNDO_EXCLUSIVO e INVEST_QUALIF!
    fi_cad_sample = fi_cad_sample.drop(columns=['FUNDO_EXCLUSIVO', 'INVEST_QUALIF'])
    
    # Carregar!
    fi_cad_sample.to_csv( "fi_cad.csv", sep=';', index=False, encoding='utf-8-sig')

def f_load(set_wd):
    """
    Carrega a base de dados transformada.
    """
    fi_cad = pd.read_csv('fi_cad.csv')

    # Validação
    print(fi_cad, 
          fi_cad.dtypes, 
          fi_cad.columns, 
          fi_cad.count(), 
          fi_cad.isnull().sum(), 
          fi_cad.nunique(), 
          fi_cad.shape)

def f_main():
    """
    Configuração do ambiente e definição dos argumentos para chamada das funções.
    """
    descr = """
        Define os argumentos para chamar funções através de linha de comandos.
    """
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument('-extract', dest='extract', action='store_const', const=True, help='Call the f_extract function')
    parser.add_argument('-transform', dest='transform', action='store_const', const=True, help='Call the f_transform function')
    parser.add_argument('-load', dest='load', action='store_const', const=True, help='Call the f_load function')
    cmd_args = parser.parse_args()

    # lista de urls para cada ano do cadastro geral de fundos de investimentos.
    fi_cad = ['http://dados.cvm.gov.br/dados/FI/CAD/DADOS/',
              'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
    set_wd = ['C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_cad',
              'C:\\Users\\eudes\\Documents\\github\\dataset\\tcc\\fi_inf_diario']
    len_count = [807, 46]

    # Define os arqgumentos e variáveis como parâmetros de entrada para funções.
    if cmd_args.extract: 
        f_extract(fi_cad, set_wd, len_count)
    if cmd_args.transform: 
        f_transform(set_wd)
    if cmd_args.load: 
        f_load(set_wd)

if __name__ == '__main__':
    f_main()
    sys.exit(0)
