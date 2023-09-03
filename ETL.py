# Importar bibliotecas
import pandas as pd
from sqlalchemy import create_engine

# Configurar conexão com o banco de dados de destino
engine = create_engine('mysql://usuario:senha@localhost/nome_do_banco')

# Definir funções para Extração, Transformação e Carga

# Função de Extração
def extract_data(source_path):
    # Suponha que estamos lendo dados de um arquivo CSV
    data = pd.read_csv(source_path)
    return data

# Função de Transformação
def transform_data(data):
    # Suponha que estamos realizando uma transformação simples aqui
    data['coluna_transformada'] = data['coluna_origem'] * 2
    return data

# Função de Carga
def load_data(data, table_name):
    data.to_sql(table_name, engine, if_exists='replace', index=False)

# Executar o pipeline ETL
def main():
    source_path = 'caminho/para/seu/arquivo.csv'
    table_name = 'nome_da_tabela'

    data = extract_data(source_path)
    transformed_data = transform_data(data)
    load_data(transformed_data, table_name)

if __name__ == '__main__':
    main()
 