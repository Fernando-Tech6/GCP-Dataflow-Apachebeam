import apache_beam as beam
from apache_beam import pipeline
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# Para utilizar expressões Regulares
import re


import os  # Operation System - Biblioteca do Python

# SERÁ OMITIDO O PIPELINE_OPTIONS COM CONFIGURAÇÃO DE PASTAS E ACESSO AO GCP.


caminho_entrada = 'gs://LOCALIZAÇÃO DO BUCKET/entrada/sample_casos_dengue.txt'
caminho_entrada2 = 'gs://LOCALIZAÇÃO DO BUCKET/entrada/sample_chuvas.csv'
caminho_saida = 'gs://LOCALIZAÇÃO DO BUCKET/saida/resultado_chuva_denge.csv'


# Não posso esquecer isso, se não ele irá enviar o arquivo para pasta de saida e não cria um template
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
pipeline = beam.Pipeline(options=pipeline_options)


# Criar um método para transformar lista em um dicionario
colunas_dengue = [
    'id', 'data_iniSE', 'casos', 'ibge_code', 'cidade', 'uf', 'cep', 'latitude', 'longitude'
]


def lista_para_dicionario(elemento, colunas):
    return dict(zip(colunas, elemento))


# Criar um método que receba uma string e converta pra uma tupla ou uma lista.
# Recebe um texto e recebe uma lista como itens divididos por cada pipe(|)
def texto_para_lista(elemento, delimitador):
    return elemento.split(delimitador)


# Criando uma coluna composto pelo valor do ano e do mês, excluido o dia, para depois unificar com o dataframe de chuva
def tratando_data(elemento):
    # Usando o [:2] para pegar apenas os primeiros dois itens da lista de data ['ano','mes','dia']
    # Usando um join para retorna como string
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento


# Criar método com uma chave por Estado
def chave_uf(elemento):
    """
    Recebe um dicionario
    Retornar uma tupla com o estado(UF) como chave e o elemento ('SP',{elemento})
    """
    chave = elemento['uf']
    return (chave, elemento)


# Criar método com um tupla de Uf+data e o numero de casos
def casos_dengue(elemento):
    """
    Recebe uma tupla ('SP',[{}, {}])
    Retornar uma tupla ('SP-2014-02', 8.0)
    Criar uma expressão regular para saber se a coluna casos possue elemento vazio
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            # tenho que converter a string dos casos para float
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            (f"{uf}-{registro['ano_mes']}", 0.0)


# Converter lista chuvas para tupla
def chave_uf_lista(elemento):
    """
    Receber uma lista de elementos
    Retornar uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES, 1.0)
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm


def chave_uf_ano_mes_de_lista(elemento):
    """
    Receber uma lista de elementos
    Retornar uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-MES', 1.3)
    ['2016-01-24', '4.2', 'TO']
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm


def arrendonda(elemento):
    """
    Recebe uma tupla('SP-2020-06', 4.2222222)
    Retorna uma tupla com valor arredondado('SP-2020-06', 4.2)
    """
    chave, mm = elemento
    return (chave, round(mm, 1))


# Filtar espaços em branco no dicionario de Chuva e Dengue (Pipeline Resultado)
def filtrando_elementos_vazios(elemento):
    """
    Remove elementos que tenhas chaves vazias
    Receber um tupla ('CE-2015-02', {'Chuvas': [], 'Dengue': [337.0]})
    Remover a tupla com dados vazios
    """
    chave, dicionario = elemento
    if all([
        # all serve para passar mais de um argumento retornando TRUE para todos verdadeiros e False para algum falso
        dicionario['Chuvas'],
        dicionario['Dengue']
    ]):
        return True
    return False


# Criar método para descompactar elementos e colocar tudo em uma unica tupla
def descompactar(elemento):
    """
    Receber uma tupla que contém um dicionario ('CE-2015-02', {'Chuvas': [55.0], 'Dengue': [337.0]})
    Retorna uma tupla sem o dicionario ('CE',2015,02, '55.0',  '337.0')
    """
    chave, dicionario = elemento
    # o [0] é para pegar o primeiro elemento da lista
    chuva = dicionario['Chuvas'][0]
    dengue = dicionario['Dengue'][0]
    uf, ano, mes = chave.split('-')
    return (uf, ano, mes, str(chuva), str(dengue))


# Transforma uma tupla e uma string para arquivo CSV
def tupla_em_string(elemento, delimitador=';'):
    """
    Receber uma tupla   ('CE',2015,02, 55.0,  337.0)
    Retornar uma string  'CE;2015;02;55.0;337.0'
    """
    return f"{delimitador}".join(elemento)


# Devidos os datasets (dengue e chuva) serem muito grandes, será utilziado o sample(dengue e chuva) que é uma arquivo menor
# Criando uma pcollection e realizando alguns tratamentos.
dengue = (
    pipeline
    | 'Leitura do dataset de sample_dengue' >> ReadFromText(caminho_entrada, skip_header_lines=1)
    | 'De texto para lista' >> beam.Map(texto_para_lista, delimitador='|')
    | 'De lista para dicionario' >> beam.Map(lista_para_dicionario, colunas_dengue)
    | 'Criar campo Ano_Mês' >> beam.Map(tratando_data)
    | 'Chave UF' >> beam.Map(chave_uf)
    | 'Agrupar pelo Estado GroupByKey' >> beam.GroupByKey()
    | 'Tuplas casos de dengue' >> beam.FlatMap(casos_dengue)
    | 'Soma dos casos pela chave' >> beam.CombinePerKey(sum)
    # | 'Exibir na tela' >> beam.Map(print)

)


# Criar um pipeline para dataset de chuvas
chuvas = (
    pipeline
    | 'Leitura dataset sample_chuvas' >> ReadFromText(caminho_entrada2, skip_header_lines=1)
    | 'De texto para lista (virgula)' >> beam.Map(texto_para_lista, delimitador=',')
    | 'Criando a chave uf-ano-mes' >> beam.Map(chave_uf_ano_mes_de_lista)
    | 'Soma de total de chuvas pela chave' >> beam.CombinePerKey(sum)
    | 'Arredondar resultados de chuvas' >> beam.Map(arrendonda)
    # | 'Exibir na tela (chuvas)' >> beam.Map(print)
)


# Unir os dois datasets. ('uf-data_ano',casos,qtd_chuva)
resultado = (
    ({'Chuvas': chuvas, 'Dengue': dengue})
    | 'Mesclar (Merge) as pcollections' >> beam.CoGroupByKey()
    | 'Filtrando dados vazios' >> beam.Filter(filtrando_elementos_vazios)
    | 'Descompactando Elementos' >> beam.Map(descompactar)
    | 'Preparar CSV' >> beam.Map(tupla_em_string)
    # | 'Resultado' >> beam.Map(print)
    | 'Enviar para Cloud Storage' >> WriteToText(caminho_saida)



)

pipeline.run()
