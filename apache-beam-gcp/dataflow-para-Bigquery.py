import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions


# SERÁ OMITIDO O PIPELINE_OPTIONS COM CONFIGURAÇÃO DE PASTAS E ACESSO AO GCP.

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)




class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]


def criar_dict_nivel1(record):
    dict_ = {}
    dict_['airport'] = record[0]
    dict_['lista'] = record[1]
    return(dict_)


def desaninhar_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return [(key + '_' + k, v) for k, v in desaninhar_dict(value).items()]
        else:
            return [(key, value)]
    items = [item for k, v in record.items() for item in expand(k, v)]
    return dict(items)


def criar_dict_nivel0(record):
    dict_ = {}
    dict_['airport'] = record['airport']
    dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
    dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
    return(dict_)


# Criando tabelas e colunas no BigQuery
table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = 'PROJETO:DATASET.TABELA'


caminho_entrada = 'BUCKET/entrada/voos_sample.csv'

Tempo_Atrasos = (
    p1
    | "Importar Dados Atraso" >> beam.io.ReadFromText(caminho_entrada, skip_header_lines=1)
    | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso" >> beam.ParDo(filtro())
    | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p1
    | "Importar Dados" >> beam.io.ReadFromText(caminho_entrada, skip_header_lines=1)
    | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com Qtd" >> beam.ParDo(filtro())
    | "Criar par Qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | beam.CoGroupByKey()
    | beam.Map(lambda record: criar_dict_nivel1(record))
    | beam.Map(lambda record: desaninhar_dict(record))
    | beam.Map(lambda record: criar_dict_nivel0(record))
    | beam.io.WriteToBigQuery(
        tabela,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location='gs://BUCKET/temp')

)

p1.run()
