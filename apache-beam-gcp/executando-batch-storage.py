
# Lendo na máquina local, executando e transformando no apache beam e apresentando resultado na nuvem

import apache_beam as beam
import os  # Operation System - Biblioteca do Python


# SERÁ OMITIDO O PIPELINE_OPTIONS COM CONFIGURAÇÃO DE PASTAS E ACESSO AO GCP.


p8 = beam.Pipeline()

Tempo_Atrasos = (
    p8
    | "Importar Dados Atraso" >> beam.io.ReadFromText("data/voos_sample.csv", skip_header_lines=1)
    | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso" >> beam.Filter(lambda record: int(record[8]) > 0)
    | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Somar por key" >> beam.CombinePerKey(sum)

)

Qtd_Atrasos = (
    p8
    | "Importar Dados" >> beam.io.ReadFromText("data/voos_sample.csv", skip_header_lines=1)
    | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso qtd" >> beam.Filter(lambda record: int(record[8]) > 0)
    | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Contar por key" >> beam.combiners.Count.PerKey()

)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "Group By" >> beam.CoGroupByKey()
    # SALVAR NO BUCKET...Necessário criar o nome do arquivo
    | 'Saída para GCP' >> beam.io.WriteToText('gs://BUCKET/entrada/voos_atrasados_cloud.csv')
)
p8.run()
