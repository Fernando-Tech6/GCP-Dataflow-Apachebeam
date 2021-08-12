# Lendo em um sistema de nuvem,  transformando e apresentando resultado na nuvem como template, antes de inserir o template no Dataflow

import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions


# SERÁ OMITIDO O PIPELINE_OPTIONS COM CONFIGURAÇÃO DE PASTAS E ACESSO AO GCP.


pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p8 = beam.Pipeline(options=pipeline_options)



# Criando uma método para substituir os filtros no beam.Filter
class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]


caminho_entrada = 'gs://BUCKET/entrada/voos_sample.csv'
caminho_saida = 'gs://BUCKET/saida/voos_gcp.csv'

Tempo_Atrasos = (
    p8
    | "Importar Dados Atraso" >> beam.io.ReadFromText(caminho_entrada, skip_header_lines=1)
    | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
    # Utilizando ParDo para o método criado
    | "Pegar voos com atraso" >> beam.ParDo(filtro())
    | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Somar por key" >> beam.CombinePerKey(sum)

)

Qtd_Atrasos = (
    p8
    | "Importar Dados" >> beam.io.ReadFromText(caminho_entrada, skip_header_lines=1)
    | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso qtd" >> beam.ParDo(filtro())
    | "Criar par qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Contar por key" >> beam.combiners.Count.PerKey()

)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "Group By" >> beam.CoGroupByKey()
    | 'Saída para GCP' >> beam.io.WriteToText(caminho_saida)
)
p8.run()
