# Executando no ambiente local.
# Lendo na máquina local, executando e transformando no apache beam e apresentando resultado na máquina local

import apache_beam as beam

p8 = beam.Pipeline()

Tempo_Atrasos = (
    p8
    | "Importar Dados Atraso" >> beam.io.ReadFromText("/data/voos_sample.csv", skip_header_lines=1)
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
    | beam.Map(print)
)

p8.run()
