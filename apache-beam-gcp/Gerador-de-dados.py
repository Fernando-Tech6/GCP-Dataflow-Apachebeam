import csv
import time
from google.cloud import pubsub_v1
import os

# SERÁ OMITIDO O PIPELINE_OPTIONS COM CONFIGURAÇÃO DE PASTAS E ACESSO AO GCP.

caminho_entrada = 'data/voos_sample.csv'

with open(caminho_entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico, row)
        time.sleep(2)  # A cada 2 segundo ele vai estar publicando no pubsub
