# Criando um consumer, gerar o envio via streaming

import csv
import time
from google.cloud import pubsub_v1
import os


# No PubSub é o Nome da Assinatura
subscription = 'NOME DO PROJETO subscriptions/MeusVoos-sub'
subscriber = pubsub_v1.SubscriberClient()


def monstrar_msg(mensagem):
    print(('Mensagem: {}'.format(mensagem)))
    mensagem.ack()


subscriber.subscribe(subscription, callback=monstrar_msg)

while True:
    time.sleep(3)
