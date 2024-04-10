import random
import logging
import sys 
import grpc

import scripts.portal_matricula.protos.portal_matricula_pb2_grpc as pb2_grpc

from concurrent import futures
from paho.mqtt import client as mqtt_client
from scripts.portal_administrativo.utils.server_actions import ServerActions as admininistrativo_server_actions
from scripts.portal_matricula.utils.server_service import ServerService
from scripts.portal_matricula.utils.server_actions import ServerActions as matricula_server_actions

broker = 'broker.emqx.io'
port_mqtt = 1883
client_id = f'python-mqtt-{random.randint(0, 1000)}'

dados = {matricula_server_actions.topic_alunos: dict(), 
        matricula_server_actions.topic_disciplinas: dict(),
        matricula_server_actions.topic_professores: dict(),
        matricula_server_actions.topic_disciplinas_alunos: dict(),
        matricula_server_actions.topic_disciplinas_professor: dict()
    }

def subscribe_topicos(client):
    def on_message(client, userdata, msg):
        print(f'\nMensagem recebida ({msg.payload.decode()} de tópico {msg.topic})')

        portal = (msg.payload.decode()).split("/")[0]
        action = ((msg.payload.decode()).split("/")[1]).split("-")[0]
        value = ((msg.payload.decode()).split("/")[1]).split("-")[1]
        value_dict = eval(value)

        if portal == 'PA':
            dict_chave, tipo_chave_entidade, crud = admininistrativo_server_actions.action_to_keys(action)
            
            admininistrativo_server_actions.crud_server(dados, dict_chave, tipo_chave_entidade, value, crud)

            if action == 'RemoveProfessor':
                for k, v in dados[matricula_server_actions.topic_disciplinas_professor].items(): 
                    if v == value_dict['id']:
                        dados[matricula_server_actions.topic_disciplinas_professor][k] = None
            elif action == 'RemoveAluno':
                for k, v in dados[matricula_server_actions.topic_disciplinas_alunos].items(): 
                    if value_dict['id'] in v:
                        aux = dados[matricula_server_actions.topic_disciplinas_alunos][k]
                        aux.remove(value_dict['id'])
                        dados[matricula_server_actions.topic_disciplinas_alunos][k] = aux
            elif action == 'RemoveDisciplina':
                for k, v in dados[matricula_server_actions.topic_disciplinas_professor].items():
                    if k == v['id']:
                        dados[matricula_server_actions.topic_disciplinas_professor].pop(k)

                for k, v in dados[matricula_server_actions.topic_disciplinas_alunos].items():
                    if k == v['id']:
                        dados[matricula_server_actions.topic_disciplinas_alunos].pop(k)
        else:
            dict_chave, tipo_chave_1, tipo_chave_2, tipo_chave_3, tipo_chave_4, crud = matricula_server_actions.action_to_keys(action)

            matricula_server_actions.crud_server(dados, dict_chave, tipo_chave_1, tipo_chave_2, tipo_chave_3, tipo_chave_4, value, crud)
        
    client.subscribe(matricula_server_actions.topic_alunos)
    client.subscribe(matricula_server_actions.topic_professores)
    client.subscribe(matricula_server_actions.topic_disciplinas)
    client.subscribe(matricula_server_actions.topic_disciplinas_professor)
    client.subscribe(matricula_server_actions.topic_disciplinas_alunos)
    client.on_message = on_message

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print('Conectado ao MQTT Broker!')
        else:
            print('Falha ao conectar ao MQTT Broker, código de retorno %d\n', rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port_mqtt)

    return client

def start_server(port_, client):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    pb2_grpc.add_PortalMatriculaServicer_to_server(
        ServerService.service(client, dados), server)
    
    server.add_insecure_port('[::]:' + port_)
    server.start()

    print('Portal Matrícula iniciado em porta '+ port_)

    return server

if __name__ == "__main__":
    logging.basicConfig()

    port_grpc = '50052' if len(sys.argv) <= 1 else sys.argv[1]

    client = connect_mqtt()
    server = start_server(port_grpc, client)

    client.loop_start()
    subscribe_topicos(client)

    server.wait_for_termination()