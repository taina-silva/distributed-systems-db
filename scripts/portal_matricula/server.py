import random
import sys 
import grpc

import scripts.portal_matricula.protos.portal_matricula_pb2_grpc as pb2_grpc

from concurrent import futures
from scripts.portal_matricula.utils.server_service import ServerService
from scripts.portal_matricula.utils.server_actions import ServerActions as matricula_server_actions

dados = {matricula_server_actions.topic_alunos: dict(), 
        matricula_server_actions.topic_disciplinas: dict(),
        matricula_server_actions.topic_professores: dict(),
        matricula_server_actions.topic_disciplinas_alunos: dict(),
        matricula_server_actions.topic_disciplinas_professor: dict()
    }

def start_server(port_, client):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    pb2_grpc.add_PortalMatriculaServicer_to_server(
        ServerService.service(client, dados), server)
    
    server.add_insecure_port('[::]:' + port_)
    server.start()

    print('Portal Matrícula iniciado em porta '+ port_)

    return server

if __name__ == "__main__":
    port_ = '50052' if len(sys.argv) <= 1 else sys.argv[1]

    server = start_server(port_)
    server.wait_for_termination()