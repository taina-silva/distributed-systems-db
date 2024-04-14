import sys 
import grpc

import scripts.portal_administrativo.protos.portal_administrativo_pb2_grpc as pb2_grpc

from concurrent import futures
from scripts.portal_administrativo.utils.server_actions import ServerActions
from scripts.portal_administrativo.utils.server_service import ServerService

dados = {ServerActions.topic_alunos: dict(), 
        ServerActions.topic_disciplinas: dict(),
        ServerActions.topic_professores: dict()}

def start_server(port_):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_PortalAdministrativoServicer_to_server(ServerService().service(dados), server)
    
    server.add_insecure_port('[::]:' + port_)
    server.start()

    print('Portal Administrativo iniciado em porta '+ port_)

    return server

if __name__ == '__main__':
    port_ = '50051' if len(sys.argv) <= 1 else sys.argv[1]

    server = start_server(port_)
    server.wait_for_termination()