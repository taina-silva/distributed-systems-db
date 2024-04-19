import random
import socket
import sys
import scripts.portal_matricula.protos.portal_matricula_pb2_grpc as pb2_grpc

from scripts.portal_matricula.utils.server_actions import ServerActions

class ServerService(object):

    @staticmethod
    def service(dicts):
        num_socket = random.randint(10001,10006)
    
        try:
            host = socket.gethostname()

            socketDB = socket.socket()
            socketDB.settimeout(1)

            socketDB.connect((host, num_socket))

            print(f'Portal Matrícula conectado ao servidor do banco na porta {num_socket}')
        except:
            print('Erro na criação das partições')
            sys.exit()

        class PortalMatricula(pb2_grpc.PortalMatricula):
            def AdicionaProfessor(self, disciplina_pessoa, _):
                return ServerActions.AdicionaPessoaEmDisciplina(socketDB,
                    disciplina_pessoa, dicts, "AdicionaProfessor"
                )

            def RemoveProfessor(self, disciplina_pessoa, _):
                return ServerActions.RemovePessoaDeDisciplina(socketDB,
                    disciplina_pessoa, dicts, "RemoveProfessor"
                )

            def AdicionaAluno(self, disciplina_pessoa, _):
                return ServerActions.AdicionaPessoaEmDisciplina(socketDB,
                    disciplina_pessoa, dicts, "AdicionaAluno"
                )

            def RemoveAluno(self, disciplina_pessoa, _):
                return ServerActions.RemovePessoaDeDisciplina(socketDB,
                    disciplina_pessoa, dicts, "RemoveAluno"
                )

            def DetalhaDisciplina(self, identificador, _):
                return ServerActions.ObtemDisciplinaDetalhada(socketDB,
                    identificador, dicts, "DetalhaDisciplina", False
                )

            def ObtemDisciplinasProfessor(self, identificador, _):
                return ServerActions.ObtemDisciplinasEntidade(socketDB,
                    identificador, dicts, "ObtemDisciplinasProfessor"
                )

            def ObtemDisciplinasAluno(self, identificador, _):
                return ServerActions.ObtemDisciplinasAluno(socketDB,
                    identificador, dicts, "ObtemDisciplinasAluno"
                )

        return PortalMatricula()
