import random
import socket
import sys
import time
import grpc
import scripts.portal_administrativo.protos.portal_administrativo_pb2 as pb2
import scripts.portal_administrativo.protos.portal_administrativo_pb2_grpc as pb2_grpc
from scripts.portal_administrativo.utils.server_actions import ServerActions

class ServerService(object):
    @staticmethod
    def service(dicts):
        num_socket = random.randint(10000,10002)
        num_socket = 10001

        try:
            socketDB = socket.socket()
            socketDB.settimeout(1)

            socketDB.connect(('127.0.0.1', num_socket))

            print(f'Servidor iniciado, conectado ao servidor do na porta {num_socket}')

        except:
            print('Erro na criação das partições')
            sys.exit()
                    
        
        class PortalAdministrativo(pb2_grpc.PortalAdministrativo):
            def NovoAluno(self, aluno, _):
                return ServerActions.NovaEntidade(socketDB, aluno, dicts, ServerActions.topic_alunos, 'NovoAluno')
                
            def EditaAluno(self, aluno, _):
                return ServerActions.EditaEntidade(aluno, dicts, ServerActions.topic_alunos, 'EditaAluno')
                
            def RemoveAluno(self, identificador, _):
                return ServerActions.RemoveEntidade(identificador, dicts, ServerActions.topic_alunos, 'RemoveAluno')

            def ObtemAluno(self, identificador, _):
                return ServerActions.ObtemEntidade(socketDB, identificador, dicts, 'ObtemAluno')
            
            def ObtemTodosAlunos(self, _, __):
                return ServerActions.ObtemTodasEntidades(dicts, 'ObtemTodosAlunos')

            # --------------------------------------------------------------

            def NovoProfessor(self, professor, _):
                return ServerActions.NovaEntidade(professor, dicts, ServerActions.topic_professores, 'NovoProfessor')
                
            def EditaProfessor(self, professor, _):
                return ServerActions.EditaEntidade(professor, dicts, ServerActions.topic_professores, 'EditaProfessor')
                
            def RemoveProfessor(self, identificador, _):
                return ServerActions.RemoveEntidade(identificador, dicts, ServerActions.topic_professores, 'RemoveProfessor')

            def ObtemProfessor(self, identificador, _):
                return ServerActions.ObtemEntidade(identificador, dicts, 'ObtemProfessor')
            
            def ObtemTodosProfessores(self, _, __):
                return ServerActions.ObtemTodasEntidades(dicts, 'ObtemTodosProfessores')

            # --------------------------------------------------------------

            def NovaDisciplina(self, disciplina, _):
                return ServerActions.NovaEntidade(disciplina, dicts, ServerActions.topic_disciplinas, 'NovaDisciplina')
                
            def EditaDisciplina(self, disciplina, _):
                return ServerActions.EditaEntidade(disciplina, dicts, ServerActions.topic_disciplinas, 'EditaDisciplina')
                
            def RemoveDisciplina(self, identificador, _):
                return ServerActions.RemoveEntidade(identificador, dicts, ServerActions.topic_disciplinas, 'RemoveDisciplina')

            def ObtemDisciplina(self, identificador, _):
                return ServerActions.ObtemEntidade(identificador, dicts, 'ObtemDisciplina')

            def ObtemTodasDisciplinas(self, _, __):
                return ServerActions.ObtemTodasEntidades(dicts, 'ObtemTodasDisciplinas')
            
        return PortalAdministrativo()
        
    