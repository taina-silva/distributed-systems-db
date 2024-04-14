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
        count = 0
        error = False

        while count < 3:
            num_socket = random.randint(10000,10002)
        
            try:
                host = socket.gethostname()

                socketDB = socket.socket()
                socketDB.settimeout(1)

                socketDB.connect((host, num_socket))
                error = False

                print(f'Portal administrativo conectado ao servidor do banco na porta {num_socket}')

                break
            except:
                count += 1
                error = True
                    
        if error is True:
            print('Erro na criação das partições')
            sys.exit()
        
        class PortalAdministrativo(pb2_grpc.PortalAdministrativo):
            def NovoAluno(self, aluno, _):
                return ServerActions.NovaEntidade(socketDB, aluno, dicts, 'NovoAluno')
                
            def EditaAluno(self, aluno, _):
                return ServerActions.EditaEntidade(socketDB, aluno, dicts, 'EditaAluno')
                
            def RemoveAluno(self, identificador, _):
                return ServerActions.RemoveEntidade(socketDB, identificador, dicts, 'RemoveAluno')

            def ObtemAluno(self, identificador, _):
                return ServerActions.ObtemEntidade(socketDB, identificador, dicts, 'ObtemAluno')
            
            def ObtemTodosAlunos(self, _, __):
                return ServerActions.ObtemTodasEntidades(socketDB, dicts, 'ObtemTodosAlunos')

            # --------------------------------------------------------------

            def NovoProfessor(self, professor, _):
                return ServerActions.NovaEntidade(socketDB, professor, dicts, 'NovoProfessor')
                
            def EditaProfessor(self, professor, _):
                return ServerActions.EditaEntidade(socketDB, professor, dicts, 'EditaProfessor')
                
            def RemoveProfessor(self, identificador, _):
                return ServerActions.RemoveEntidade(socketDB, identificador, dicts, 'RemoveProfessor')

            def ObtemProfessor(self, identificador, _):
                return ServerActions.ObtemEntidade(socketDB, identificador, dicts, 'ObtemProfessor')
            
            def ObtemTodosProfessores(self, _, __):
                return ServerActions.ObtemTodasEntidades(socketDB, dicts, 'ObtemTodosProfessores')

            # --------------------------------------------------------------

            def NovaDisciplina(self, disciplina, _):
                return ServerActions.NovaEntidade(socketDB, disciplina, dicts, 'NovaDisciplina')
                
            def EditaDisciplina(self, disciplina, _):
                return ServerActions.EditaEntidade(socketDB, disciplina, dicts, 'EditaDisciplina')
                
            def RemoveDisciplina(self, identificador, _):
                return ServerActions.RemoveEntidade(socketDB, identificador, dicts, 'RemoveDisciplina')

            def ObtemDisciplina(self, identificador, _):
                return ServerActions.ObtemEntidade(socketDB, identificador, dicts, 'ObtemDisciplina')

            def ObtemTodasDisciplinas(self, _, __):
                return ServerActions.ObtemTodasEntidades(socketDB, dicts, 'ObtemTodasDisciplinas')
            
        return PortalAdministrativo()
        
    