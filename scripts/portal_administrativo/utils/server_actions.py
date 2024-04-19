import json
import copy
import scripts.portal_administrativo.protos.portal_administrativo_pb2 as pb2
import scripts.portal_administrativo.protos.portal_administrativo_pb2_grpc as pb2_grpc


class ServerActions:
    topic_alunos = "alunos"
    topic_professores = "professores"
    topic_disciplinas = "disciplinas"

    @staticmethod
    def action_to_keys(action):
        ta, ca = (ServerActions.topic_alunos, "matricula")
        tp, cp = (ServerActions.topic_professores, "siape")
        td, cd = (ServerActions.topic_disciplinas, "sigla")
        crd = "id"

        dict_chave, tipo_chave_entidade_1, tipo_chave_entidade_2, crud = {
            "NovoAluno": (ta, ca, ca, "c"),
            "ObtemAluno": (ta, crd, ca, "r"),
            "EditaAluno": (ta, ca, ca, "u"),
            "RemoveAluno": (ta, crd, ca, "d"),
            "ObtemTodosAlunos": (ta, ca, ca, "ra"),
            "NovoProfessor": (tp, cp, cp, "c"),
            "ObtemProfessor": (tp, crd, cp, "r"),
            "EditaProfessor": (tp, cp, cp, "u"),
            "RemoveProfessor": (tp, crd, cp, "d"),
            "ObtemTodosProfessores": (tp, cp, cp, "ra"),
            "NovaDisciplina": (td, cd, cd, "c"),
            "ObtemDisciplina": (td, crd, cd, "r"),
            "EditaDisciplina": (td, cd, cd, "u"),
            "RemoveDisciplina": (td, crd, cd, "d"),
            "ObtemTodasDisciplinas": (td, cd, cd, "ra"),
        }[action]

        return dict_chave, tipo_chave_entidade_1, tipo_chave_entidade_2, crud

    @staticmethod
    # recebe value como str e retorna dict (quando retorna algo)
    def crud_server(
        server_dict,
        dict_chave,
        tipo_chave_entidade_1,
        tipo_chave_entidade_2,
        value,
        crud,
    ):

        # exemplo o dicionario dos 'alunos'
        dicionario_dados = server_dict[dict_chave]

        if crud == "c":
            # exemplo '{'matricula': 123, 'nome': 'teste'}'
            entidade_dict = eval(value)
            # exemplo '123'
            entidade_id = str(entidade_dict[tipo_chave_entidade_1])
            entidade_dict.pop(tipo_chave_entidade_1)
            # vai colocar {'123': {'nome': 'teste'}}
            dicionario_dados[entidade_id] = entidade_dict

            return value

        if crud == "u":
            # exemplo '{'matricula': 123, 'nome': 'teste'}'
            entidade_dict = eval(value)
            # exemplo '123'
            entidade_id = str(entidade_dict[tipo_chave_entidade_1])

            try:
                dicionario_dados.pop(entidade_id)
                dicionario_dados[entidade_id] = entidade_dict

                return value
            except:
                return None

        # no caso do 'r', value é pb2.Identificador como str (exemplo '{'id': '123'}')
        elif crud == "r":
            try:
                # exemplo '{'matricula': 123, 'nome': 'teste'}'
                chave = eval(value)[tipo_chave_entidade_1]
                entidade_dict = copy.deepcopy(dicionario_dados[chave])
                entidade_dict[tipo_chave_entidade_2] = chave

                return entidade_dict
            except:
                return None

        # no caso do 'd', value é pb2.Identificador como str (exemplo '{'id': '123'}')
        elif crud == "d":
            try:
                chave = eval(value)[tipo_chave_entidade_1]
                entidade_dict = dicionario_dados.pop(chave)
                entidade_dict[tipo_chave_entidade_2] = chave

                return entidade_dict
            except:
                return None

    # exemplo ({'matricula': '123', 'nome': 'teste'}, x, 'alunos', 'NovoAluno', f)
    # porém, recebe essa entidade como .proto
    @staticmethod
    def NovaEntidade(server_socket, entidade, server_dict, action):
        # exemplo ('alunos', 'matricula', 'c' <-- necessariamente 'c')
        dict_chave, tipo_chave_entidade_1, _, crud = ServerActions.action_to_keys(
            action
        )

        value_dict = ServerActions.__dict_from_entidade(entidade, dict_chave)
        value_str = json.dumps(value_dict)

        """ entidade_dict = ServerActions.crud_server(
            server_dict, dict_chave, tipo_chave_entidade_1, _, value_str, crud
        )

        if entidade_dict is None:
            return pb2.Status(
                status=1,
                msg=f"Falha ao inserir objeto '{value_str}'.",
            ) """

        chave = value_dict[tipo_chave_entidade_1]
        valor = value_str

        msg = json.dumps(
            {
                "function": "insert",
                "key": f"{tipo_chave_entidade_1}-{chave}",
                "value": valor,
            }
        )

        server_socket.send(msg.encode())
        response = server_socket.recv(2048)
        response = json.loads(response.decode())

        if response.get("msg") is None:
            return pb2.Status(
                status=1,
                msg=f"Falha ao inserir objeto '{value_str}'.",
            )
        else:
            return pb2.Status(
                status=0,
                msg=f"Sucesso ao inserir objeto '{value_str}'.",
            )

    @staticmethod
    def EditaEntidade(server_socket, entidade, server_dict, action):
        # exemplo ('alunos', 'matricula', 'u' <-- necessariamente 'u')
        dict_chave, tipo_chave_entidade_1, _, crud = ServerActions.action_to_keys(
            action
        )

        value_dict = ServerActions.__dict_from_entidade(entidade, dict_chave)
        value_str = json.dumps(value_dict)

        """ ServerActions.crud_server(
            server_dict, dict_chave, tipo_chave_entidade_1, _, value_str, crud
        ) """

        chave = value_dict[tipo_chave_entidade_1]
        valor = value_str

        msg = json.dumps(
            {
                "function": "edit",
                "key": f"{tipo_chave_entidade_1}-{chave}",
                "value": valor,
            }
        )

        server_socket.send(msg.encode())
        response = server_socket.recv(2048)
        response = json.loads(response.decode())

        if response.get("msg") is None:
            return pb2.Status(
                status=1,
                msg=f"Falha ao editar objeto '{value_str}'.",
            )
        else:
            return pb2.Status(
                status=0,
                msg=f"Sucesso ao editar objeto '{value_str}'.",
            )

    # entidade é um pb2.Identificador
    @staticmethod
    def RemoveEntidade(server_socket, entidade, server_dict, action):
        # exemplo ('alunos', 'matricula', 'd' <-- necessariamente 'd')
        dict_chave, tipo_chave_entidade_1, tipo_chave_entidade_2, crud = (
            ServerActions.action_to_keys(action)
        )

        value_dict = ServerActions.__dict_from_entidade(entidade, dict_chave)
        value_str = str(value_dict)

        """ ServerActions.crud_server(
            server_dict,
            dict_chave,
            tipo_chave_entidade_1,
            tipo_chave_entidade_2,
            value_str,
            crud,
        ) """

        chave = value_dict[tipo_chave_entidade_1]

        msg = json.dumps(
            {"function": "delete", "key": f"{tipo_chave_entidade_1}-{chave}"}
        )

        server_socket.send(msg.encode())
        response = server_socket.recv(2048)
        response = json.loads(response.decode())

        if response.get("msg") is None:
            return pb2.Status(
                status=1,
                msg=f"Falha ao remover objeto '{value_str}'.",
            )
        else:
            return pb2.Status(
                status=0,
                msg=f"Sucesso ao remover objeto '{value_str}'.",
            )

    # entidade é um pb2.Identificador
    @staticmethod
    def ObtemEntidade(server_socket, entidade, server_dict, action):
        # exemplo ('alunos', 'matricula', 'r' <-- necessariamente 'r')
        dict_chave, tipo_chave_entidade_1, tipo_chave_entidade_2, crud = (
            ServerActions.action_to_keys(action)
        )

        value_dict = ServerActions.__dict_from_entidade(entidade, tipo_chave_entidade_1)
        value_str = str(value_dict)

        """ entidade_dict = ServerActions.crud_server(
            server_dict,
            dict_chave,
            tipo_chave_entidade_1,
            tipo_chave_entidade_2,
            value_str,
            crud,
        )

        if entidade_dict is not None:
            return ServerActions.__entidade_from_dict(entidade_dict, dict_chave) """

        chave = value_dict[tipo_chave_entidade_1]

        msg = json.dumps(
            {"function": "read", "key": f"{tipo_chave_entidade_2}-{chave}"}
        )

        server_socket.send(msg.encode())
        response = server_socket.recv(2048)
        response = json.loads(response.decode())

        if response.get("data") is not None:
            data = json.loads(response.get("data"))
            return ServerActions.__entidade_from_dict(data, dict_chave)

    @staticmethod
    def ObtemTodasEntidades(server_socket, server_dict, action):
        # exemplo ('alunos', 'matricula', 'ra' <-- necessariamente 'r')
        dict_chave, _, __, ___ = ServerActions.action_to_keys(action)

        action = (
            "read_students"
            if dict_chave == ServerActions.topic_alunos
            else (
                "read_teachers"
                if dict_chave == ServerActions.topic_professores
                else "read_disciplines"
            )
        )

        msg = json.dumps(
            {
                "function": action,
            }
        )

        server_socket.send(msg.encode())
        response = server_socket.recv(2048)
        response = json.loads(response.decode())

        if response.get("data") is None:
            return iter([])
        else:
            entidades = []

            for data in response.get("data"):
                data = json.loads(data)

                entidades.append(ServerActions.__entidade_from_dict(data, dict_chave))

            return iter(entidades)

    @staticmethod
    def __dict_from_entidade(entidade, dict_chave):
        if type(entidade) is pb2.Identificador:
            return {"id": entidade.id}
        if dict_chave == ServerActions.topic_alunos:
            return {"matricula": entidade.matricula, "nome": entidade.nome}
        elif dict_chave == ServerActions.topic_professores:
            return {"siape": entidade.siape, "nome": entidade.nome}
        elif dict_chave == ServerActions.topic_disciplinas:
            return {
                "sigla": entidade.sigla,
                "nome": entidade.nome,
                "vagas": entidade.vagas,
            }

    @staticmethod
    def __entidade_from_dict(entidade_dict, dict_chave):
        if dict_chave == ServerActions.topic_alunos:
            return pb2.Aluno(
                matricula=entidade_dict["matricula"], nome=entidade_dict["nome"]
            )
        elif dict_chave == ServerActions.topic_professores:
            return pb2.Professor(
                siape=entidade_dict["siape"], nome=entidade_dict["nome"]
            )
        else:
            return pb2.Disciplina(
                sigla=entidade_dict["sigla"],
                nome=entidade_dict["nome"],
                vagas=entidade_dict["vagas"],
            )
