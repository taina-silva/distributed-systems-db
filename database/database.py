from __future__ import print_function
from leveldb import Database

import sys
import socket
import threading
import json
            
def database_functions(replica, conn, addr):
    while True:
        data = conn.recv(2048)
        msg = data.decode()

        print(msg)

        if msg:
            response_msg = json.loads(msg)
            function_name = response_msg['function']
            key = response_msg['key'] if 'key' in response_msg else None
            value = response_msg['value'] if 'value' in response_msg else None
            
        if msg and function_name == 'insert':   
            print(type(replica))

            replica.insert_data(key, value)
            resp = json.dumps({'msg': "Insert realizado com sucesso."})

        if msg and function_name == 'edit':           
            replica.edit_data(key, value)            
            resp = json.dumps({'msg': "Edit realizado com sucesso."})

        if msg and function_name == 'delete':
            replica.delete_data(key)
            resp = json.dumps({'msg': "Delete realizado com sucesso."})

        if msg and function_name == 'read':
            response = replica.get_data(key)

            if response != '':
                response = json.loads(response)   

                if key.contains('sigla'):
                    response.pop('teachers', None)
                    response.pop('students', None)
            
            resp = json.dumps({'data': response})

        if msg and function_name == 'read_students':
            response = replica.get_all_students()

            if response != '':
                resp = json.dumps({'data': response})
            else:
                resp = json.dumps({'data': []})
        
        if msg and function_name == 'read_teachers':
            response = replica.get_all_teachers()

            if response != '':
                resp = json.dumps({'data': response})
            else:
                resp = json.dumps({'data': []})

        if msg and function_name == 'read_disciplines':
            response = replica.get_all_disciplines()

            if response != '':                
                resp = json.dumps({'data': response})
            else:
                resp = json.dumps({'data': []})  

        if msg and function_name == 'add_teacher': 
            
            
        if msg:
            conn.send(resp.encode())

def run():
    if len(sys.argv) < 3:
        print("Informe a réplica (0, 1 ou 2) e a partição (0, 1) do banco.")
        sys.exit(-1)

    replica = int(sys.argv[1])
    particao = int(sys.argv[2])

    if replica not in [0, 1, 2]:
        print("Escolha 0, 1 ou 2 para réplica")
        sys.exit(-1)

    if particao not in [0, 1]:
        print("Escolha 0 ou 1 para partição")
        sys.exit(-1)
    
    if replica == 0:
        port = 10000
        database = Database(port, 'particao00' if particao == 0 else 'particao01', 'localhost:20001',['localhost:20002', 'localhost:20003'])
    elif replica == 1:
        port = 10001
        database = Database(port, 'particao00' if particao == 0 else 'particao01', 'localhost:20002',['localhost:20001', 'localhost:20003'])
    elif replica == 2:
        port = 10002
        database = Database(port, 'particao00' if particao == 0 else 'particao01', 'localhost:20003',['localhost:20001', 'localhost:20002'])

    sock = socket.socket()
    host = socket.gethostname()
    sock.bind((host, port))
    sock.listen(15)

    while True:
        conn, addr = sock.accept()
        threading.Thread(target=database_functions, args=(database, conn, addr)).start()

if __name__ == '__main__':
    run()