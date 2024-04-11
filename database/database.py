from __future__ import print_function
from leveldb import LevelDB

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
            key = response_msg['key']
            value = response_msg['value']
            
        if function_name == 'insert':           
            replica.insert_data(key, value)            
            resp = json.dumps({'msg': "Insert realizado com sucesso."})

        if function_name == 'edit':           
            replica.edit_data(key, value)            
            resp = json.dumps({'msg': "Edit realizado com sucesso."})

        if function_name == 'delete':
            replica.delete_data(key)
            resp = json.dumps({'msg': "Delete realizado com sucesso."})

        if function_name == 'read_students':
            response = replica.get_all_students(key)

            if response != '':
                response = json.loads(response)    
            
            resp = json.dumps({'data': response})
        
        if function_name == 'read_teachers':
            response = replica.get_all_teachers(key)

            if response != '':
                response = json.loads(response)    
            
            resp = json.dumps({'data': response})

        if function_name == 'read_disciplines':
            response = replica.get_all_disciplines(key)

            if response != '':
                response = json.loads(response)    
            
            resp = json.dumps({'data': response})

        conn.send(resp.encode())

def run():
    if len(sys.argv) < 2:
        print("Informe a partição do banco desejada (0, 1 ou 2).")
        sys.exit(-1)

    arg = int(sys.argv[1])

    if arg not in [0, 1, 2]:
        print("Escolha: 0, 1 ou 2")
        sys.exit(-1)
    
    # Réplica 0
    if arg == 0:
        port = 10000
        replica = LevelDB(port, 'particao00', 'localhost:20001',['localhost:20002', 'localhost:20003'])
    # Réplica 1
    if arg == 1:
        port = 10001
        replica = LevelDB(port, 'particao00', 'localhost:20002',['localhost:20001', 'localhost:20003'])
    # Réplica 2        
    if arg == 2:
        port = 10002
        replica = LevelDB(port, 'particao00', 'localhost:20003',['localhost:20001', 'localhost:20002'])
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', port))
        s.listen()

        while True:
            conn, addr = s.accept()
            threading.Thread(target=database_functions, args=(replica, conn, addr)).start()

if __name__ == '__main__':
    run()