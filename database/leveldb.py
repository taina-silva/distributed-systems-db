import plyvel
import json
from pysyncobj import SyncObj, replicated

class LevelDB(SyncObj):
    def __init__(self, port, part, primary, secundary):
        super(LevelDB, self).__init__(primary, secundary)
        self.database = f'database/leveldb/{part}/{port}'
        
    @replicated
    def insert_data(self, key, value): 
        db = plyvel.DB(self.database, create_if_missing=True) 

        bytes_key = bytes(key, 'utf-8')            
        bytes_value = bytes(json.dumps(value),'utf-8')

        if not db.get(bytes_key):
            db.put(bytes_key, bytes_value)
        else:
            db.delete(bytes_key)
            db.put(bytes_key, bytes_value)

        db.close()

    @replicated
    def edit_data(self, key, value): 
        db = plyvel.DB(self.database, create_if_missing=True) 
               
        bytes_key = bytes(key, 'utf-8')       
        db.delete(bytes_key)

        bytes_value = bytes(json.dumps(value),'utf-8')
        db.put(bytes_key, bytes_value)

        db.close()
        
    @replicated
    def delete_data(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, 'utf-8')

        db.delete(bytes_key)
        db.close()

    def get_data(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, 'utf-8')
        response_bytes = db.get(bytes_key)
        response = '' if not response_bytes else response_bytes.decode()

        db.close()
        return response
    
    def get_all_students(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value_object = json.loads(value)

            if 'matricula' in value_object:
                response.append(value)

        db.close()
        return response
    
    def get_all_teachers(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value_object = json.loads(value)

            if 'siape' in value_object:
                response.append(value)

        db.close()
        return response
    
    def get_all_disciplines(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value_object = json.loads(value)

            if 'sigla' in value_object:
                response.append(value)

        db.close()
        return response