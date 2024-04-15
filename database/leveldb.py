import copy
import plyvel
import json
from pysyncobj import SyncObj, replicated

class Database(SyncObj):
    def __init__(self, port, part, primary, secundary):
        super(Database, self).__init__(primary, secundary)
        self.database = f'database/leveldb/{part}/{port}'
        
    # @replicated(sync = True)
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

    # @replicated
    def edit_data(self, key, value): 
        db = plyvel.DB(self.database, create_if_missing=True) 
               
        bytes_key = bytes(key, 'utf-8')       
        db.delete(bytes_key)

        bytes_value = bytes(json.dumps(value),'utf-8')
        db.put(bytes_key, bytes_value)

        db.close()
    
    # @replicated
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
            value = value.decode()
            value_object = json.loads(value)

            if 'matricula' in value_object:
                response.append(value_object)

        db.close()
        return response
    
    def get_all_teachers(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value = value.decode()
            value_object = json.loads(value)

            if 'siape' in value_object:
                response.append(value_object)

        db.close()
        return response
    
    def get_all_disciplines(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value = value.decode()
            value_object = json.loads(value)

            if 'sigla' in value_object:
                response.append(value_object)
                response.pop('teachers', None)
                response.pop('students', None)

        db.close()
        return response
    
    def add_teacher_at_discipline(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, 'utf-8')
        discpline_bytes = db.get(bytes_key)

        if not discpline_bytes:
            db.close()
            return False
        
        discipline_object = json.loads(discpline_bytes.decode())
        discipline_object['teacher'] = value 
        bytes_value = bytes(json.dumps(discipline_object),'utf-8')

        db.delete(bytes_key)
        db.put(bytes_key, bytes_value)

        db.close()
        return True
    
    def add_student_at_discipline(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, 'utf-8')
        discpline_bytes = db.get(bytes_key)

        if not discpline_bytes:
            db.close()
            return False
        
        discipline_object = json.loads(discpline_bytes.decode())
        students = discipline_object['students'] if discipline_object[''] is not None else []

        if value not in students:
            students.append(value)

        discipline_object['students'] = students
        bytes_value = bytes(json.dumps(discipline_object),'utf-8')

        db.delete(bytes_key)
        db.put(bytes_key, bytes_value)

        db.close()
        return True
    
    def remove_teacher_from_discipline(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, 'utf-8')
        discpline_bytes = db.get(bytes_key)

        if not discpline_bytes:
            db.close()
            return False
        
        discipline_object = json.loads(discpline_bytes.decode())
        discipline_object['teacher'] = None 
        bytes_value = bytes(json.dumps(discipline_object),'utf-8')

        db.delete(bytes_key)
        db.put(bytes_key, bytes_value)

        db.close()
        return True
    
    def remove_student_at_discipline(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, 'utf-8')
        discpline_bytes = db.get(bytes_key)

        if not discpline_bytes:
            return False
        
        discipline_object = json.loads(discpline_bytes.decode())
        students = discipline_object['students'] if discipline_object[''] is not None else []

        if value not in students:
            students.remove(value)

        discipline_object['students'] = students
        bytes_value = bytes(json.dumps(discipline_object),'utf-8')

        db.delete(bytes_key)
        db.put(bytes_key, bytes_value)

        return True
    
    def get_teacher_disciplines(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for k, v in db.iterator():
            aux = {}

            if k.contains('sigla'):
                v = v.decode()
                v_object = json.loads(v)

                v_aux = copy.deepcopy(v_object)
                v_aux.remove('teacher', None)
                v_aux.remove('students', None)

                aux.add({'discipline': v_aux})
                aux.add({'teacher': None})
                aux.add({'students': []})

                if v_object['teacher'] is not None and v_object['teacher'] == key:
                    bytes_key = bytes(key, 'utf-8')
                    response_bytes = db.get(bytes_key)

                    if response_bytes:
                        teacher = response_bytes.decode()
                        teacher_object = copy.deepcopy(json.loads(teacher))

                        aux['teacher'] = teacher_object

                    if v_object['students'] is not None:
                        students_objects = []

                        for s in v_object['students']:
                            bytes_key = bytes(s, 'utf-8')
                            response_bytes = db.get(bytes_key)

                            if response_bytes:
                                student = response_bytes.decode()
                                student_object = copy.deepcopy(json.loads(student))

                                students_objects.append(student_object)

                        aux['students'] = students_objects

                response.append(aux)

        db.close()
        return response