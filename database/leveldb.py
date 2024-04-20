import copy
import plyvel
import json
from pysyncobj import replicated, SyncObjConf, SyncObj


class Database(SyncObj):
    def __init__(self, port, part, primary, secundary):
        cfg = SyncObjConf(dynamicMembershipChange=True)
        super(Database, self).__init__(primary, secundary, cfg)
        self.database = f"database/leveldb/{part}/{port}"

    @replicated
    def insert_data(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")
        bytes_value = bytes(value, "utf-8")

        if not db.get(bytes_key):
            db.put(bytes_key, bytes_value)
        else:
            db.delete(bytes_key)
            db.put(bytes_key, bytes_value)

        db.close()

    @replicated
    def edit_data(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")
        db.delete(bytes_key)

        bytes_value = bytes(value, "utf-8")
        db.put(bytes_key, bytes_value)

        db.close()

    @replicated
    def delete_data(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")

        db.delete(bytes_key)
        db.close()

    def get_data(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")
        response_bytes = db.get(bytes_key)

        print(response_bytes.decode())

        response = "" if not response_bytes else response_bytes.decode()

        db.close()
        return response

    def get_all_students(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value = value.decode()

            if "matricula" in value and "sigla" not in value:
                response.append(value)

        db.close()
        return response

    def get_all_teachers(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value = value.decode()

            if "siape" in value and "sigla" not in value:
                response.append(value)

        db.close()
        return response

    def get_all_disciplines(self):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for _, value in db.iterator():
            value = value.decode()

            if value[0] == '"':
                value = value[1:]
                value = value[:-1]

            value = value.replace("\\", "")
            value = value.replace("'", '"')

            value_object = json.loads(value)

            if "sigla" in value:
                value_object.pop("teachers", None)
                value_object.pop("students", None)
                response.append(json.dumps(value_object))

        db.close()
        return response

    @replicated
    def add_person_at_discipline(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")

        discpline_bytes = db.get(bytes_key)

        discipline = discpline_bytes.decode()
        discipline = json.loads(discipline)

        if "siape" in value:
            discipline["teacher"] = value
        else:
            students = discipline["students"] if "students" in discipline else ""
            students = students.split()

            print(discipline)

            if len(students) == ["vagas"]:
                return False

            if value not in students:
                students.append(value)

            students = " ".join(str(x) for x in students)
            discipline["students"] = students

        bytes_value = bytes(json.dumps(discipline), "utf-8")

        db.delete(bytes_key)
        db.put(bytes_key, bytes_value)

        db.close()

    @replicated
    def remove_person_at_discipline(self, key, value):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")

        discpline_bytes = db.get(bytes_key)

        discipline = discpline_bytes.decode()
        discipline = json.loads(discipline)

        if "siape" in value:
            discipline.pop("teacher", None)
        else:
            students = discipline["students"] if "students" in discipline else ""
            students = students.split()

            if value in students:
                students.remove(value)

            students = " ".join(str(x) for x in students)
            discipline["students"] = students

        bytes_value = bytes(json.dumps(discipline), "utf-8")

        db.delete(bytes_key)
        db.put(bytes_key, bytes_value)

        db.close()

    def detailed_discipline(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        bytes_key = bytes(key, "utf-8")

        discipline_bytes = db.get(bytes_key)
        discipline = discipline_bytes.decode()
        discipline = json.loads(discipline)

        response = {}

        if "teacher" in discipline:
            teacher_bytes = db.get(bytes(discipline["teacher"], "utf-8"))
            teacher = teacher_bytes.decode()

            response["teacher"] = json.loads(teacher)

        if "students" in discipline:
            st = discipline["students"].split()

            if st:
                students = []

                for s in st:
                    student_bytes = db.get(bytes(s, "utf-8"))
                    student = student_bytes.decode()
                    student = json.loads(student)

                    students.append(student)

                response["students"] = students

        discipline.pop("teacher", None)
        discipline.pop("students", None)
        response["discipline"] = discipline

        db.close()
        return response

    def get_teacher_disciplines(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for k, v in db.iterator():
            k = k.decode()
            aux = {}

            if "sigla" in k:
                v = v.decode()
                v_object = json.loads(v)

                v_aux = copy.deepcopy(v_object)
                v_aux.pop("teacher", None)
                v_aux.pop("students", None)

                aux["discipline"] = v_aux
                aux["teacher"] = None
                aux["students"] = []

                if "teacher" in v_object and v_object["teacher"] == key:
                    bytes_key = bytes(key, "utf-8")
                    response_bytes = db.get(bytes_key)

                    if response_bytes:
                        teacher = response_bytes.decode()
                        teacher_object = json.loads(teacher)

                        aux["teacher"] = teacher_object

                    if "students" in v_object:
                        students_objects = []

                        for s in v_object["students"].split():
                            bytes_key = bytes(s, "utf-8")
                            response_bytes = db.get(bytes_key)

                            if response_bytes:
                                student = response_bytes.decode()
                                student_object = json.loads(student)

                                students_objects.append(student_object)

                        aux["students"] = students_objects

                response.append(aux)

        db.close()
        return response
    
    def get_student_disciplines(self, key):
        db = plyvel.DB(self.database, create_if_missing=True)

        response = []

        for k, v in db.iterator():
            k = k.decode()
            aux = {}

            if "sigla" in k:
                v = v.decode()
                v_object = json.loads(v)

                v_aux = copy.deepcopy(v_object)
                v_aux.pop("teacher", None)
                v_aux.pop("students", None)

                aux["discipline"] = v_aux
                aux["teacher"] = None
                aux["students"] = 0

                if "students" in v_object and key in v_object["students"]:
                    bytes_key = bytes(key, "utf-8")
                    response_bytes = db.get(bytes_key)

                    aux["students"] = len(v_object["students"].split())

                    if "teacher" in v_object:
                        teacher = response_bytes.decode()
                        teacher_object = json.loads(teacher)

                        aux["teacher"] = teacher_object

                response.append(aux)

        db.close()
        return response
