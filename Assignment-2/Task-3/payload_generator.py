import random
import string

class PayloadGenerator:
    def __init__(self, limit):
        self.available_ids = set(range(0, limit))
        self.allocated_ids = set()

    def generate_random_payload(self, endpoint):
        if endpoint == "/read":
            return self._read_payload()
        elif endpoint == "/write":
            return self._write_payload(random.randint(1, 10))
        elif endpoint == "/update":
            return self._update_payload()
        elif endpoint == "/delete":
            return self._delete_payload()
        else:
            raise ValueError("Invalid endpoint.")

    def _read_payload(self):
        low = random.randint(100000, 999999)
        high = random.randint(low, 999999)
        return {"Stud_id": {"low": low, "high": high}}

    def _write_payload(self, num_students):
        data = []
        for _ in range(num_students):
            if len(self.available_ids) == 0:
                break
            Stud_id = random.choice(tuple(self.available_ids))
            self.available_ids.remove(Stud_id)
            self.allocated_ids.add(Stud_id)
            Stud_name = ''.join(random.choices(
                string.ascii_letters, k=random.randint(5, 20)))
            Stud_marks = random.randint(0, 100)
            data.append(
                {'Stud_id': Stud_id, 'Stud_name': Stud_name, 'Stud_marks': Stud_marks})
        return {'data': data}

    def _updated_payload(self):
        if len(self.allocated_ids) == 0:
            raise ValueError("No allocated student IDs.")
        Stud_id = random.choice(tuple(self.allocated_ids))
        Stud_name = ''.join(random.choices(
            string.ascii_letters, k=random.randint(5, 20)))
        Stud_marks = random.randint(0, 100)
        return {'Stud_id': Stud_id,
                'data': {'Stud_id': Stud_id, 'Stud_name': Stud_name, 'Stud_marks': Stud_marks}}

    def _delete_payload(self):
        if len(self.allocated_ids) == 0:
            raise ValueError("No allocated student IDs.")
        Stud_id = random.choice(tuple(self.allocated_ids))
        self.allocated_ids.remove(Stud_id)
        self.available_ids.add(Stud_id)
        return {'Stud_id': Stud_id}
