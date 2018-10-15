import random
import string

NUM_ROWS = 1000
NUM_COLS = 5
TYPES = ["int", "int", "string", "int", "float"]

def rand_string():
    length = random.randrange(10, 128)
    to_r = ""

    for _ in range(length):
        to_r += random.choice(string.ascii_letters)
    return to_r

        
for _ in range(NUM_ROWS):
    row = ""
    for t in TYPES:
        if t == "int":
            row += str(random.randrange(-10000, 10000))
        elif t == "string":
            row += rand_string()
        elif t == "float":
            row += str(random.random())
        else:
            raise NotImplementedError
        row += ","
    row = row[:-1]
    print(row)
