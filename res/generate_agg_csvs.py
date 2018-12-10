import random
import string

NUM_ROWS = 50000
NUM_COLS = 3

print("c1,c2,c3")
for _ in range(NUM_ROWS):
    print(random.randrange(0, 5),
          random.randrange(-10000, 10000),
          random.random(),
          sep=",")
