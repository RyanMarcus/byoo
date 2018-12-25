# < begin copyright > 
# Copyright Ryan Marcus 2018
# 
# This file is part of byoo.
# 
# byoo is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# byoo is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with byoo.  If not, see <http://www.gnu.org/licenses/>.
# 
# < end copyright > 
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
