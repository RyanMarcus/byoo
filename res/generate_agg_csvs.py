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

NUM_ROWS = 50000
NUM_COLS = 3

print("c1,c2,c3")
for _ in range(NUM_ROWS):
    print(random.randrange(0, 5),
          random.randrange(-10000, 10000),
          random.random(),
          sep=",")
