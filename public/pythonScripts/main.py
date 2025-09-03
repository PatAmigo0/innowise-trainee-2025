import numpy as np

np.random.shuffle(random_array:=np.arange(100))
random_array[(random_array >= 3) & (random_array <= 8)] *= -1;
print(random_array)
