# Arnaud de Latour, Sylvain Durand
# 2015 – MIT license


import os


# Define and create directories

path = dict(
    tmp = 'download',
    out = 'output',
)

for key, dir in path.items():
    os.makedirs(dir, exist_ok=1)
