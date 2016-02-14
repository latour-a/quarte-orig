PYTHON=python

all: output/correspondances_anciennes_communes.csv \
	output/correspondances_communes2015.csv output/finances_locales.h5 \
	output/insee.h5 output/elections.h5 output/cog.csv

reset:
	rm -rf download output
	rm output/correspondances_anciennes_communes.csv
	rm output/correspondances_communes2015.csv
	rm output/finances_locales.h5
	rm output/insee.h5
	rm output/elections.h5
	rm output/cog.csv

output/correspondances_anciennes_communes.csv:
	$(PYTHON) correspondances.py

output/correspondances_communes2015.csv:
	$(PYTHON) correspondances.py

output/finances_locales.h5: output/correspondances_anciennes_communes.csv \
	output/correspondances_communes2015.csv
	$(PYTHON) finances_locales.py

output/insee.h5: output/correspondances_anciennes_communes.csv \
	output/correspondances_communes2015.csv
	$(PYTHON) insee.py

output/elections.h5: output/correspondances_anciennes_communes.csv \
	output/correspondances_communes2015.csv
	$(PYTHON) elections.py

output/cog.csv:
	$(PYTHON) choropleth-data.py
