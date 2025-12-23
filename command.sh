
#!/bin/bash

# Define input and output paths
INPUT_FOLDER="data/arun/pf/"
OUTPUT_CSV="out/arun/pf.csv"

# Run the Python script
python src/extract_pf.py --input-folder "data/arun/pf/" --output-csv "out/arun/pf.csv"

python src/extract_pf.py --input-folder "data/kurinji/pf/" --output-csv "out/kurinji/pf.csv"

python src/extract_equity_icici.py --input-folder data/arun/equity/icici/tr-statements --output-csv out/arun/equity-icici.csv

python src/extract_equity_iifl.py --input-folder data/arun/equity/iifl/contract-notes --output-csv out/arun/equity-iifl.csv

python src/extract_equity_icici.py --input-folder data/kurinji/equity/icici/tr-statements --output-csv out/kurinji/equity-icici.csv

python src/extract_equity_geojit.py --input-folder data/kurinji/equity/geojit/contract-notes --output-csv out/kurinji/equity_geojit.py