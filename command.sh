
#!/bin/bash

# Define input and output paths
INPUT_FOLDER="data/arun/pf/"
OUTPUT_CSV="out/arun/pf.csv"

# Run the Python script
python src/extract_pf.py --input-folder "$INPUT_FOLDER" --output-csv "$OUTPUT_CSV"
