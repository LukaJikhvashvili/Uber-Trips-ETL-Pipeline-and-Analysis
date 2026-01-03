#!/bin/bash
set -euo pipefail

# Check for required environment variables
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ] || [ -z "$SNOWSQL_PWD" ]; then
  echo "Error: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD environment variables must be set."
  exit 1
fi

STAGE="@fhv_internal_stage"

for file in data/parquet/*/*.parquet; do
  filename=$(basename "$file")
  
  # Check if file name exists in stage
  echo "Checking if $filename exists in stage $STAGE..."
  exists=$(snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -d FHV_DB -s RAW -q "REMOVE $STAGE/$filename DRY_RUN=TRUE" -o output_format=csv -o header=false -o timing=false -o friendly=false)
  
  if [ -n "$exists" ]; then
    echo "Skipping $filename — already exists in stage"
  else
    echo "Uploading $filename..."
    snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -d FHV_DB -s RAW -q "PUT file://$(readlink -f "$file") $STAGE AUTO_COMPRESS=TRUE PARALLEL=16;"
    echo "Successfully uploaded $filename"
  fi
done

echo "All files processed."
