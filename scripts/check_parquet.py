import pyarrow.parquet as pq

file = pq.ParquetFile("filesys/test/test_write/parquet.test")

print(f'num_row_groups: {file.num_row_groups}')
print(f'metadata: {file.metadata}')
print(f'compression: {file.metadata.row_group(0).column(0).compression}')
print(file.schema)