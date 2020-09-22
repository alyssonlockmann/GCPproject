from libs import JsonFuncs as js
from libs import StorageFuncs as st
from libs import BigQueryFuncs as bf

# Paths dos arquivos de dados:
input_file = 'files/file_posts.json'
output_file = 'files/json-formatted-bq.json'

# Path/nome arquivos para upload no storage:
input_formatted_file = output_file
storage_file_name = 'storage-json-file.json'
storage_bucket = 'cloud_project-1' #--- Altera para o teu bucket no Storage

# Nome do projeto e do dataset no BigQuery:
bq_project = 'cloud-storage-289515' #--- Altera para o teu projeto no BigQuery
bq_dataset = 'storage_dataset' #--- Altere para o teu dataset no BigQuery

# Criando arquivo JSON formatado para o BigQuery:
print('Chamando método: {} da classe: {} '.format(js.JsonFuncs.__name__, js.JsonFuncs.createBQFormattedJson.__name__))
js.JsonFuncs.createBQFormattedJson(input_file,output_file)

# Fazendo upload do arquivo para o bucket:
print('Chamando método: {} da classe: {} '.format(st.StorageFuncs.uploadObjectIntoBucket.__name__, st.StorageFuncs.__name__))
st.StorageFuncs.uploadObjectIntoBucket(storage_bucket, input_formatted_file, storage_file_name)

# Carregando dados na tabela do BigQuery:
print('Chamando método: {} da classe: {} '.format(bf.BigQueryFuncs.loadPostTableData.__name__, bf.BigQueryFuncs.__name__))
bf.BigQueryFuncs.loadPostTableData(storage_file_name, storage_bucket, bq_project, bq_dataset)

# Listando N linhas da tabela posts_table:
print('Chamando método: {} da classe: {} '.format(bf.BigQueryFuncs.listPostsTableRows.__name__, bf.BigQueryFuncs.__name__))
bf.BigQueryFuncs.listPostsTableRows(5, bq_project, bq_dataset)