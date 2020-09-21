from libs import JsonFuncs as js
from libs import StorageFuncs as st
from libs import BigQueryFuncs as bf

# Paths dos arquivos de dados:
input_file = 'files/file_posts.json'
output_file = 'files/json-formatted-bq.json'

# Criando arquivo JSON formatado para o BigQuery:
print('Chamando método: {} da classe: {} '.format(js.JsonFuncs.__name__, js.JsonFuncs.createBQFormattedJson.__name__))
js.JsonFuncs.createBQFormattedJson(input_file,output_file)

# Path/nome arquivos para upload no storage:
input_formatted_file = 'files/json-formatted-bq.json'
storage_file_name = 'storage-json-file.json'

# Fazendo upload do arquivo para o bucket:
print('Chamando método: {} da classe: {} '.format(st.StorageFuncs.__name__, st.StorageFuncs.uploadObjectIntoBucket.__name__))
st.StorageFuncs.uploadObjectIntoBucket(input_formatted_file, storage_file_name)

# Carregando dados na tabela do BigQuery:
print('Chamando método: {} da classe: {} '.format(bf.BigQueryFuncs.__name__, bf.BigQueryFuncs.loadPostTableData.__name__))
bf.BigQueryFuncs.loadPostTableData(storage_file_name)

# Listando N linhas da tabela posts_table:
print('Chamando método: {} da classe: {} '.format(bf.BigQueryFuncs.__name__, bf.BigQueryFuncs.listPostsTableRows.__name__))
bf.BigQueryFuncs.listPostsTableRows(5)