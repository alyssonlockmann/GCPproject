from google.cloud import bigquery

class BigQueryFuncs():
    # Atributos padrões:
    __CREDENTIALS_FILE = 'credentials/cloud-bigquery.json'
    __URI_STORAGE = 'gs://cloud_project-1/'
    __POST_TABLE_ID = 'cloud-storage-289515.storage_dataset.posts_table'

    # Construção do cliente BigQuery:
    client = bigquery.Client.from_service_account_json(__CREDENTIALS_FILE)
    
    @classmethod
    def loadPostTableData(self, storage_file):
        ''' Carrega dados na tabela posts_table. 
        \nO paramêtro "storage_file" recebe uma string com o nome do arquivo no bucket.'''
        # Configurando campos:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('postId', 'INTEGER'),
                bigquery.SchemaField('id', 'INTEGER'),
                bigquery.SchemaField('name', 'STRING'),
                bigquery.SchemaField('email', 'STRING'),
                bigquery.SchemaField('body', 'STRING'),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, # OBS: Só irá funcionar para arquivos onde o separador do JSON for \n(nova linha)
        )

        # Solicitando a criação da tabela via API:
        print(' -> Carregando dados na tabela {} {}...'.format(self.__POST_TABLE_ID, self.__name__))
        try:
            load_job = self.client.load_table_from_uri(
                self.__URI_STORAGE + storage_file, 
                self.__POST_TABLE_ID, 
                job_config=job_config
            )
            load_job.result()
            print(' -> Job finalizado com sucesso!')

        except Exception as inst:
            print('*** Erro ao carregar dados na tabela {} \n*** Arquivo origem: {} \n*** Erro no job: {}'.format(self.__POST_TABLE_ID, self.__URI_STORAGE, load_job.path))
            raise inst
        
    @classmethod
    def listPostsTableRows(self, lines):
        ''' Lista N linhas da tabela posts_table.
        \n O paramêtro "lines" recebe inteiro com o nr de linhas.'''
        explain_table = self.client.list_rows(self.__POST_TABLE_ID,max_results=lines)
        for row in explain_table:
            print('     * Linha: {}'.format(row))