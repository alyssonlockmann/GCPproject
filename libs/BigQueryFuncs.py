from google.cloud import bigquery

class BigQueryFuncs():
    # Atributos padrões:
    __CREDENTIALS_FILE = 'credentials/cloud-bigquery.json'

    # Construção do cliente BigQuery:
    client = bigquery.Client.from_service_account_json(__CREDENTIALS_FILE)
    
    @classmethod
    def loadPostTableData(self, storage_file, uri, dataset_id):
        ''' Carrega dados na tabela posts_table. 
        \nO paramêtro "storage_file" recebe uma string com o nome do arquivo no bucket.
        \nO paramêtro "uri" recebe uma string com o caminho do bucket no storage.
        \nO paramêtro "dataset_id" recebe uma string com o nome do dataset.'''

        # Criando table_id:
        table_id = dataset_id + '.posts_table'

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
        print(' -> Carregando dados na tabela {} {}...'.format(table_id, self.__name__))
        try:
            load_job = self.client.load_table_from_uri(
                uri + storage_file, 
                table_id, 
                job_config=job_config
            )
            load_job.result()
            print(' -> Job finalizado com sucesso!')

        except Exception as inst:
            print('*** Erro ao carregar dados na tabela {} \n*** Arquivo origem: {} \n*** Erro no job: {}'.format(table_id, uri, load_job.path))
            raise inst
        
    @classmethod
    def listPostsTableRows(self, lines, dataset_id):
        ''' Lista N linhas da tabela posts_table.
        \n O paramêtro "lines" recebe inteiro com o nr de linhas.
        \n O paramêtro "dataset_id" recebe inteiro com o com o nome do dataset.'''

        table_id = dataset_id + '.posts_table'
        explain_table = self.client.list_rows(table_id,max_results=lines)
        for row in explain_table:
            print('     * Linha: {}'.format(row))