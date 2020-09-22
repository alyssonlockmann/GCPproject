from google.cloud import bigquery

class BigQueryFuncs():
    # Atributos padrões:
    __CREDENTIALS_FILE = 'credentials/cloud-bigquery.json'

    # Construção do cliente BigQuery:
    client = bigquery.Client.from_service_account_json(__CREDENTIALS_FILE)

    @classmethod
    def loadPostTableData(self, storage_file, bucket, project, dataset):
        ''' Carrega dados na tabela posts_table. 
        \nO paramêtro "storage_file" recebe uma string com o nome do arquivo no bucket.
        \nO paramêtro "bucket" recebe uma string com o nome do bucket.
        \nO paramêtro "project" recebe uma string com o nome do projeto.
        \nO paramêtro "dataset" recebe uma string com o nome do dataset.'''

        # Criando uri:
        uri_file = 'gs://{}/{}'.format(bucket,storage_file)

        # Criando table_id:
        table_id = '{}.{}.{}'.format(project , dataset , 'posts_table')

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
        print(' -> Carregando dados na tabela {} ...'.format(table_id))
        try:
            load_job = self.client.load_table_from_uri(
                uri_file, 
                table_id, 
                job_config=job_config
            )
            load_job.result()
            print(' -> Job finalizado com sucesso!')

        except Exception as inst:
            print('*** Erro ao carregar dados na tabela {} \n*** Arquivo origem: {} \n*** Erro no job: {}'.format(table_id, uri_file, load_job.path))
            raise inst
        
    @classmethod
    def listPostsTableRows(self, lines, project, dataset):
        ''' Lista N linhas da tabela posts_table.
        \n O paramêtro "lines" recebe inteiro com o nr de linhas.
        \n O paramêtro "project" recebe uma string com o com o nome do projeto.
        \n O paramêtro "dataset" recebe uma string com o com o nome do dataset.'''

        table_id = '{}.{}.{}'.format(project, dataset, 'posts_table')
        query = """
            SELECT *
            FROM {}
            LIMIT {}
        """.format(table_id, lines)
        try:
            query_job = self.client.query(query)
        except Exception as inst:
            print('*** Erro ao executar query!!!')
            raise inst

        for row in query_job:
            print("* Ids: {}  Email: {}".format(row['id'], row['email']))