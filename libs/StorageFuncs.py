from google.cloud import storage

class StorageFuncs():

    # Atributos padrões:
    __CREDENTIALS_FILE = 'credentials/cloud-bigquery.json'

    # Criando cliente do storage:
    __storage_client = storage.Client.from_service_account_json(__CREDENTIALS_FILE)


    @classmethod
    def upload_object_into_bucket(self, bucket_name, input_file, storage_file_name):
        ''' Faz o upload do objeto no bucket. 
        \nO paramêtro "bucket_name" recebe uma string o nome do bucket criado no storage
        \nO paramêtro "input_file" recebe uma string com o caminho do arquivo de dados
        \nO paramêtro "storage_file_name" recebe uma string com o nome do arquivo que será criado no bucket.'''

        # Definindo o bucket e o arquivo de destino:
        bucket = self.__storage_client.bucket(bucket_name)
        blob = bucket.blob(storage_file_name)

        # Fazendo upload do arquivo:
        try:
            blob.upload_from_filename(input_file)
            print(' -> Arquivo carrregado com sucesso!') 
            print(' -> Origem: {} \n -> Destino {}'.format(input_file, storage_file_name))
        except Exception as inst:
            print('*** Erro no upload do arquivo: {}'.format(input_file))
            raise inst

        # Listando conteudo do bucket:
        print(' -> Bucket: {}'.format(bucket_name))
        print('     Objetos do bucket:')
        bucket_blobs = self.__storage_client.list_blobs(bucket_name)
        for obj in bucket_blobs:
                print('         * {}'.format(obj.name))
        