
# Funcionalidade criada para formatar arquivos JSON com o delimitador que é aceito pelo BigQuery.

import json
import pandas as pd

class JsonFuncs():

    @classmethod
    def createBQFormattedJson(self, input_file, output_file):
        ''' Crian arquivo JSON formatado para o BigQuery 
        \nO paramêtro "input_file" e "output_file" recebem STRING'''
        try:
            df = pd.read_json(input_file)
            print(' -> Arquivo carregado com sucesso! INPUT_FILE= {}'.format(input_file))
        except Exception as inst:
            print('*** Erro ao carregar arquivo!!! INPUT_FILE= {}'.format(input_file))
            raise inst

        # Tranformando dataframe em um dicionário:
        jsonfile = df.to_dict('records')

        # Criando arquivo para upload no storage:
        try:
            with open(output_file, 'w') as outfile:
                for entry in jsonfile:
                    json.dump(entry, outfile)
                    outfile.write('\n')
            print(' -> Arquivo criado com sucesso! OUTPUT_FILE= {}'.format(output_file))
        except Exception as inst:
            print('*** Erro ao criar arquivo!!! OUTPUT_FILE= {}'.format(output_file))
            raise inst