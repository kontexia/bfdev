import os
import shutil
import csv
import json
import pandas as pd
import redis
from redisgraph import Node, Edge, Graph, Path
from redisgraph_bulk_loader.bulk_insert import bulk_insert
import timeit
from click.testing import CliRunner

uid_for_database_types = {'redis': 'id',
                          'arango_db': '_key'}


def insert_items(item_name, items):
    start = timeit.default_timer()
    print("Creating", str(len(items)), item_name, "nodes")
    for item in items:
        item_node = Node(label=item_name, properties={'id': item, 'name': item})
        transaction_graph.add_node(item_node)
    stop = timeit.default_timer()
    duration = stop - start
    print('Time:', duration, 'per', item_name, ':', duration / len(items))


def insert_node(parameters=None):
    transaction_graph.add_node(Node(label=parameters['label'],
                                    properties={'id': parameters['id'],
                                                'name': parameters['name']}))

'''
Utility function to format the raw input data csv file into several csv files for nodes and edges to be fed as inputs
to the redisgraph bulk loader command line tool.
parameters :
'column_names': list of lists (or of tuples with an operator) of columns headers of the raw input csv file to copy after being undergoing an operation
                (+,-,*,/ or _ which stands for concatenation)
                has more than one column header.
'labels': list of column headers of the output csv file
'uid': lists of columns headers of the raw input csv file to concatenate to produce a unique id
'graph_element': tuple where the graph element is either 'node' or 'edge' and the second is the name of the graph element
                 that will be used as the name of the csv file produced
'from': for edge collections only, the name of the edge source collection
'to': for edge collections only, the name of the edge destination collection

parameters={'transaction': {'column_names': [['step'],['amount'],['nameOrig'],['oldbalanceOrg'],
                                                          ['newbalanceOrig'],['nameDest'],['oldbalanceDest'],
                                                          ['newbalanceDest'],['isFraud'],['isFlaggedFraud']],
                             'labels': ['step','amount','nameOrig','oldbalanceOrg','newbalanceOrig','nameDest','oldbalanceDest','newbalanceDest','isFraud','isFlaggedFraud'],
                             'uid': ['step', 'nameOrig', 'nameDest'],
                             'graph_element': ('node', 'transaction')},
             'has_sent': {'column_names': [['nameOrig'], (['step', 'nameOrig', 'nameDest'], '_')],
                          'from': 'payer',
                          'to': 'transition',
                          'graph_element': ('edge', 'has_sent')}
              }
'''
def generate_files(generated_file_parameters=None,
                   data_folder_path=None, df=None, parameters=None):
    for generated_file_parameter in generated_file_parameters:
        print('raw_dataset_path.generate_files - start', generated_file_parameter['database_type'])
        start_1 = timeit.default_timer()

        print('raw_dataset_path.generate_files - drop_duplicates start...')
        df_copy = df.drop_duplicates(parameters['drop_duplicates']) if parameters.get('drop_duplicates', False) else df.copy()
        row_count = len(df_copy.index)
        stop_1 = timeit.default_timer()
        duration_1 = stop_1 - start_1
        print('raw_dataset_path.generate_files - drop_duplicates completed - time:', duration_1, 'per row:', duration_1/row_count)

        print('raw_dataset_path.generate_files - column operations start...')
        start_2 = timeit.default_timer()
        df_columns = []
        uid = uid_for_database_types[generated_file_parameter['database_type']]
        for column_list in parameters['column_names']:
            if isinstance(column_list, tuple):
                if column_list[1] == '+':
                    df_columns.append(df_copy[column_list[0]].sum(axis=1))
                elif column_list[1] == '-':
                    df_columns.append(df_copy[column_list[0][0]] - df_copy[column_list[0][1]])
                elif column_list[1] == '*':
                    df_columns.append(df_copy[column_list[0][0]].multiply(df_copy[column_list[0][1]], axis='index'))
                elif column_list[1] == '/':
                    df_columns.append(df_copy[column_list[0][0]] / df_copy[column_list[0][1]])
                else:
                    df_columns.append(df_copy[column_list[0]].astype(str).agg('_'.join, axis=1))
            else:
                df_columns.append(df_copy[column_list].astype(str).agg('_'.join, axis=1))

        is_edge_collection = parameters['graph_element'][0] == 'edge'
        stop_2 = timeit.default_timer()
        duration_2 = stop_2 - start_2
        print('raw_dataset_path.generate_files - column operations completed - time:', duration_2, 'per row:', duration_2/row_count)

        print('raw_dataset_path.generate_files - concat start...')
        start_3 = timeit.default_timer()
        file_df = pd.concat(df_columns, axis=1, keys=['_from', '_to', 'relation'] if is_edge_collection else parameters['labels'])
        stop_3 = timeit.default_timer()
        duration_3 = stop_3 - start_3
        print('raw_dataset_path.generate_files - concat completed - time:', duration_3, 'per row:', duration_3/row_count)

        print('raw_dataset_path.generate_files - insert uid column start...')
        start_4 = timeit.default_timer()
        if is_edge_collection:
            if generated_file_parameter['database_type'] == 'arango_db':
                file_df['_from'] = parameters['from'] + '/' + file_df['_from']
                file_df['_to'] = parameters['to'] + '/' + file_df['_to']
            else:
                file_df['relation'] = parameters['graph_element'][1]
        else:
            file_df[uid] = file_df[parameters['uid']].astype(str).agg('_'.join, axis=1)
            first_column = file_df.pop(uid)
            file_df.insert(0, uid, first_column)
        stop_4 = timeit.default_timer()
        duration_4 = stop_4 - start_4
        print('raw_dataset_path.generate_files - insert uid column completed - time:', duration_4, 'per row:', duration_4/row_count)

        print('raw_dataset_path.generate_files - split start...')
        start_5 = timeit.default_timer()
        split_dataframe(file_df, 500000, data_folder_path + generated_file_parameter['database_type'] + '/',
                        parameters['graph_element'][1], generated_file_parameter['file_extension'])
        stop_5 = timeit.default_timer()
        duration_5 = stop_5 - start_5
        print('raw_dataset_path.generate_files - split completed - time:', duration_5, 'per row:', duration_5/row_count)

        stop = timeit.default_timer()
        duration = stop - start_1
        print('generate_files',
              generated_file_parameter['database_type']+'/'+parameters['graph_element'][1] +'.'+generated_file_parameter['file_extension'],
              '- time:', duration, 'per row:', duration/row_count)


def split_dataframe(dataframe, size, folder_path, file_name, file_extension):
    file_path_prefix = folder_path + '/' + file_name
    counter = 0
    for i in range(0, len(dataframe), size):
        file_path = file_path_prefix + str(counter) + '.' + file_extension
        if file_extension == 'csv':
            dataframe.loc[i:i + size - 1, :].to_csv(file_path, index=False)
        elif file_extension == 'json':
            dataframe.loc[i:i + size - 1, :].to_json(file_path, orient='records')
        counter += 1


def create_formatted_data(raw_dataset_path, formatted_data_folder_path, collection_parameters, generated_file_parameters,
                          head_size=None):
    start = timeit.default_timer()
    print('raw_data_harvester.create_formatted_data - start creating dataframe from raw data file...')
    df = pd.read_csv(raw_dataset_path)
    print('raw_data_harvester.create_formatted_data - dataframe created')
    if head_size:
        df = df.head(head_size)

    for generated_file_parameter in generated_file_parameters:
        shutil.rmtree(formatted_data_folder_path + generated_file_parameter['database_type'])
        os.mkdir(formatted_data_folder_path + generated_file_parameter['database_type'])

    for collection_name in collection_parameters:
        print('raw_dataset_path.create_formatted_data - generate_files start for', collection_name, len(df.index), 'rows')
        generate_files(generated_file_parameters=generated_file_parameters,
                      data_folder_path=formatted_data_folder_path,
                      df=df,
                      parameters=collection_parameters[collection_name])

    stop = timeit.default_timer()
    duration = stop - start
    print('raw_data_harvester.create_formatted_data complete in', duration)


def get_dict(file_path, extension):
    df = pd.read_csv(file_path) if extension == 'csv' else pd.read_json(file_path)
    return df.to_dict(orient='records')


if __name__ == "__main__":

    r = redis.Redis(host='localhost', port=6379)
    transaction_graph = Graph('paysim_graph', r)
    if False:
        try:
            transaction_graph.delete()
        except:
            print('graph could not be deleted')

    formatted_data_folder_path = r'/home/brunofontenla/Documents/PycharmProjects/transaction_generator/data/'
    raw_dataset_path = formatted_data_folder_path+'paysim_dataset.csv'

    collection_parameters = {'type': {'column_names': [['type']],
                                      'drop_duplicates': 'type',
                                      'labels': ['type'],
                                      'uid': ['type'],
                                      'graph_element': ('node', 'type')},
                             'payee': {'column_names': [['nameOrig']],
                                       'drop_duplicates': 'nameOrig',
                                       'labels': ['payer'],
                                       'uid': ['payer'],
                                       'graph_element': ('node', 'payer')},
                             'payer': {'column_names': [['nameDest']],
                                       'drop_duplicates': 'nameDest',
                                       'labels': ['payee'],
                                       'uid': ['payee'],
                                       'graph_element': ('node', 'payee')},
                             'transaction': {'column_names': [['step'],['amount'],['nameOrig'],['oldbalanceOrg'],
                                                          ['newbalanceOrig'],['nameDest'],['oldbalanceDest'],
                                                          ['newbalanceDest'],['isFraud'],['isFlaggedFraud']],
                                             'labels': ['step','amount','nameOrig','oldbalanceOrg','newbalanceOrig','nameDest','oldbalanceDest','newbalanceDest','isFraud','isFlaggedFraud'],
                                             'uid': ['step', 'nameOrig', 'nameDest'],
                                             'graph_element': ('node', 'transaction')},
                             'has_sent': {'column_names': [['nameOrig'], (['step', 'nameOrig', 'nameDest'], '_')],
                                          'from': 'payer',
                                          'to': 'transition',
                                          'graph_element': ('edge', 'has_sent')}
                             }
    generated_file_parameters = [#{'database_type': 'redis', 'file_extension': 'csv'},
                                 {'database_type': 'arango_db', 'file_extension': 'json'}]

    create_formatted_data(raw_dataset_path, formatted_data_folder_path, collection_parameters, generated_file_parameters)

    if False:
        df = df.head(20)

        format_to_csv(database_type='redis', df=df, parameters={'column_names': [['type']],
                                                                'labels': ['type'],
                                                                'uid': ['type'],
                                                                'graph_element': ('node', 'type')})
        format_to_csv(database_type='redis', df=df, parameters={'column_names': [['nameOrig']],
                                                                'labels': ['payer'],
                                                                'uid': ['payer'],
                                                                'graph_element': ('node', 'payer')})
        format_to_csv(database_type='redis', df=df, parameters={'column_names': [['nameDest']],
                                                                'labels': ['payee'],
                                                                'uid': ['payee'],
                                                                'graph_element': ('node', 'payee')})
        format_to_csv(database_type='redis', df=df, parameters={'database_type': 'redis',
                                         'column_names': [['step'],['amount'],['nameOrig'],['oldbalanceOrg'],
                                                          ['newbalanceOrig'],['nameDest'],['oldbalanceDest'],
                                                          ['newbalanceDest'],['isFraud'],['isFlaggedFraud']],
                                         'labels': ['step','amount','nameOrig','oldbalanceOrg','newbalanceOrig','nameDest','oldbalanceDest','newbalanceDest','isFraud','isFlaggedFraud'],
                                         'uid': ['step', 'nameOrig', 'nameDest'],
                                         'graph_element': ('node', 'transaction')})

        format_to_csv(database_type='redis', df=df, parameters={'database_type': 'redis',
                                         'column_names': [['nameOrig'], (['step', 'nameOrig', 'nameDest'], '_')],
                                         'labels': ['source','destination','relation'],
                                         'graph_element': ('edge', 'has_sent')})

        format_to_csv(database_type='redis', df=df, parameters={'database_type': 'redis',
                                         'column_names': [['nameOrig'],
                                                          (['oldbalanceOrg', 'amount'], '+'),
                                                          (['oldbalanceOrg', 'newbalanceOrig'], '-'),
                                                          (['amount', 'oldbalanceOrg'], '*'),
                                                          (['oldbalanceOrg', 'amount'], '/')],
                                         'labels': ['source','destination','relation'],
                                         'graph_element': ('edge', 'has_sent')})

        runner = CliRunner()
        start = timeit.default_timer()
        res = runner.invoke(bulk_insert, [#'--nodes', type_file_path,
                                          #'--nodes', payee_file_path,
                                          '--nodes', payer_file_path,
                                          '--nodes', transaction_file_path,
                                          '--relations', has_sent_file_path,
                                          '--max-token-count', 1,
                                          'paysim_graph'], catch_exceptions=False)
        print(res.output)
        stop = timeit.default_timer()
        duration = stop - start
        print('bulk_insert - time:', duration)

        start = timeit.default_timer()
        transaction_graph.commit()
        stop = timeit.default_timer()
        duration = stop - start
        print('commit - time:', duration)

        print('End')

