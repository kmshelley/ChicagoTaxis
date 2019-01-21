from elasticsearch import Elasticsearch
from bq_helper import BigQueryHelper
import pandas as pd
import os
from pprint import pprint


HOST = os.environ.get('ELASTICSEARCH_HOST', 'localhost')
PORT = os.environ.get('ELASTICSEARCH_PORT', 9200)


if __name__ == '__main__':

    # Create the ES index for taxi trip data
    es = Elasticsearch(hostname=HOST, port=PORT)
    
    body = {
        'settings': {
            "number_of_replicas" : 1
            },
        'mappings': {
            'trip': {
                'properties': {
                    'pickup_loc': {'type': 'geo_point'},
                    'dropoff_loc': {'type': 'geo_point'},
                    'trip_start_time': {'type': 'date'},
                    'trip_end_time': {'type': 'date'},
                    'fare': {'type': 'long'},
                    'extras': {'type': 'long'},
                    'tips': {'type': 'long'},
                    'tolls': {'type': 'long'},
                    'trip_miles': {'type': 'long'},
                    'trip_seconds': {'type': 'long'},
                    'trip_total': {'type': 'long'},
                    }
                }
            }
        }
    es.indices.create(index='chicago-taxis', body=body)
    
    # query for chicago taxi records using BigQuery
    chicago_taxis = BigQueryHelper(active_project="bigquery-public-data", dataset_name="chicago_taxi_trips")
    i = 0
    succeeded = 0
    failed = 0
    while True:
        # we need to iterate through the table because a single query is too large
        # we'll query 5000 records at a time
        query = "SELECT * from `bigquery-public-data.chicago_taxi_trips.taxi_trips` LIMIT 5000 OFFSET {}".format(5000 * i)
        df = chicago_taxis.query_to_pandas(query)

        if df.shape[0] == 0:
            break

        # drop any records without a pickup or dropoff location
        df.dropna(subset=['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude'], how='any', inplace=True)

        # convert the data frame to a list of dicts
        L = df.fillna('').to_dict(orient='records')

        for l in L:
            # convert lat long and date fields for storage in ES
            l['pickup_loc'] = {'lat': l['pickup_latitude'],
                               'lon': l['pickup_longitude']}
            l['dropoff_loc'] = {'lat': l['dropoff_latitude'],
                                'lon': l['dropoff_longitude']}

            # elasticsearch stores epoch time in milliseconds
            l['trip_start_time'] = int(l['trip_start_timestamp'].timestamp()) * 1000
            l['trip_end_time'] = int(l['trip_end_timestamp'].timestamp()) * 1000

            del l['pickup_latitude']
            del l['pickup_longitude']
            del l['dropoff_latitude']
            del l['dropoff_longitude']
            del l['trip_start_timestamp']
            del l['trip_end_timestamp']

            # cast everything else to a string
            for k in l.keys():
                if k not in ['pickup_loc', 
                             'dropoff_loc', 
                             'trip_start_time', 
                             'trip_end_time',
                             'fare',
                             'extras',
                             'tips',
                             'tolls',
                             'trip_miles',
                             'trip_seconds',
                             'trip_total']:
                    l[k] = str(l[k])
            # add the document to ES
            try:
                es.create(index='chicago-taxis', body=l, doc_type='trip', id=l['unique_key'])
                succeeded += 1
            except Exception as e:
                # print(e)
                failed += 1

        i += 1

    print('Done uploading documents. {} succeeded, {} failed.'.format(succeeded, failed))

    
        
