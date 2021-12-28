from helpers.queries import query_pararelize_services
from preparation import *
import time
import sys

start_time = time.time()  # to check time execute


if __name__ == '__main__':

    try:
        # running asycn prosess
        loop = asyncio.get_event_loop()

        # state 0
        ops_reader_data = asyncio.ensure_future(operation_read_data(query_pararelize_services.select_service_data_mart_model_sds.format(date_start='2021-12-01', date_end='2021-12-03', cluster_mod= '6', cluster='0')))
        act_reader_data = loop.run_until_complete(ops_reader_data)

        ops_prepare_data = asyncio.ensure_future(operation_prepare_data(act_reader_data[0]))
        act_prepare_data = loop.run_until_complete(ops_prepare_data)

        ops_ingest_data = asyncio.ensure_future(operation_ingestion_data(act_prepare_data[0]))
        act_ingest_data = loop.run_until_complete(ops_ingest_data)
        print('state 0 clear \n')
        


    except:
        print('Error ingestion in : \n', sys.exc_info())
    else:
        print('Success ingestion data')

    print("--- %s seconds ---" % (time.time() - start_time))
