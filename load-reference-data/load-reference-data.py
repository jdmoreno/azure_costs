import csv
# from datetime import date
import datetime as datetime
import sys
from pathlib import Path

import psycopg2
from loguru import logger

import modules.arguments as arguments
import modules.configuration as configuration

REFERENCE_DATA_TABLE = 'reference_data'
RESOURCE_GROUPS_TABLE = 'resource_groups'
COST_DATA_INSERT_STATEMENT = ('INSERT INTO public.cost_data('
                              'file_name, cost_period, '
                              'subscription_name, subscription_id, '
                              'resource_group, resource_group_id, '
                              'resource, resource_id, resource_type, resource_location, tags, '
                              'cost_c, cost_currency, cost_usd, '
                              'area, capability, environment)'
                              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);')

RESOURCE_GROUPS_INSERT_STATEMENT = ('INSERT INTO public.resource_groups('
                                    'subscription_name, resource_group, affix) VALUES(%s, %s, %s);')

REFERENCE_DATA_INSERT_STATEMENT = ('INSERT INTO public.reference_data('
                                   'reference_c, key_c, value_c) VALUES (%s, %s, %s);')

COMMIT_COUNT = 100
MAX_ROWS_PER_QUERY = -1


def main():

    with psycopg2.connect(
            host=configuration.db_config['host'],
            port=configuration.db_config['port'],
            dbname=configuration.db_config['dbname'],
            user=configuration.db_config['user'],
            password=configuration.db_config['password']
    ) as conn:
        with conn.cursor() as cursor:
            filelist = arguments.args.files.split(',')
            for fn in filelist:
                file_path = Path(fn)
                parent_path = file_path.parent
                file_name = file_path.name
                files = sorted(parent_path.rglob(file_name))
                for file in files:
                    print(f'Processing file {file}')
                    match arguments.args.dest:
                        case 'rs':
                            process_file_rs(conn, cursor, file)
                        case 'rd':
                            process_file_rd(conn, cursor, file)
                        case 'cd':
                            process_file_cost(conn, cursor, file)
                        case _:
                            print(f"Wrong destination table {arguments.args.dest}")
                            break
                    conn.commit()
    return


def config():
    try:
        # CMD Parameters
        command = "load-reference-data"
        version = '1.0'
        version_description = f"{command} - Load Azure Costs {version}"

        # Process arguments
        args = arguments.process_arguments(version_description)

        if args.files == '':
            print('You need to pass the CSV file names to load the reference data.')
            print(f'Example: {command}.py --file /path/file.csv or {command}.py -f /path/*.csv')
            return
    finally:
        arguments.print_arguments()
    # Read configuration
    try:
        config_path = args.config
        print(f"config path {config_path}")
        configuration.process_configuration(config_path)
    except OSError as err:
        print(f"OS error:", err)
        return
    except ValueError as err:
        print(f"Could not convert data to a number {err=}, {type(err)=}")
        return
    except KeyError as err:
        print(f"Configuration error {err=}, {type(err)=}")
        return
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        return
    finally:
        configuration.print_configuration()


def process_file_cost(conn, cursor, file: Path):
    file_name = file.stem
    parts = file_name.split('_')
    if len(parts) != 2:
        return

    month = parts[0][:3]
    year = int(parts[1])
    if year > 2000:
        year -= 2000
    file_date = datetime.datetime.strptime(f'{year}-{month}-01', '%y-%b-%d').date()

    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            process_cd(cursor, file_name, file_date, row['SubscriptionName'], row['SubscriptionId'],
                       row['ResourceGroup'], row['ResourceGroupId'], row['Resource'], row['ResourceId'],
                       row['ResourceType'], row['ResourceLocation'], row['Tags'], row['Cost'], row['Currency'],
                       row['CostUSD'])

            if i % COMMIT_COUNT == 0:
                conn.commit()
            i += 1

            # Exit for tests
            if 0 < MAX_ROWS_PER_QUERY <= i:
                break


def process_file_rs(conn, cursor, file: Path):
    # Process resource group files
    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            cursor.execute(RESOURCE_GROUPS_INSERT_STATEMENT,
                           (row['SUBSCRIPTION-NAME'], row['RESOURCE-GROUP'], row['AFFIX']))
            if i % COMMIT_COUNT == 0:
                conn.commit()
            i += 1

            # Exit for tests
            if 0 < MAX_ROWS_PER_QUERY <= i:
                break


def process_file_rd(conn, cursor, file: Path):
    # Process reference data files
    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            cursor.execute(REFERENCE_DATA_INSERT_STATEMENT,
                           (row['REFERENCE'], row['KEY'], row['VALUE']))
            if i % COMMIT_COUNT == 0:
                conn.commit()
            i += 1

            # Exit for tests
            if 0 < MAX_ROWS_PER_QUERY <= i:
                break


# def process_cd(file_name, cost_period, subscription_name, resource_group):
def process_cd(cursor, file_name, cost_period, subscription_name, subscription_id, resource_group, resource_group_id,
               resource, resource_id, resource_type, resource_location, tags, cost, currency, cost_usd):
    affix_query = 'SELECT affix FROM public.resource_groups WHERE subscription_name = %s and resource_group = %s;'
    cursor.execute(affix_query, (subscription_name, resource_group))

    logger.debug("query executed: {}", cursor.query)
    affix = fetch_one_with_default(cursor, 'Not Found')

    ref_data_query = 'SELECT value_c FROM public.reference_data WHERE reference_c = %s and key_c = %s;'
    cursor.execute(ref_data_query, ('AREAS', affix))
    logger.debug("query executed: {}", cursor.query)
    area = fetch_one_with_default(cursor, 'Not Found')

    cursor.execute(ref_data_query, ('CAPABILITIES', affix))
    logger.debug("query executed: {}", cursor.query)
    capability = fetch_one_with_default(cursor, 'Not Found')

    cursor.execute(ref_data_query, ('ENVIRONMENTS', subscription_name))
    logger.debug("query executed: {}", cursor.query)
    environment = fetch_one_with_default(cursor, 'Not Found')

    if affix.find('eps') >= 0:
        logger.info(f'affix: {affix}, area: {area}, capability: {capability}, environment: {environment}')

    cursor.execute(COST_DATA_INSERT_STATEMENT, (
        file_name, cost_period,
        subscription_name,
        subscription_id,
        resource_group,
        resource_group_id,
        resource,
        resource_id,
        resource_type,
        resource_location,
        tags,
        cost,
        currency,
        cost_usd,
        area, capability, environment))


def fetch_one_with_default(cursor, default):
    result_set = cursor.fetchone()

    if result_set is not None:
        result = result_set[0]
        logger.debug("result_set: {} - result: {}", result_set, result)
    else:
        result = default
    return result


def test(file_name, cost_period, subscription_name, resource_group):
    with psycopg2.connect(
            host=configuration.db_config['host'],
            port=configuration.db_config['port'],
            dbname=configuration.db_config['dbname'],
            user=configuration.db_config['user'],
            password=configuration.db_config['password']
    ) as conn:
        with conn.cursor() as cursor:
            process_cd(cursor, file_name, cost_period, subscription_name, row['SubscriptionId'], resource_group,
                       row['ResourceGroupId'], row['Resource'], row['ResourceId'], 1, 2, 3, 4, 5, 6)


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr, level="WARNING", enqueue=True)
    config()
    main()

    # test('file_name', 'cost_period', 'RMG Hosting Non Production Env', 'rmg-we-epscommon-a-rg-app-01')
