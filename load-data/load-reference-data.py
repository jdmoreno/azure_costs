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

RESOURCE_GROUPS_AFFIX_QUERY = ('SELECT affix FROM public.resource_groups WHERE subscription_name = %s and '
                               'resource_group = %s;')

REF_DATA_QUERY = 'SELECT value_c FROM public.reference_data WHERE reference_c = %s and key_c = %s;'

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
        if arguments.args.operation == 'Validate':
            print_missing_subs_rg()

    return


def config():
    try:
        # CMD Parameters
        command = "load-data"
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


def db_commit(conn):
    if arguments.args.operation == 'Update':
        conn.commit()


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
                db_commit(conn)
            i += 1

            # Exit for tests
            if 0 < MAX_ROWS_PER_QUERY <= i:
                break
        db_commit(conn)


def process_file_rs(conn, cursor, file: Path):
    # Process resource group files
    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            if arguments.args.operation == 'Update':
                cursor.execute(RESOURCE_GROUPS_INSERT_STATEMENT,
                               (row['SUBSCRIPTION-NAME'], row['RESOURCE-GROUP'], row['AFFIX']))

            if i % COMMIT_COUNT == 0:
                db_commit(conn)
            i += 1

            # Exit for tests
            if 0 < MAX_ROWS_PER_QUERY <= i:
                break
        db_commit(conn)


def process_file_rd(conn, cursor, file: Path):
    # Process reference data files
    with open(file, 'r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            if arguments.args.operation == 'Update':
                cursor.execute(REFERENCE_DATA_INSERT_STATEMENT,
                               (row['REFERENCE'], row['KEY'], row['VALUE']))

            if i % COMMIT_COUNT == 0:
                db_commit(conn)
            i += 1

            # Exit for tests
            if 0 < MAX_ROWS_PER_QUERY <= i:
                break
        db_commit(conn)


# def process_cd(file_name, cost_period, subscription_name, resource_group):
def process_cd(cursor, file_name, cost_period, subscription_name, subscription_id, resource_group, resource_group_id,
               resource, resource_id, resource_type, resource_location, tags, cost, currency, cost_usd):
    # Obtain Affix from Resource Groups
    cursor.execute(RESOURCE_GROUPS_AFFIX_QUERY, (subscription_name, resource_group))
    logger.debug("query executed: {}", cursor.query)
    affix = fetch_one_with_default(cursor, 'Not Found', 'RESOURCE_GROUP', subscription_name, resource_group)

    # Obtain Area from AREAS reference data
    cursor.execute(REF_DATA_QUERY, ('AREAS', affix))
    logger.debug("query executed: {}", cursor.query)
    area = fetch_one_with_default(cursor, 'Not Found', 'REF_DATA', 'AREAS', affix)

    # Obtain Capability from CAPABILITIES reference data
    cursor.execute(REF_DATA_QUERY, ('CAPABILITIES', affix))
    logger.debug("query executed: {}", cursor.query)
    capability = fetch_one_with_default(cursor, 'Not Found', 'REF_DATA', 'CAPABILITIES', affix)

    # Obtain Environment from ENVIRONMENTS reference data
    cursor.execute(REF_DATA_QUERY, ('ENVIRONMENTS', subscription_name))
    logger.debug("query executed: {}", cursor.query)
    environment = fetch_one_with_default(cursor, 'Not Found', 'REF_DATA', 'ENVIRONMENTS', subscription_name)

    if affix.find('eps') >= 0:
        logger.info(f'affix: {affix}, area: {area}, capability: {capability}, environment: {environment}')

    if arguments.args.operation == 'Update':
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

    return


def fetch_one_with_default(cursor, default, category, element_1, element_2):
    result_set = cursor.fetchone()

    if result_set is not None:
        result = result_set[0]
        logger.debug("result_set: {} - result: {}", result_set, result)
    else:
        logger.warning('category: {} - reference_c: {} - key_c: {}', category, element_1, element_2)
        result = default
        add_to_missing_subs_rg(category, element_1, element_2)
    return result


# Validate
missing_subs_rg = []
missing_areas = []
missing_capabilities = []
missing_environments = []


def clear_missing_subs_rg():
    global missing_subs_rg, missing_areas, missing_capabilities, missing_environments
    missing_subs_rg = []
    missing_areas = []
    missing_capabilities = []
    missing_environments = []


def add_to_missing_subs_rg(category, element_1, element_2):
    global missing_subs_rg

    # Create the item dict according to the category
    item = create_item(category, element_1, element_2)

    # Obtain the corresponding list
    list_missing_items = get_list(category, element_1)

    # append the item if not found in the list
    try:
        list_missing_items.index(item)
    except ValueError:
        list_missing_items.append(item)


def print_missing_subs_rg():
    with open('./output/missing_subs_rg.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['subscription_name', 'resource_group', 'affix'])
        writer.writeheader()
        writer.writerows(missing_subs_rg)

    with open('./output/missing_areas.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['reference_c', 'key_c', 'value_c'])
        writer.writeheader()
        writer.writerows(missing_areas)

    with open('./output/missing_capabilities.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['reference_c', 'key_c', 'value_c'])
        writer.writeheader()
        writer.writerows(missing_capabilities)

    with open('./output/missing_environments.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['reference_c', 'key_c', 'value_c'])
        writer.writeheader()
        writer.writerows(missing_environments)


def create_item(category, element_1, element_2):
    if category == 'RESOURCE_GROUP':
        return {
            'subscription_name': element_1,
            'resource_group': element_2,
            'affix': ''
        }
    else:
        if category == 'REF_DATA':
            return {
                'reference_c': element_1,
                'key_c': element_2,
                'value_c': ''
            }
        else:
            logger.error('Unknown category {}', category)


def get_list(category, element_1):
    global missing_subs_rg, missing_areas, missing_capabilities, missing_environments
    if category == 'RESOURCE_GROUP':
        return missing_subs_rg
    else:
        if category == 'REF_DATA':
            match element_1:
                case 'AREAS':
                    return missing_areas
                case 'CAPABILITIES':
                    return missing_capabilities
                case 'ENVIRONMENTS':
                    return missing_environments
                case _:
                    logger.error('Unknown element_1 {}', element_1)
        else:
            logger.error('Unknown category {}', category)


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr, level="WARNING", enqueue=True)
    # logger.add("./output/file.log", level="WARNING", colorize=False, enqueue=True)
    config()
    main()
