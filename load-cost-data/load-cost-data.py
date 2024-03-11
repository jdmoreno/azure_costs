import psycopg2
import csv
import modules.arguments as arguments
import modules.configuration as configuration
from pathlib import Path


def main():
    # Process CLI arguments
    try:
        # CMD Parameters
        version = '1.0'
        version_description = f"load-cost-data - Load Azure Costs {version}"

        # Process arguments
        args = arguments.process_arguments(version_description)

        if args.files == '':
            print('You need to pass the AWR HTML file name.')
            print('Example: AWR2Excel.py -file /path/awrfile.html or AWR2Excel.py -file /path/*.html')
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

    insert_statement = "INSERT INTO public.reference_data(reference_c, key_c, value_c)VALUES (%s, %s, %s);"
    filelist = args.files.split(',')

    with psycopg2.connect(
            host=configuration.db_config['host'],
            port=configuration.db_config['port'],
            dbname=configuration.db_config['dbname'],
            user=configuration.db_config['user'],
            password=configuration.db_config['password']
    ) as conn:
        with conn.cursor() as cursor:
            for fn in filelist:
                file_path = Path(fn)
                if file_path.exists():
                    print(file_path)
                    with open(file_path, 'r', newline='') as csv_file:
                        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                        i = 0
                        for row in csv_reader:
                            print(f"row {i}: {row}")
                            i += 1
                            cursor.execute(insert_statement, (row['REFERENCE'], row['KEY'], row['VALUE']))
                            if i % 100 == 0:
                                conn.commit()
                        conn.commit()
    return


if __name__ == '__main__':
    main()
