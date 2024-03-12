import argparse
# from argparse import Namespace

args = None


def process_arguments(version_description):
    global args
    parser = argparse.ArgumentParser(description=version_description)
    parser.add_argument('--files', '-f', dest='files',
                        help='Comma-delimited list of CSV files',
                        required=True, default='')
    parser.add_argument('--config', '-c', dest='config',
                        help='Path to configuration file',
                        default='./resources/properties.toml')

    parser.add_argument('--destination', '-d', dest='dest',
                        help='Destination table rs for resource_groups, rd for reference_data, cd for cost data',
                        required=True, choices=['rd', 'rs', 'cd'])

    parser.add_argument('--operation', '-o', dest='operation',
                        help='Operation to perform Validate - no insert in DB - or Update',
                        choices=['Validate', 'Update'], default='Validate')

    # If parse fail will show help
    args = parser.parse_args()
    return args


def get_args():
    global args
    return args


def print_arguments():
    global args
    print(f"\nArguments:")
    print(f"\targuments: {args}")
