import argparse
# from argparse import Namespace

args = None


def process_arguments(version_description):
    global args
    parser = argparse.ArgumentParser(description=version_description)
    parser.add_argument('--files', '-f', dest='files', required=True, help='Comma-delimited list of CSV files', default='')
    parser.add_argument('--config', '-c', dest='config', help='Path to configuration file', default='./resources/properties.toml')
    parser.add_argument('--destination', '-d', dest='dest', help='Destination table rs for resource_groups, rd for reference_data, cd for cost data', required=True, choices=['rd', 'rs', 'cd'])
    # parser.add_argument('--period', '-p', dest='period', help='Only required cost data. Month of the cost data, format YYYY-MM-DD')

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
