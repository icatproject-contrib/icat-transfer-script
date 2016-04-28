import icat
import requests
import ConfigParser
import os
import argparse
import uuid
import json
__author__ = 'Cox-Andrew'


# Reads the content of a section, returns section in dictionary form
#
# :param section: the section to be read
def section_reader(section):
    section_content = {}
    options = config.options(section)
    for option in options:
        section_content[option] = config.get(section, option)
    return section_content


# Adds  arguments to the command line command, returns parsed arguments in object form
def add_arguments():
    parser = argparse.ArgumentParser(description='Move an entity from one ICAT database to another')
    parser.add_argument('query', help='Defines the ICAT entity to be exported.')
    parser.add_argument('duplicate', choices=['throw', 'ignore', 'check', 'overwrite'],
                        help='Defines the action to be taken if a duplicate is found. Throw: throw an exception. Ignore: go to the next row. Check: check that new data matches the old - and throw exception if it does not. Overwrite: replace old data with new.')
    parser.add_argument('-all', action='store_true',
                        help='All fields will be moved (default: values for modId, create Id, modDate and createdate will not be moved). This option is only available to those specified in the rootUserNames in the icat.properties file.')
    return parser.parse_args()


# If -all has been toggled sets args.attributes to 'all', else 'user'
def attribute_assign():
    if args.all:
        args.attributes = 'all'
    else:
        args.attributes = 'user'


# Logs in to an ICAT client using parameters specified in config, returns session ID
#
# :param
def client_login(client_name, config_name):
    return client_name.login('simple', {'username': config_name['username'], 'password': config_name['password']})


def get_icat_limit():
    return json.loads(requests.get(export_config['url'] + '/icat/properties').text)['maxEntities']


def get_entities():
    return export_client.search('SELECT count(entity) FROM ' + args.query + ' entity')[0]


# Generates a random uuid and returns it
def uuid_gen():
    while True:
        generated = str(uuid.uuid1())
        if not os.path.exists(generated + '.txt'):
            return generated


# Uses session ID, and arguments specified at the command line, to return data from export ICAT server
def export_data():
    payload = {
        'json': '{"sessionId":"' + export_id + '", "query":"SELECT entity FROM ' + args.query + ' entity LIMIT ' + str(current_pos) + ',' + str(increment) + '", "attributes":"' + args.attributes + '"}'}
    return requests.get(export_config['url'] + '/icat/port', params=payload)


# Writes the exported data to specified data_file
#
# :param data: the exported ICAT data
def write_data(data, data_file):
    with open(data_file, 'w') as f:
        f.write(data.text.encode('utf8'))


# Streams exported data to import ICAT server, returns request operation details
def post_data(data_file):
    with open(data_file, 'rb') as stream:
        files = {
            'data': (
                'data',
                stream,
                'application/octet-stream'
            ),
            'json': (
                '',
                '{"sessionId":"' + import_id + '", "duplicate":"' + args.duplicate + '", "attributes":"' + args.attributes + '"}',
                'text/plain'
            )
        }

        return requests.post(
            import_config['url'] + '/icat/port',
            files=files,
            verify=False
        )


# Assigns the status codes from the post request to report strings and returns them
#
# :param code: http status code from request.post().status_code
def debug(return_data):
    print return_data.text


def transfer_data():
    data_file = uuid_gen() + '.txt'

    get_return = export_data()
    write_data(get_return, data_file)

    post_return = post_data(data_file)
    debug(post_return)

    os.remove(data_file)

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('config.ini')

    export_config = section_reader('export')
    import_config = section_reader('import')

    args = add_arguments()
    attribute_assign()

    export_client = icat.client.Client(export_config['url'] + '/ICATService/ICAT?wsdl')
    import_client = icat.client.Client(import_config['url'] + '/ICATService/ICAT?wsdl')

    export_id = client_login(export_client, export_config)
    import_id = client_login(import_client, import_config)

    data_left = True
    current_pos = 0
    increment = get_icat_limit()
    entities = get_entities()

    while data_left:
        transfer_data()
        if current_pos + increment >= entities:
            data_left = False
        else:
            current_pos += increment
