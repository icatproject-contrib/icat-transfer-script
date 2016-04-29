import icat
import requests
import ConfigParser
import os
import argparse
import uuid
import json
import warnings
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


# Logs in to an ICAT client using parameters specified in config, returns session ID
#
# :param client_name: the name of the client defined, config_name: the name of the config defined
def client_login(client_name, config_name):
    return client_name.login(config_name['auth'], {'username': config_name['username'], 'password': config_name['password']})


# Returns the version of the export ICAT server as a float
def get_version():
    return float(json.loads(requests.get(export_config['url'] + '/icat/version').text)['version'][0:3])


# Returns the entity transfer limit of export server
def get_limit():
    return json.loads(requests.get(export_config['url'] + '/icat/properties').text)['maxEntities']


# Adds  arguments to the command line command, returns parsed arguments in object form
def add_arguments(toggle_limit):
    parser = argparse.ArgumentParser(description='Move an entity from one ICAT database to another')
    parser.add_argument('query',
                        help='Defines the ICAT entity to be exported.'
                        )
    parser.add_argument('duplicate',
                        choices=['throw', 'ignore', 'check', 'overwrite'],
                        help='Defines the action to be taken if a duplicate is found. Throw: throw an exception. Ignore: go to the next row. Check: check that new data matches the old - and throw exception if it does not. Overwrite: replace old data with new.'
                        )
    parser.add_argument('-all',
                        action='store_true',
                        help='All fields will be moved (default: values for modId, create Id, modDate and createdate will not be moved). This option is only available to those specified in the rootUserNames in the icat.properties file.'
                        )
    if toggle_limit:
        parser.add_argument('limit',
                            type=int,
                            help='The maximum number of entities you want to transfer in one chunk')
    return parser.parse_args()


# If -all has been toggled sets args.attributes to 'all', else 'user'
def attribute_assign():
    if args.all:
        args.attributes = 'all'
    else:
        args.attributes = 'user'


# Returns the count of entities on the ICAT, through the use of python ICAT API
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
        'json': '{"sessionId":"' + export_id + '", "query":"SELECT entity FROM ' + args.query + ' entity LIMIT ' + str(current_pos) + ',' + str(limit) + '", "attributes":"' + args.attributes + '"}'}
    return requests.get(export_config['url'] + '/icat/port', params=payload)


# Writes the exported data to specified data_file
#
# :param data: the data to be written to disk, data_file: the filename for the file
def write_data(data, data_file):
    with open(data_file, 'w') as f:
        f.write(data.text.encode('utf8'))


# Posts stream of data taken from specified data file
#
# :param data_file: the name of the file to be streamed
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


# Also if the operation is not successful it issues an error report
# Returns error either 'successfully' or 'with error'
#
# :param return_data: the data returned by an import request
def debug(return_data):
    error = 'successfully'
    if return_data.status_code != 204 and return_data.status_code != 200:
        error = 'with error'
        return_text = json.loads(return_data.text)
        print 'Error: ' + return_text['code'] + ': ' + return_text['message']
    return error


# Prints out the number of entities processed as of execution
#
# :param error: the suffix added onto the end of the entities processed message
def print_position(error):
    if current_pos + limit >= entities:
        print str(entities) + ' of ' + str(entities) + ' entities processed ' + error
    else:
        print str(current_pos + limit) + ' of ' + str(entities) + ' entities processed ' + error


# Executes a series of functions to transfer the data
def transfer_data():
    data_file = uuid_gen() + '.txt'

    get_return = export_data()
    if debug(get_return) == 'with error':
        exit()
    print 'Data exported to memory'
    write_data(get_return, data_file)
    print 'Data written to disk'

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        post_return = post_data(data_file)

    print_position(debug(post_return))

    os.remove(data_file)

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('config.ini')

    export_config = section_reader('export')
    import_config = section_reader('import')

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        export_client = icat.client.Client(export_config['url'] + '/ICATService/ICAT?wsdl')
        import_client = icat.client.Client(import_config['url'] + '/ICATService/ICAT?wsdl')

    export_id = client_login(export_client, export_config)
    import_id = client_login(import_client, import_config)

    version = get_version()
    if version > 4.5:
        args = add_arguments(False)
        limit = get_limit()
    else:
        args = add_arguments(True)
        limit = args.maxEntities

    attribute_assign()
    entities = get_entities()
    data_left = True
    current_pos = 0

    while data_left:
        transfer_data()
        if current_pos + limit >= entities:
            data_left = False
        else:
            current_pos += limit
