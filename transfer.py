import icat
import requests
import ConfigParser
import os
import argparse
import uuid
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


# Logs in to export ICAT client using parameters specified in config, returns session ID
def export_login():
    export_client = icat.client.Client(export_config['url'])
    return export_client.login('simple', {'username': export_config['username'], 'password': export_config['password']})


# Logs in to import ICAT client using parameters specified in config, returns session ID
def import_login():
    import_client = icat.client.Client(import_config['url'])
    return import_client.login('simple', {'username': import_config['username'], 'password': import_config['password']})


# Generates a random uuid and returns it
def uuid_gen():
    x = True
    while x:
        generated = str(uuid.uuid1())
        if not os.path.exists(generated + '.txt'):
            x = False
            return generated


# Uses session ID, and arguments specified at the command line, to return data from export ICAT server
def export_data():
    payload = {
        'json': '{"sessionId":"' + export_id + '", "query":"' + args.query + '", "attributes":"' + args.attributes + '"}'}
    return requests.get('https://icatdev15.isis.cclrc.ac.uk/icat/port', params=payload)


# Writes the exported data to specified data_file
#
# :param data: the exported ICAT data
def write_data(data):
    with open(data_file, 'w') as f:
        f.write(data.text)


# Streams exported data to import ICAT server, returns request operation details
def post_data():
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
            'https://icat-dev.isis.stfc.ac.uk/icat/port',
            files=files,
            verify=False
        )


# Assigns the status codes from the post request to report strings and returns them
#
# :param code: http status code from request.post().status_code
def code_assignment(code):
    if code == 412 and args.duplicate == 'throw':
        code = 'throw'
    if code == 412 and args.duplicate == 'check':
        code = 'check'
    return {
        'throw': 'New data is duplicate of old or ' + args.query + ' does not exist.',
        'check': 'New data does not match old data or ' + args.query + ' does not exist.',
        204: 'Operation Successful',
        412: args.query + ' is not an entity',
        403: 'Only root users may import all attributes'
    }[code]


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('config.ini')

    export_config = section_reader('export')
    import_config = section_reader('import')

    args = add_arguments()
    attribute_assign()

    export_id = export_login()
    import_id = import_login()

    data_file = 'test.txt' #uuid_gen() + '.txt'
    return_export_data = export_data()
    print return_export_data
    print return_export_data.status_code
    print return_export_data.text

    write_data(return_export_data)

    #status_code = post_data().status_code
    #print code_assignment(status_code)
    return_import_data = post_data()
    print return_import_data
    print return_import_data.status_code
    print return_import_data.text

    #os.remove(data_file)
