#!./venv/bin/python3
#!/usr/bin/python3

# one-file imp.  i'm sorry if that hurts your feelings
# default config file: ~/.qingstor/config.yaml

import os
import sys
from argparse   import ArgumentParser
from difflib    import get_close_matches

from qingstor.sdk.service.qingstor  import QingStor
from qingstor.sdk.service.bucket    import Bucket
from qingstor.sdk.config            import Config

# the server side READ, WRITE, FULL_CONTROL should be
#   QC_RO, QC_WO, QC_RW respectively

# global macros
VERSION = '0.0.1 alpha rc1 rev2'
INDENT  = ' ' * 4
NEWLINE = '\n' + INDENT

BUFSIZE = 1024 * 1024 * 4

HTTP_OK                 = 200
HTTP_OK_CREATED         = 201
HTTP_OK_NO_CONTENT      = 204
HTTP_OK_PARTIAL_CONTENT = 206


class BaseAction(object):
    command     = ''
    usage       = ''
    description = ''

    conn        = None

    @classmethod
    def add_common_arguments(self, parser, args):
        # FIX: get default config file path
        config = Config()
        default_conf_path = config.get_user_config_file_path()
        parser.add_argument(
            '-f', 
            '--config', 
            dest    = 'conf_file', 
            default = default_conf_path, 
            help    = 'Config file location'
        )

        # get zone info. from config file
        options = parser.parse_known_args(args)
        # index 0 is known arg for -f or --config option
        options = options[0]

        conf    = self.get_config(options.conf_file)
        parser.add_argument(
            '-z', 
            '--zone', 
            dest    = 'zone', 
            default = conf.zone, 
            help    = 'On which zone', 
        )

    @classmethod
    def add_ext_arguments(self, parser):
        pass

    @classmethod
    def get_argument_parser(self, args):
        parser = ArgumentParser(
            prog        = 'qs_cli %s' % self.command, 
            usage       = self.usage, 
            description = self.description, 
        )
        self.add_common_arguments(parser, args)
        self.add_ext_arguments(parser)
        return parser

    @classmethod
    def get_config(self, path):
        config = Config()
        try:
            config.load_config_from_filepath(path)
        except:
            print('[ERROR] failed to load config info from %s' % path)
            sys.exit(-1)
        return config

    @classmethod
    def get_connection(self, conf):
        try:
            key_id      = conf.qy_access_key_id
        except AttributeError:
            print('[ERROR] cannot find key id info in config file')
            sys.exit(-1)

        try:
            secret_key  = conf.qy_secret_access_key
        except AttributeError:
            print('[ERROR] cannot find secret key info in config file')
            sys.exit(-1)

        try:
            zone        = conf.zone
        except AttributeError:
            print('[ERROR] cannot find zone info in config file')
            sys.exit(-1)

        config      = Config(key_id, secret_key)
        service     = QingStor(config)
        return service

    @classmethod
    def send_request(self, options):
        return None

    @classmethod
    def main(self, args):
        # parse config file path
        parser      = self.get_argument_parser(args)

        # parse arguments
        options     = parser.parse_args(args)

        # load config from config file
        conf   = self.get_config(options.conf_file)
        if conf is None: sys.exit(-1)

        # get a connection from server
        self.conn   = self.get_connection(conf)

        return self.send_request(options)

class NoAction(BaseAction):
    pass

class ListBucketsAction(BaseAction):
    command = 'list-buckets'
    usage   = '%(prog)s [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        pass

    @classmethod
    def send_request(self, options):
        resp = self.conn.list_buckets(options.zone)
        print(resp.status_code, options.zone, resp.content.decode())

class CreateBucketAction(BaseAction):
    command = 'create-bucket'
    usage   = '%(prog)s -b <bucket> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest        = 'bucket', 
            required    = True, 
            help        = 'The bucket name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp = bucket.put()
        if resp.status_code == HTTP_OK_CREATED:
            print("Bucket %s at %s created successfully" 
                        % (options.bucket, options.zone))
        else:
            print(resp.status_code, resp.res.reason, resp.content.decode())

class DeleteBucketAction(BaseAction):
    command = 'delete-bucket'
    usage   = '%(prog)s -b <bucket> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest        = 'bucket', 
            required    = True, 
            help        = 'The bucket name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket  = self.conn.Bucket(options.bucket, options.zone)
        resp    = bucket.delete()
        if resp.status_code != HTTP_OK_NO_CONTENT:
            print(resp.status_code, resp.content.decode())
        else:
            print('Bucket %s at %s deleted successfully' 
                        % (options.bucket, options.zone))

class HeadBucketAction(BaseAction):
    command = 'head-bucket'
    usage   = '%(prog)s -b <bucket> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.head()
        print(resp.status_code, resp.res.reason)

class StatsBucketAction(BaseAction):
    command = 'stats-bucket'
    usage   = '%(prog)s -b <bucket> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest = 'bucket', 
            required = True, 
            help = 'The bucket name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.get_statistics()
        print(resp.status_code, resp.res.reason, resp.content.decode())

class ListObjectsAction(BaseAction):
    command = 'list-objects'
    usage   = '%(prog)s -b <bucket> [-z <zone> -p <prefix> ' \
              '-d <delimiter> -m <marker> -l <limit> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-p', 
            '--prefix', 
            dest = 'prefix', 
            help = 'The specified prefix that returned keys should start with', 
        )
        parser.add_argument(
            '-d', 
            '--delimiter', 
            dest = 'delimiter', 
            help = 'Which character to use for grouping the keys', 
        )
        parser.add_argument(
            '-m', 
            '--marker', 
            dest = 'marker', 
            help = 'The key to start with when listing objects in the bucket', 
        )
        parser.add_argument(
            '-l', 
            '--limit', 
            dest    = 'limit', 
            type    = int, 
            help    = 'The maximum number of keys returned', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.list_objects(
                    options.delimiter, 
                    options.limit, 
                    options.marker, 
                    options.prefix
                )
        print(resp.status_code, resp.res.reason, resp.content.decode())

class GetBucketAclAction(BaseAction):
    command = 'get-bucket-acl'
    usage   = '%(prog)s -b <bucket> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.get_acl()
        print(resp.status_code, resp.res.reason, resp.content.decode())

class SetBucketAclAction(BaseAction):
    command = 'set-bucket-acl'
    usage   = '%(prog)s -b <bucket> -A <acl> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-A', 
            '--acl', 
            required = True, 
            nargs    = '*', 
            help     = 'ACL entries, each entry is in format type, id | name, '
                'permission. permission can be READ, WRITE or FULL_CONTROL. '
                'Multiple entries are separated by spaces '
                'default global group name is QS_ALL_USERS', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        acl = []

        if not options.acl:
            print('[ERROR] Argument -A or --acl is empty')
            sys.exit(-1)

        for pairs in options.acl:

            parts = pairs.split(',')

            if len(parts) != 3:
                print('[ERROR] Argument -A or --acl is wrong')
                sys.exit(-1)
            
            t, grantee, perm = parts
            if t == 'user':
                grantee = {'type' : t, 'id' : grantee}
            elif t == 'group':
                grantee = {'type' : t, 'name' : grantee}
            else:
                print('[ERROR] Wrong grantee type %s' % t)
                sys.exit(-1)

            acl.append({
                'grantee' : grantee, 
                'permission' : perm, 
            })

        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.put_acl(acl)
        print(resp.status_code, resp.res.reason, resp.content.decode())

class CreateObjectAction(BaseAction):
    command = 'create-object'
    usage   = '%(prog)s -b <bucket> -k <key> -F <file> -d <data> ' \
                            '[-t <type> -z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-F', 
            '--file', 
            dest = 'file', 
            help = 'The object file', 
        )
        parser.add_argument(
            '-d', 
            '--data', 
            dest = 'data', 
            help = 'The object data', 
        )
        parser.add_argument(
            '-t', 
            '--type', 
            dest    = 'type', 
            default = 'application/octet-stream', 
            help    = 'The object type', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        if options.file:
            if not os.path.isfile(options.file):
                print('[ERROR] No such file %s' % options.file)
                sys.exit(-1)
            key  = options.key or os.path.basename(options.file)
            data = open(options.file, 'rb')
        elif options.data:
            key  = options.key
            if not key:
                print('[ERROR] Must specify -k or --key argument')
                sys.exit(-1)
            data = options.data
        else:
            print('[ERROR] must specify -F, --file, -d or --data argument')
            sys.exit(-1)

        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.put_object(key, body = data)
        print(resp.status_code, resp.res.reason, resp.content.decode())

class GetObjectAction(BaseAction):
    command = 'get-object', 
    usage   = '%(prog)s -b <bucket> -k <key> ' \
                '[-F <file> -B <bytes> -z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-F', 
            '--file', 
            dest = 'file', 
            help = 'The file that the object should save to', 
        )
        parser.add_argument(
            '-B', 
            '--bytes', 
            dest = 'bytes', 
            help = 'The object data range', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        if options.file:
            if os.path.isdir(options.file):
                path = '%s/%s' % (options.file, options.key)
            else:
                path = options.file
        else:
            path = '%s/%s' % (os.getcwd(), options.key)

        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            print('[ERROR] No such directory %s' % directory)
            sys.exit(-1)

        ranges = ''
        if options.bytes:
            ranges = 'bytes=%s' % options.bytes

        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.get_object(object_key = options.key, range=ranges)

        if resp.status_code in(HTTP_OK, HTTP_OK_PARTIAL_CONTENT):
            with open(path, 'wb') as f:
                while True:
                    try:
                        buf = b''.join(resp.iter_content(BUFSIZE))
                    except:     # chunk size less than BUFSIZE
                        break
                    if not buf: break
                    f.write(buf)
            print(os.path.basename(path), '(' + str(os.path.getsize(path)) 
                                        + ' bytes) written successfully')
        else:
            print(resp.status_code, resp.res.reason, resp.content.decode())

class DeleteObjectAction(BaseAction):
    command = 'delete-object'
    usage   = '%(prog)s -b <bucket> -k <key> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.delete_object(options.key)
        # FIX: server side should not return 204 when key not exists
        print(resp.status_code, resp.res.reason, resp.content.decode())

class HeadObjectAction(BaseAction):
    command = 'head-object'
    usage   = '%(prog)s -b <bucket> -k <key> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.head_object(options.key)
        if resp.status_code == HTTP_OK:
            print(resp.status_code, resp.res.reason, resp.headers)
        else:
            print(resp.status_code, resp.res.reason)

class InitiateMultipartAction(BaseAction):
    command = 'initiate-multipart'
    usage   = '%(prog)s -b <bucket> -k <key> ' \
                '[-t <type> -z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-t', 
            '--type', 
            dest    = 'type', 
            default = 'application/octet-stream', 
            help    = 'The object type', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.initiate_multipart_upload(options.key, options.type)
        print(resp.status_code, resp.res.reason, resp.content.decode())

class UploadMultipartAction(BaseAction):
    command = 'upload-multipart'
    usage   = '%(prog)s -b <bucket> -k <key> -u <upload_id> -p <part_number>' \
                      ' -F <file> -d <data> [-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-u', 
            '--upload-id', 
            dest     = 'upload_id', 
            required = True, 
            help     = 'ID for the initiated multipart upload', 
        )
        parser.add_argument(
            '-p', 
            '--part-number', 
            dest     = 'part_number', 
            required = True, 
            type     = int, 
            help     = 'The number of upload part', 
        )
        parser.add_argument(
            '-F', 
            '--file', 
            dest = 'file', 
            help = 'The object file', 
        )
        parser.add_argument(
            '-d', 
            '--data', 
            dest = 'data', 
            help = 'The object data', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        if options.file:
            if not os.path.isfile(options.file):
                print("[ERROR] No such file %s" % options.file)
                sys.exit(-1)
            data = open(options.file, "rb")
        elif options.data:
            data = options.data
        else:
            print("[ERROR] Must specify -F, --file, -d or --data argument")
            sys.exit(-1)

        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.upload_multipart(
                    options.key, 
                    options.part_number, 
                    options.upload_id, 
                    body = data
                )

        print(resp.status_code, resp.res.reason, resp.content.decode())

class ListMultipartAction(BaseAction):
    command = 'list-multipart'
    usage   = '%(prog)s -b <bucket> -k <key> -u <upload_id> ' \
            '[-p <part_number_marker> -l <limit> -z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-u', 
            '--upload-id', 
            dest     = 'upload_id', 
            required = True, 
            help     = 'ID for the initiated multipart upload', 
        )
        parser.add_argument(
            '-p', 
            '--part-number-marker', 
            dest     = 'part_number_marker', 
            type     = int, 
            help     = 'The number to start with when listing multipart', 
        )
        parser.add_argument(
            '-l', 
            '--limit', 
            dest = 'limit', 
            type = int, 
            help = 'The maximum number of parts returned', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.list_multipart(
                    options.key, 
                    options.limit, 
                    options.part_number_marker, 
                    options.upload_id
                )

        print(resp.status_code, resp.res.reason, resp.content.decode())

class CompleteMultipartAction(BaseAction):
    command = 'complete-multipart'
    usage   = '%(prog)s -b <bucket> -k <key> -u <upload_id> ' \
                '-P <part_numbers> [-e <etag> -z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-u', 
            '--upload-id', 
            dest     = 'upload_id', 
            required = True, 
            help     = 'ID for the initiated multipart upload', 
        )
        parser.add_argument(
            '-P', 
            '--part-numbers', 
            nargs    = '*', 
            type     = int, 
            required = True, 
            help     = 'The number of multiparts', 
        )
        parser.add_argument(
            '-e', 
            '--etag', 
            dest = 'etag',  
            help = 'The checksum value of the object', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        parts = []
        for part_number in options.part_numbers:
            parts.append({
                'part_number' : part_number, 
            })

        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.complete_multipart_upload(
                    options.key, 
                    options.upload_id, 
                    options.etag, 
                    object_parts = parts, 
                )

        print(resp.status_code, resp.res.reason, resp.content.decode())

class AbortMultipartAction(BaseAction):
    command = 'abort-multipart'
    usage   = '%(prog)s -b <bucket> -k <key> -u <upload_id> ' \
                                '[-z <zone> -f <conf_file>]'

    @classmethod
    def add_ext_arguments(self, parser):
        parser.add_argument(
            '-b', 
            '--bucket', 
            dest     = 'bucket', 
            required = True, 
            help     = 'The bucket name', 
        )
        parser.add_argument(
            '-k', 
            '--key', 
            dest     = 'key', 
            required = True, 
            help     = 'The object name', 
        )
        parser.add_argument(
            '-u', 
            '--upload-id', 
            dest     = 'upload_id', 
            required = True, 
            help     = 'ID for the initiated multipart upload', 
        )
        return parser

    @classmethod
    def send_request(self, options):
        bucket = self.conn.Bucket(options.bucket, options.zone)
        resp   = bucket.abort_multipart_upload(options.key, options.upload_id)
        print(resp.status_code, resp.res.reason, resp.content.decode())


class ActionManager(object):
    dispatch_table = [
        ('list-buckets', ListBucketsAction), 

        ('create-bucket', CreateBucketAction), 
        ('delete-bucket', DeleteBucketAction), 
        ('head-bucket', HeadBucketAction), 
        ('stats-bucket', StatsBucketAction), 
        ('list-objects', ListObjectsAction), 

        ('get-bucket-acl', GetBucketAclAction), 
        ('set-bucket-acl', SetBucketAclAction), 

        ('create-object', CreateObjectAction), 
        ('get-object', GetObjectAction), 
        ('delete-object', DeleteObjectAction), 
        ('head-object', HeadObjectAction), 

        ('initiate-multipart', InitiateMultipartAction), 
        ('upload-multipart', UploadMultipartAction), 
        ('list-multipart', ListMultipartAction), 
        ('complete-multipart', CompleteMultipartAction), 
        ('abort-multipart', AbortMultipartAction), 
    ]

    @classmethod
    def get_valid_actions(self):
        return [item[0] for item in self.dispatch_table]

    @classmethod
    def get_action(self, action):
        for item in self.dispatch_table:
            if item[0] == action:
                return item[1]
        return NoAction


def exit_due_to_invalid_action(valid_actions, suggest_actions = None):
    usage = NEWLINE + '%(prog)s <action> [parameters]\n\n'  \
            + 'here are valid actions: \n'                  \
            + INDENT + NEWLINE.join(valid_actions)

    if (suggest_actions):
        usage += '\n\ninvalid action, do you mean: \n'      \
                + INDENT + NEWLINE.join(suggest_actions)

    parser = ArgumentParser(
        prog  = 'qs_cli', 
        usage = usage, 
    )
    parser.add_argument('-v', '--version', 
        help = 'print version', action = 'store_true')
    parser.print_help()
    sys.exit(-1)

def get_valid_actions():
    return ActionManager.get_valid_actions()

def chk_args(args):
    valid_actions = get_valid_actions()

    if len(args) < 2: exit_due_to_invalid_action(valid_actions)

    if args[1] in ('-v', '--version'):
        print('qs_cli version %s' % VERSION)
        sys.exit(0)

    if args[1] not in valid_actions:
        suggest_actions = get_close_matches(args[1], valid_actions)
        exit_due_to_invalid_action(valid_actions, suggest_actions)

def get_action(action):
    return ActionManager.get_action(action)

def main():
    args = sys.argv
    chk_args(args)
    action = get_action(args[1])
    action.main(args[2:])

if __name__ == '__main__':
    main()

