import os
import json
import logging
import hashlib
import struct
from collections import OrderedDict
from functools import wraps

# we import PollingObserver instead of Observer because the deleted event
# is not capturing https://github.com/gorakhargosh/watchdog/issues/46
from watchdog.observers.polling import PollingObserver as Observer
import keyring


# The path for config directory, config files and sharing folder
DEFAULT_CONFIG_FOLDER = os.path.join(os.environ['HOME'], '.PyBox')
DEFAULT_SHARING_FOLDER = os.path.join(os.environ['HOME'], 'sharing_folder')

# Calculate int size in the machine architecture
INT_SIZE = struct.calcsize('!i')

module_logger = logging.getLogger('daemon.utils')


WRAPPER_ASSIGNMENTS = ('__module__', '__name__', '__doc__')


def available_attrs(fn):
    """
    Return the list of functools-wrappable attributes on a callable.
    This is required as a workaround for http://bugs.python.org/issue3445.
    """
    return tuple(a for a in WRAPPER_ASSIGNMENTS if hasattr(fn, a))


def is_directory(method):
    """This decorator block directory event"""

    def decorator(method):
        @wraps(method, assigned=available_attrs(method))
        def inner(self, e):
            if e.is_directory:
                return
            return method(self, e)
        return inner
    return decorator(method)


def get_cmdmanager_request(socket):
    """
    Communicate with cmd_manager and get the request
    Returns the request decoded by json format or None if cmd_manager send connection closure
    """
    packet_size = socket.recv(INT_SIZE)
    if len(packet_size) == INT_SIZE:

        packet_size = int(struct.unpack('!i', packet_size)[0])
        packet = ''
        remaining_size = packet_size

        while len(packet) < packet_size:
            packet_buffer = socket.recv(remaining_size)
            remaining_size -= len(packet_buffer)
            packet = ''.join([packet, packet_buffer])

        req = json.loads(packet)
        return req
    else:
        return None


def set_cmdmanager_response(socket, message):
    """
    Makes cmd_manager response encoding it in json format and send it to cmd_manager
    """
    response = {'message': message}
    response_packet = json.dumps(response)
    socket.sendall(struct.pack('!i', len(response_packet)))
    socket.sendall(response_packet)
    return response_packet


def build_directory(dir_path):
    """
    Check that the given folder exists otherwise create it.
    """
    if not os.path.isdir(dir_path):
        try:
            os.makedirs(dir_path)
            module_logger.info('Folder created in path: {}'.format(dir_path))
        except Exception:
            module_logger.error('Impossible to create folder in path: {}'
                                .format(dir_path))
            raise


def hash_file(file_path, chunk_size=1024):
    """
    Accept an absolute file path and hash it.
    In case receive custom chunk_size use it.
    :return the md5 hash of received file
    """

    md5hash = hashlib.md5()
    try:
        f1 = open(file_path, 'rb')
        while True:
            # Read file in as little chunks
            buf = f1.read(chunk_size)
            if not buf:
                break
            md5hash.update(buf)
        f1.close()
        return md5hash.hexdigest()
    except Exception as e:
        module_logger.error(1, 'ERROR during hash of file: {}\n'
                               'Error happened: '.format(file_path, e))
        raise


def is_shared_file(path):
    """
    Check if the given path is a shared file.(Check if is located in 'shared' folder)
    :param path:
    :return: True, False
    """

    if path.split('/')[0] == 'shared':
        return True
    return False


class SkipObserver(Observer):
    def __init__(self, *args):
        Observer.__init__(self, *args)
        self._skip_list = []

    def skip(self, path):
        self._skip_list.append(path)
        module_logger.debug('Path "{}" added to skip list!!!'.format(path))

    def dispatch_events(self, event_queue, timeout):
        event, watch = event_queue.get(block=True, timeout=timeout)
        skip = False
        try:
            event.dest_path
        except AttributeError:
            pass
        else:
            if event.dest_path in self._skip_list:
                self._skip_list.remove(event.dest_path)
                skip = True
        try:
            event.src_path
        except AttributeError:
            pass
        else:
            if event.src_path in self._skip_list:
                self._skip_list.remove(event.src_path)
                skip = True

        if not skip:
            self._dispatch_event(event, watch)

        event_queue.task_done()


class DaemonUtils(object):

    CFG_FILENAME = 'daemon_config'
    LOCAL_DIR_STATE_FILENAME = 'local_dir_state'
    # Default configuration for Daemon
    DEF_CONF = OrderedDict()
    DEF_CONF['cmd_address'] = 'localhost'
    DEF_CONF['cmd_port'] = 50001
    DEF_CONF['api_suffix'] = '/API/V1/'
    DEF_CONF['server_address'] = 'http://localhost:5000'

    def __init__(self, cfg_dir, sharing_dir):
        # Init sharing directory
        self.sharing_dir = sharing_dir
        build_directory(sharing_dir)

        # Init cfg directory
        self.cfg_dir = cfg_dir
        self.cfg_filepath = os.path.join(self.cfg_dir, self.CFG_FILENAME)
        self.local_dir_state_filepath = os.path.join(self.cfg_dir, self.LOCAL_DIR_STATE_FILENAME)

        # Load config from cfg file
        self.cfg = self.load_configuration()
        self.local_dir_state = {}  # EXAMPLE {'last_timestamp': '<timestamp>', 'global_md5': '<md5>'}

    def load_configuration(self):
        """
        Loads configuration from "daemon_config" file contained in self.cfg_dir,
        if config file is corrupted restore it and load default configuration.

        :param cfg_path: Path of config
        :return: dictionary containing configuration
        """
        loaded_config = OrderedDict()
        try:
            with open(self.cfg_filepath, 'r') as fo:
                # To maintain the order i will extract one by one record
                for k, v in json.load(fo).iteritems():
                    loaded_config[k] = v
        except (ValueError, OSError, IOError):
            module_logger.warning('Impossible to load configuration from filepath: {0}\n'
                                  'Config file overwrited and loaded with default '
                                  'configuration!'.format(self.cfg_filepath))
            return self.create_cfg()

        # Check that all the key in DEF_CONF are in loaded_config
        for k in self.DEF_CONF:
            if k not in loaded_config:
                module_logger.warning('Warning "{0}" corrupted!\n'
                                      'Config file overwrited and loaded with '
                                      'default configuration!'.format(self.cfg_filepath))
                return self.create_cfg()

        return loaded_config

    def create_cfg(self):
        """
        Creates the cfg file for client_daemon with the given cfg filepath and sharing folder path
        and returns the result config.

        :param cfg_path: Path of config
        :param sharing_path: Indicate the path of observed directory
        """

        # Creates cfg directory
        build_directory(self.cfg_dir)
        with open(self.cfg_filepath, 'w') as cfg_filepath:
            json.dump(self.DEF_CONF, cfg_filepath, skipkeys=True, ensure_ascii=True, indent=4)
        return self.DEF_CONF

    def update_cfg(self):
        """
        Update cfg with new state in self.cfg
        """
        with open(self.cfg_filepath, 'w') as daemon_config:
            json.dump(self.cfg, daemon_config, skipkeys=True, ensure_ascii=True, indent=4)

    def _save_pass(self, password):
        """
        Save password in keyring
        """
        keyring.set_password('PyBox', self.cfg['user'], password)

    def _load_pass(self):
        """
        Load from keyring the password
        :return: the account password
        """
        return keyring.get_password('PyBox', self.cfg.get('user', ''))

    def relativize_path(self, abs_path):
        """
        This function relativize the path watched by daemon:
        for example: /home/user/watched/subfolder/ will be subfolder/
        """
        if abs_path.startswith(self.sharing_dir):
            relative_path = abs_path[len(self.sharing_dir) + 1:]
            return relative_path
        else:
            raise Exception

    def absolutize_path(self, rel_path):
        """
        This function absolutize a path that i have relativize before:
        for example: subfolder/ will be /home/user/watched/subfolder/
        """
        return os.path.join(self.sharing_dir, rel_path)

    def _is_directory_modified(self):
        """
        The function check if the shared folder has been modified.
        It recalculate the md5 from client_snapshot and compares it with the global md5 stored in local_dir_state
        :return: True or False
        """

        if self.md5_of_client_snapshot() != self.local_dir_state['global_md5']:
            return True
        else:
            return False

    def search_md5(self, searched_md5):
        """
        Receive as parameter the md5 of a file and return the first knowed path with the same md5
        """
        for path, tupla in self.client_snapshot.iteritems():
            if searched_md5 in tupla[1]:
                return path
        else:
            return None

    def build_client_snapshot(self):
        """
        Build a snapshot of the sharing folder with the following structure

        self.client_snapshot
        {
            "<file_path>":('<timestamp>', '<md5>')
        }
        """
        self.client_snapshot = {}
        for dirpath, dirs, files in os.walk(self.sharing_dir):
            for filename in files:
                filepath = os.path.join(dirpath, filename)
                rel_filepath = self.relativize_path(filepath)

                if not is_shared_file(rel_filepath):
                    self.client_snapshot[rel_filepath] = ['', hash_file(filepath)]

    def md5_of_client_snapshot(self):
        """
        Calculate the md5 of the entire directory snapshot,
        with the md5 in client_snapshot and the md5 of full filepath string.
        :return is the md5 hash of the directory
        """

        md5hash = hashlib.md5()

        for path, time_md5 in sorted(self.client_snapshot.iteritems()):
            # extract md5 from tuple. we don't need hexdigest it's already md5
            md5hash.update(time_md5[1])
            md5hash.update(path)

        return md5hash.hexdigest()

    def update_local_dir_state(self, last_timestamp):
        """
        Update the local_dir_state with last_timestamp operation and save it on disk
        """

        self.local_dir_state['last_timestamp'] = last_timestamp
        self.local_dir_state['global_md5'] = self.md5_of_client_snapshot()
        self.save_local_dir_state()

    def save_local_dir_state(self):
        """
        Save local_dir_state on disk
        """
        json.dump(self.local_dir_state, open(self.local_dir_state_filepath, 'w'), indent=4)

    def load_local_dir_state(self):
        """
        Load local dir state on self.local_dir_state variable
        if file doesn't exists it will be created without timestamp
        """

        def _rebuild_local_dir_state():
            self.local_dir_state = {'last_timestamp': 0, 'global_md5': self.md5_of_client_snapshot()}
            json.dump(self.local_dir_state, open(self.local_dir_state_filepath, 'w'), indent=4)

        if os.path.isfile(self.local_dir_state_filepath):
            self.local_dir_state = json.load(open(self.local_dir_state_filepath, 'r'))
            module_logger.debug('Loaded local_dir_state')
        else:
            module_logger.debug('local_dir_state not found. Initialize new local_dir_state')
            _rebuild_local_dir_state()
