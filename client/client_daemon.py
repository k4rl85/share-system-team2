#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import socket
import struct
import select
import os
import hashlib
import re
import time

from sys import exit as exit
from collections import OrderedDict
from shutil import copy2

# we import PollingObserver instead of Observer because the deleted event
# is not capturing https://github.com/gorakhargosh/watchdog/issues/46
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import RegexMatchingEventHandler
from connection_manager import ConnectionManager


class Daemon(RegexMatchingEventHandler):

    # The path for configuration directory and daemon configuration file
    CONFIG_DIR = os.path.join(os.environ['HOME'], '.PyBox')
    CONFIG_FILEPATH = os.path.join(CONFIG_DIR, 'daemon_config')
    LOCAL_DIR_STATE_PATH = os.path.join(CONFIG_DIR,'dir_state')

    # Default configuration for Daemon, loaded if fail to load the config file from CONFIG_DIR
    DEFAULT_CONFIG = OrderedDict()
    DEFAULT_CONFIG['sharing_path'] = os.path.join(os.environ['HOME'], 'sharing_folder')
    DEFAULT_CONFIG['cmd_address'] = 'localhost'
    DEFAULT_CONFIG['cmd_port'] = 50001
    DEFAULT_CONFIG['api_suffix'] = '/API/V1/'
    DEFAULT_CONFIG['server_address'] = 'http://localhost:5000'
    DEFAULT_CONFIG['user'] = 'pasquale'
    DEFAULT_CONFIG['pass'] = 'secretpass'
    DEFAULT_CONFIG['timeout_listener_sock'] = 0.5
    DEFAULT_CONFIG['backlog_listener_sock'] = 1

    IGNORED_REGEX = ['.*\.[a-zA-z]+?#',  # Libreoffice suite temporary file ignored
                     '.*\.[a-zA-Z]+?~',  # gedit issue solved ignoring this pattern:
                     # gedit first delete file, create, and move to dest_path *.txt~
                     '.*\/(\..*)',  # hidden files TODO: improve
                     ]

    # Calculate int size in the machine architecture
    INT_SIZE = struct.calcsize('!i')

    def __init__(self):
        RegexMatchingEventHandler.__init__(self, ignore_regexes=Daemon.IGNORED_REGEX, ignore_directories=True)

        # Just Initialize variable the Daemon.start() do the other things
        self.daemon_state = 'down'  # TODO implement the daemon state (disconnected, connected, syncronizing, ready...)
        self.running = 0
        self.client_snapshot = {} # EXAMPLE {'<filepath1>: ['<timestamp>', '<md5>', '<filepath2>: ...}
        self.local_dir_state = {} # EXAMPLE {'last_timestamp': '<timestamp>', 'global_md5': '<md5>'}
        self.listener_socket = None
        self.observer = None
        self.cfg = self.load_cfg(Daemon.CONFIG_FILEPATH)
        self.init_sharing_path()
        self.conn_mng = ConnectionManager(self.cfg)

    def load_cfg(self, config_path):
        """
        Load config, if impossible to find it or config file is corrupted restore it and load default configuration
        :param config_path: Path of config
        :return: dictionary containing configuration
        """
        def build_default_cfg():
            """
            Restore default config file by writing on file
            :return: default configuration contained in the dictionary DEFAULT_CONFIG
            """
            with open(Daemon.CONFIG_FILEPATH, 'wb') as fo:
                json.dump(Daemon.DEFAULT_CONFIG, fo, skipkeys=True, ensure_ascii=True, indent=4)
            return Daemon.DEFAULT_CONFIG

        # Search if config directory exists otherwise create it
        if not os.path.isdir(Daemon.CONFIG_DIR):
            try:
                os.makedirs(Daemon.CONFIG_DIR)
            except (OSError, IOError):
                self.stop(1, '\nImpossible to create "{}" directory! Permission denied!\n'.format(Daemon.CONFIG_DIR))

        if os.path.isfile(config_path):
            try:
                with open(config_path, 'r') as fo:
                    loaded_config = json.load(fo)
            except ValueError:
                print '\nImpossible to read "{0}"! Config file overwrited and loaded default config!\n'.format(config_path)
                return build_default_cfg()
            corrupted_config = False
            for k in Daemon.DEFAULT_CONFIG:
                if k not in loaded_config:
                    corrupted_config = True
            # In the case is all gone right run config in loaded_config
            if not corrupted_config:
                return loaded_config
            else:
                print '\nWarning "{0}" corrupted! Config file overwrited and loaded default config!\n'.format(config_path)
                return build_default_cfg()
        else:
            print '\nWarning "{0}" doesn\'t exist, Config file overwrited and loaded default config!\n'.format(config_path)
            return build_default_cfg()

    def init_sharing_path(self):
        """
        Check that the sharing folder exists otherwise create it.
        If is impossible to create exit with msg error.
        """
        if not os.path.isdir(self.cfg['sharing_path']):
            try:
                os.makedirs(self.cfg['sharing_path'])
            except OSError:
                self.stop(1, '\nImpossible to create "{0}" directory! Check sharing_path value contained in the following file:\n"{1}"\n'
                          .format(self.cfg['sharing_path'], Daemon.CONFIG_FILEPATH))

    def build_client_snapshot(self):
        """
        Build a snapshot of the sharing folder with the following structure

        self.client_snapshot
        {
            "<file_path>":('<timestamp>', '<md5>')
        }
        """
        self.client_snapshot = {}
        for dirpath, dirs, files in os.walk(self.cfg['sharing_path']):
                for filename in files:
                    filepath = os.path.join(dirpath, filename)
                    unwanted_file = False
                    for r in Daemon.IGNORED_REGEX:
                        if re.match(r, filepath) is not None:
                            unwanted_file = True
                            print 'Ignored Path:', filepath
                            break
                    if not unwanted_file:
                        relative_path = self.relativize_path(filepath)
                        with open(filepath, 'rb') as f:
                            self.client_snapshot[relative_path] = ['', hashlib.md5(f.read()).hexdigest()]

    def _is_directory_modified(self):
        # TODO process directory and get global md5. if the directory is modified return 'True', else return 'False'
        return True

    def get_server_files(self):
        # TODO makes request to server and return a tuple (timestamp, dir_tree)
        pass

    def search_md5(self, searched_md5):
        """
        Recive as parameter the md5 of a file and return the first knowed path with the same md5
        """
        for path in self.client_snapshot:
                if searched_md5 in self.client_snapshot[path][1]:
                    return path
        else:
            return None

    def sync_with_server_to_future(self):
        """
        Download from server the files state and find the difference from actual state.
        """
        def _filter_tree_difference(server_dir_tree):
            # process local dir_tree and server dir_tree
            # and make a diffs classification
            # return a dict representing that classification
            # { 'new'     : <[(<filepath>, <timestamp>, <md5>), ...]>,  # files in server, but not in client
            #   'modified': <[(<filepath>, <timestamp>, <md5>), ...]>,  # files in server and client, but different
            #   'deleted' : <[(<filepath>, <timestamp>, <md5>), ...]>,  # files not in server, but in client
            # }
            return {'created': [], 'modified': [], 'deleted': []}

        def _make_copy(src, dst):
            try:
                copy2(src, dst)
            except IOError:
                return False
            rel_src = self.relativize_path(src)
            rel_dst = self.relativize_path(dst)
            self.client_snapshot[rel_dst] = self.client_snapshot[rel_src]
            return True

        local_timestamp = self.dir_state['timestamp']
        server_timestamp, server_dir_tree = self._get_server_files()

        tree_diff = _filter_tree_difference(server_dir_tree)

        if self._is_directory_modified():
            if local_timestamp >= server_timestamp:
                pass
            else:  # local_timestamp < server_timestamp
                for filepath, timestamp, md5 in tree_diff['new']:
                    if timestamp > local_timestamp:
                        founded_path = self.search_md5(md5)
                        rel_filepath = self.relativize_path(filepath)
                        if founded_path:
                            _make_copy(src=self.absolutize_path(founded_path), dst=filepath)
                        else:
                            # TODO check if download succeed
                            self.conn_mng.dispatch_request('download', {'filepath': filepath})

                            with open(filepath, 'rb') as fo:
                                self.client_snapshot[rel_filepath][1] = hashlib.md5(fo.read()).hexdigest()
                    else:  # file older then local_timestamp, this mean is time to delete it!
                        # TODO check if delete succeed
                        self.conn_mng.dispatch_request('delete', {'filepath': filepath})
                        if rel_filepath in self.client_snapshot:
                            del self.client_snapshot[rel_filepath]

                for filepath, timestamp, md5 in tree_diff['modified']:
                    pass  # download all files

                for filepath, timestamp, md5 in tree_diff['deleted']:
                    pass  # deleted files

        else:
            if local_timestamp == server_timestamp:
                # send all diffs to server
                pass
            else:  # local_timestamp < server_timestamp
                for filepath, timestamp, md5 in tree_diff['new']:
                    retval = self.search_md5(md5)
                    if retval:
                        if retval[0] in self.client_snapshot:
                            pass  # copy file
                        else:
                            pass  # rename file
                    else:
                        if timestamp > local_timestamp:
                            pass  # dowload file
                        else:
                            pass  # delete file in server

                for filepath, timestamp, md5 in tree_diff['modified']:
                    if timestamp < local_timestamp:
                        pass  # upload file to server (update)
                    else:
                        pass  # duplicate file (.conflicted)
                        # upload .conflicted file to server

                for filepath, timestamp, md5 in tree_diff['deleted']:  # !!!! file in client and not in server ('deleted' isn't appropriate label, but now functionally)
                    pass  # upload file to server

    def _sync_with_server(self):
        """
        Download from server the files state and find the difference from actual state.
        """

        server_snapshot = self.conn_mng.dispatch_request('get_server_snapshot')
        if server_snapshot is None:
            self.stop(1, '\nReceived bad snapshot. Server down?\n')
        else:
            server_timestamp = server_snapshot['server_timestamp']
            server_snapshot = server_snapshot['files']

        total_md5 = self.calculate_md5_of_dir(self.cfg['sharing_path'])
        print "TOTAL MD5: ", total_md5

        for filepath in server_snapshot:

            if filepath not in self.client_snapshot:
                self.conn_mng.dispatch_request('download', {'filepath': filepath})
                self.client_snapshot[filepath] = server_snapshot[filepath]

            elif server_snapshot[filepath][1] != self.client_snapshot[filepath][1]:
                self.conn_mng.dispatch_request('modify', {'filepath': filepath})
                hashed_file = self.hash_file(self.absolutize_path(filepath))
                self.client_snapshot[filepath] = ['', hashed_file]

        for filepath in self.client_snapshot:
            if filepath not in server_snapshot:
                self.conn_mng.dispatch_request('upload', {'filepath': filepath})

    def relativize_path(self, abs_path):
        """
        This function relativize the path watched by daemon:
        for example: /home/user/watched/subfolder/ will be subfolder/
        """
        folder_watched_abs = self.cfg['sharing_path']
        tokens = abs_path.split(folder_watched_abs)
        # if len(tokens) is not 2 this mean folder_watched_abs is repeated in abs_path more then one time...
        # in this case is impossible to use relative path and have valid path!
        if len(tokens) is 2:
            relative_path = tokens[-1]
            return relative_path[1:]
        else:
            self.stop(1, 'Impossible to use "{}" path, please change dir path'.format(abs_path))

    def absolutize_path(self, rel_path):
        """
        This function absolutize a path that i have relativize before:
        for example: subfolder/ will be /home/user/watched/subfolder/
        """
        abs_path = os.path.join(self.cfg['sharing_path'], rel_path)
        if os.path.isfile(abs_path):
            return abs_path
        else:
            self.stop(1, 'Impossible to use "{}" path, please change dir path'.format(abs_path))

    def create_observer(self):
        """
        Create an instance of the watchdog Observer thread class.
        """
        self.observer = Observer()
        self.observer.schedule(self, path=self.cfg['sharing_path'], recursive=True)

    # TODO handly erorrs in dictionary if the client_dispatcher miss required data!!
    # TODO update struct with new more performance data structure
    # TODO verify what happen if the server return a error message
    ####################################

    def on_created(self, e):
        def build_data(cmd, rel_new_path, new_md5, founded_path=None):
            """
            Prepares the data from event handler to be delivered to connection_manager.
            """
            data = {'cmd': cmd}
            if cmd == 'copy':
                data['file'] = {'src': founded_path,
                                'dst': rel_new_path,
                                'md5': new_md5,
                                }
            else:
                data['file'] = {'filepath': rel_new_path,
                                'md5': new_md5,
                                }
            return data

        new_md5 = self.hash_file(e.src_path)
        rel_new_path = self.relativize_path(e.src_path)
        founded_path = self.search_md5(new_md5)

        # with this check i found the copy events
        if founded_path:
            print 'start copy'
            data = build_data('copy', rel_new_path, new_md5, founded_path)

        # this elif check that this created aren't modified event
        elif rel_new_path in self.client_snapshot:
            print 'start modified FROM CREATE!!!!!'
            data = build_data('modify', rel_new_path, new_md5)

        else: # Finally we find a real create event!
            print 'start create'
            data = build_data('upload', rel_new_path, new_md5)

        # Send data to connection manager dispatcher and check return value. If all go right update client_snapshot and local_dir_state
        event_timestamp = self.conn_mng.dispatch_request(data['cmd'], data['file'])
        if event_timestamp:
            self.client_snapshot[rel_new_path] = [event_timestamp, new_md5]
            self.update_local_dir_state(event_timestamp)
        else:
            self.stop(1, 'Impossible to connect with the server. Failed during "{0}" operation on "{1}" file'
                      .format(data['cmd'], e.src_path ))

    def on_moved(self, e):

        print 'start move'
        rel_src_path = self.relativize_path(e.dest_path)
        rel_dest_path = self.relativize_path(e.dest_path)
        # If i can't find rel_src_path inside client_snapshot there is inconsistent problem in client_snapshot!
        if self.client_snapshot.get(rel_src_path, 'ERROR') != 'ERROR':
            md5 = self.client_snapshot[rel_src_path][1]
        else:
            self.stop(1, 'Error during move event! Impossible to find "{}" inside client_snapshot'.format(rel_dest_path))

        data = {'src': rel_src_path,
                 'dst': rel_dest_path,
                 'md5': md5,
                 }
        # Send data to connection manager dispatcher and check return value. If all go right update client_snapshot and local_dir_state
        event_timestamp = self.conn_mng.dispatch_request('move', data)
        if event_timestamp:
            self.client_snapshot[rel_dest_path] = [event_timestamp, md5]
            # I'm sure that rel_src_path exists inside client_snapshot because i check above so i don't check pop result
            self.client_snapshot.pop(rel_src_path)
            self.update_local_dir_state(event_timestamp)
        else:
            self.stop(1, 'Impossible to connect with the server. Failed during "move" operation on "{}" file'.format(e.src_path ))

    def on_modified(self, e):

        print 'start modified'
        new_md5 = self.hash_file(e.src_path)
        rel_path = self.relativize_path(e.src_path)

        data = {'filepath': rel_path,
                'md5': new_md5
                }

        # Send data to connection manager dispatcher and check return value. If all go right update client_snapshot and local_dir_state
        event_timestamp = self.conn_mng.dispatch_request('modify', data)
        if event_timestamp:
            self.client_snapshot[rel_path] = [event_timestamp, new_md5]
            self.update_local_dir_state(event_timestamp)
        else:
            self.stop(1, 'Impossible to connect with the server. Failed during "delete" operation on "{}" file'.format(e.src_path))

    def on_deleted(self, e):

        print 'start delete'
        rel_deleted_path = self.relativize_path(e.src_path)

        # Send data to connection manager dispatcher and check return value. If all go right update client_snapshot and local_dir_state
        event_timestamp = self.conn_mng.dispatch_request('delete', {'filepath': rel_deleted_path})
        if event_timestamp:
            # If i can't find rel_deleted_path inside client_snapshot there is inconsistent problem in client_snapshot!
            if self.client_snapshot.pop(rel_deleted_path, 'ERROR') != 'ERROR':
                self.update_local_dir_state(event_timestamp)
            else:
                self.stop(1, 'Error during delete event! Impossible to find "{}" inside client_snapshot'.format(rel_deleted_path))
        else:
            self.stop(1, 'Impossible to connect with the server. Failed during "delete" operation on "{}" file'.format(e.src_path))

    def start(self):
        """
        Starts the communication with the command_manager.
        """
        self.build_client_snapshot()
        self.load_local_dir_state()

        # Operations necessary to start the daemon
        self._sync_with_server()
        self.create_observer()

        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind((self.cfg['cmd_address'], self.cfg['cmd_port']))
        self.listener_socket.listen(self.cfg['backlog_listener_sock'])
        r_list = [self.listener_socket]
        self.observer.start()
        self.daemon_state = 'started'
        self.running = 1
        try:
            while self.running:
                r_ready, w_ready, e_ready = select.select(r_list, [], [], self.cfg['timeout_listener_sock'])

                for s in r_ready:

                    if s == self.listener_socket:
                        # handle the server socket
                        client_socket, client_address = self.listener_socket.accept()
                        r_list.append(client_socket)
                    else:
                        # handle all other sockets
                        length = s.recv(Daemon.INT_SIZE)
                        if length:
                            # i need to do [0] and cast int because the struct.unpack return a tupla like (23234234,)
                            # with the length as a string
                            length = int(struct.unpack('!i', length)[0])
                            message = json.loads(s.recv(length))
                            for cmd, data in message.items():
                                if cmd == 'shutdown':
                                    raise KeyboardInterrupt
                                self.conn_mng.dispatch_request(cmd, data)
                        else:
                            s.close()
                            r_list.remove(s)
        except KeyboardInterrupt:
            self.stop(0)

    def stop(self, exit_status, exit_message=None):
        """
        Stop the Daemon components (observer and communication with command_manager).
        """
        if self.daemon_state == 'started':
            self.running = 0
            self.observer.stop()
            self.observer.join()
            self.listener_socket.close()
        print exit_message
        self.daemon_state == 'down'
        self.save_local_dir_state()
        print exit_message
        if exit_message:
            print exit_message
        exit(exit_status)

    def update_local_dir_state(self, last_timestamp):
        """
        Update the local_dir_state with last_timestamp operation and save it on disk
        """
        self.local_dir_state['last_timestamp'] = last_timestamp
        self.local_dir_state['global_md5'] = self.calculate_md5_of_dir()
        self.save_local_dir_state()

    def save_local_dir_state(self):
        """
        Save local_dir_state on disk
        """
        json.dump(self.local_dir_state, open(Daemon.LOCAL_DIR_STATE_PATH, "wb"), indent=4)
        print "local_dir_state saved"

    def load_local_dir_state(self):
        """
        Load local dir state on self.local_dir_state variable
        if file doesn't exists it will be created without timestamp
        """
        if os.path.isfile(Daemon.LOCAL_DIR_STATE_PATH):
            self.local_dir_state = json.load(open(Daemon.LOCAL_DIR_STATE_PATH, "rb"))
            print "Loaded dir_state"
        else:
            self.local_dir_state = {'last_timestamp': '', 'global_md5': self.calculate_md5_of_dir()}
            json.dump(self.local_dir_state, open(Daemon.LOCAL_DIR_STATE_PATH, "wb"), indent=4)
            print "dir_state not found, Initialize new dir_state"

    def calculate_md5_of_dir(self, verbose=0):
        """
        Calculate the md5 of the entire directory,
        with the md5 in client_snapshot and the md5 of full filepath string.
        When the filepath isn't in client_snapshot the md5 is calculated on fly
        :return is the md5 hash of the directory
        """
        directory = self.cfg['sharing_path']
        if verbose:
            start = time.time()
        md5Hash = hashlib.md5()
        if not os.path.exists(directory):
            return -1

        for root, dirs, files in os.walk(directory, followlinks=False):
            for names in files:
                filepath = os.path.join(root, names)
                rel_path = self.relativize_path(filepath)
                if rel_path in self.client_snapshot:
                    md5Hash.update(self.client_snapshot[rel_path][1])
                    md5Hash.update(hashlib.md5(filepath).hexdigest())
                else:
                    hashed_file = self.hash_file(filepath)
                    if hashed_file:
                        md5Hash.update(hashed_file)
                        md5Hash.update(hashlib.md5(filepath).hexdigest())
                    else:
                        print "can't hash file: ", filepath

        if verbose:
            stop = time.time()
            print stop - start
        return md5Hash.hexdigest()

    def hash_file(self, file_path, chunk_size=1024):
        """
        :accept an absolute file path
        :return the md5 hash of received file
        """
        md5Hash = hashlib.md5()
        try:
            f1 = open(file_path, 'rb')
            while 1:
                # Read file in as little chunks
                    buf = f1.read(chunk_size)
                    if not buf:
                        break
                    md5Hash.update(hashlib.md5(buf).hexdigest())
            f1.close()
            return md5Hash.hexdigest()
        except (OSError, IOError) as e:
            print e
            return None
            # You can't open the file for some reason

if __name__ == '__main__':
    daemon = Daemon()
    daemon.start()