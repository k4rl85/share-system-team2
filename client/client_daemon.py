#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import select
import os
import logging
import datetime
import argparse
from sys import exit as exit
from shutil import copy2, move

from watchdog.events import FileSystemEventHandler

from connection_manager import ConnectionManager
import utils



# Logging configuration
# =====================
LOG_FILENAME = 'log/client_daemon.log'

# create logger
logger = logging.getLogger('daemon')
logger.setLevel(logging.DEBUG)

# create console handler
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

# First log message
launched_or_imported = {True: 'launched', False: 'imported'}[__name__ == '__main__']
logger.info('-' * 60)
logger.info('Client {} at {}'.format(launched_or_imported, datetime.datetime.now().isoformat(' ')))
logger.info('-' * 60)

# create formatter and add it to the handler
console_formatter = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s', '%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(console_formatter)


def create_log_file_handler():
    """Creates file handler which logs even info messages"""
    if not os.path.isdir('log'):
        os.mkdir('log')
    file_handler = logging.FileHandler(LOG_FILENAME)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s %(name)-15s  %(levelname)-8s  %(message)s', '%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    return file_handler


class Daemon(FileSystemEventHandler, utils.DaemonUtils):

    # Allowed operation before user is activated
    ALLOWED_OPERATION = {'register', 'activate', 'login'}

    def __init__(self, cfg_path, sharing_path):
        FileSystemEventHandler.__init__(self)
        super(Daemon, self).__init__(cfg_path, sharing_path)
        # Initialize variable the Daemon.start() will use
        self.daemon_state = 'down'  # TODO implement the daemon state (disconnected, connected, syncronizing, ready...)
        self.running = 0
        self.client_snapshot = {}  # EXAMPLE {'<filepath1>: ['<timestamp>', '<md5>', '<filepath2>: ...}
        self.shared_snapshot = {}
        self.listener_socket = None
        self.observer = None

        self.password = self._load_pass()

        self.conn_mng = ConnectionManager(self.cfg, self.sharing_dir)

        self.INTERNAL_COMMANDS = {
            'addshare': self._add_share,
            'removeshare': self._remove_share,
            'removeshareduser': self._remove_shared_user,
        }

    def build_shared_snapshot(self):
        """
        Build the snapshot of the shared files according this structure:
        {
            "shared/<user>/<file_path>":('<timestamp>', '<md5>')
        }
        """
        response = self.conn_mng.dispatch_request('get_server_snapshot', '')
        if response['successful']:
            try:
                self.shared_snapshot = response['content']['shared_files']
            except KeyError:
                self.shared_snapshot = {}
        else:
            self.stop(1, 'Received None snapshot. Server down?')

        # check the consistency of client snapshot retrieved by the server with the real files on clients
        file_md5 = None
        for filepath in self.shared_snapshot.keys():
            file_md5 = utils.hash_file(self.absolutize_path(filepath))
            if not file_md5 or file_md5 != self.shared_snapshot[filepath][1]:
                # force the re-download at next synchronization
                self.shared_snapshot.pop(filepath)

        # NOTE: for future implementation:
        #
        # At startup it can't know if the owner of shared file has removed that share, so the files will remain into
        # shared folder as a zombie files, that means they will not update their status
        #
        # P.S. It can't delete them because some files could be not shared files, so it's safe don't force
        # the deletion of them

    def _make_copy_on_client(self, src, dst):
        """
        Copy the file from src to dst if the dst already exists will be overwritten
        :param src: the relative path of source file to copy
        :param dst: the relative path of destination file to copy
        :return: True or False
        """

        abs_src = self.absolutize_path(src)
        if not os.path.isfile(abs_src):
            return False
        abs_dst = self.absolutize_path(dst)
        dst_dir = os.path.dirname(abs_dst)

        if not os.path.isdir(dst_dir):
            os.makedirs(dst_dir)
        # Skip next operation to prevent watchdog to see this copy on client
        self.observer.skip(abs_dst)
        try:
            copy2(abs_src, abs_dst)
        except IOError:
            return False
        self.client_snapshot[dst] = self.client_snapshot[src]
        logger.info('Copied file on client during SYNC.\n'
                     'Source filepath: {}\nDestination filepath: {}'.format(abs_src, abs_dst))
        return True

    def _make_move_on_client(self, src, dst):
        """
        Move the file from src to dst. if the dst already exists will be overwritten
        :param src: the relative path of source file to move
        :param dst: the relative path of destination file to move
        :return: True or False
        """

        abs_src = self.absolutize_path(src)
        if not os.path.isfile(abs_src):
            return False
        abs_dst = self.absolutize_path(dst)
        dst_dir = os.path.dirname(abs_dst)

        if not os.path.isdir(dst_dir):
            os.makedirs(dst_dir)
        # Skip next operation to prevent watchdog to see this move on client
        self.observer.skip(abs_dst)
        try:
            move(abs_src, abs_dst)
        except IOError:
            return False

        self.client_snapshot[dst] = self.client_snapshot[src]
        self.client_snapshot.pop(src)
        logger.info('Moved file on client during SYNC.\n'
                    'Source filepath: {}\nDestination filepath: {}'.format(abs_src, abs_dst))
        return True

    def _make_delete_on_client(self, filepath):
        """
        Delete the file in filepath. In case of error print information about it.
        :param filepath: the path of file i will delete
        """
        abs_path = self.absolutize_path(filepath)
        # Skip next operation to prevent watchdog to see this delete
        self.observer.skip(abs_path)
        try:
            os.remove(abs_path)
        except OSError as e:
            logger.warning('WARNING impossible delete file during SYNC on path: {}\n'
                           'Error occurred: {}'.format(abs_path, e))
        if self.client_snapshot.pop(filepath, 'ERROR') != 'ERROR':
            logger.info('Deleted file on client during SYNC.\nDeleted filepath: {}'.format(abs_path))
        else:
            logger.warning('WARNING inconsistency error during delete operation!\n'
                           'Impossible to find the following file in stored data (client_snapshot):\n'
                           '{}'.format(abs_path))

    def _sync_process(self, server_timestamp, server_dir_tree, shared_dir_tree={}):
        # Makes the synchronization logic and return a list of commands to launch
        # for server synchronization

        def _filter_tree_difference(client_dir_tree, server_dir_tree):
            # process local dir_tree and server dir_tree
            # and makes a diffs classification
            # return a dict representing that classification
            # E.g. { 'new_on_server'     : <[<filepath>, ...]>,  # files in server, but not in client
            # 'modified'          : <[<filepath>, ...]>,  # files in server and client, but different
            # 'new_on_client'     : <[<filepath>, ...]>,  # files not in server, but in client
            # }
            client_files = set(client_dir_tree.keys())
            server_files = set(server_dir_tree.keys())

            new_on_server = list(server_files.difference(client_files))
            new_on_client = list(client_files.difference(server_files))
            modified = []

            for filepath in server_files.intersection(client_files):
                # check files md5

                if server_dir_tree[filepath][1] != client_dir_tree[filepath][1]:
                    modified.append(filepath)

            return {'new_on_server': new_on_server, 'modified': modified, 'new_on_client': new_on_client}

        def _check_md5(dir_tree, md5):
            result = []
            for k, v in dir_tree.iteritems():
                if md5 == v[1]:
                    result.append(k)
            return result

        local_timestamp = self.local_dir_state['last_timestamp']
        tree_diff = _filter_tree_difference(self.client_snapshot, server_dir_tree)
        shared_tree_diff = _filter_tree_difference(self.shared_snapshot, shared_dir_tree)
        sync_commands = []

        if self._is_directory_modified():
            if local_timestamp == server_timestamp:
                logger.debug('local_timestamp == server_timestamp and directory IS modified')
                logger.debug(tree_diff)
                # simple case: the client has the command
                # it sends all folder modifications to server

                # files in server but not in client: remove them from server
                for filepath in tree_diff['new_on_server']:
                    sync_commands.append(('delete', filepath))
                    # self.conn_mng.dispatch_request('delete', {'filepath': filepath})

                # files modified in client: send modified files to server
                for filepath in tree_diff['modified']:
                    sync_commands.append(('modify', filepath))

                # files in client but not in server: upload them to server
                for filepath in tree_diff['new_on_client']:
                    sync_commands.append(('upload', filepath))
                    # self.conn_mng.dispatch_request('upload', {'filepath': filepath})

            else:  # local_timestamp < server_timestamp
                logger.debug('local_timestamp < server_timestamp and directory IS modified')
                logger.debug(tree_diff)
                assert local_timestamp <= server_timestamp, 'ERROR something bad happen during SYNC process, ' \
                                                            'local_timestamp > di server_timestamp '
                # the server has the command
                for filepath in tree_diff['new_on_server']:
                    file_timestamp, md5 = server_dir_tree[filepath]
                    existed_filepaths_on_client = _check_md5(self.client_snapshot, md5)
                    # If i found at least one path in client_snapshot with the same md5 of filepath this mean in the
                    # past client_snapshot have stored one or more files with the same md5 but different paths.

                    if existed_filepaths_on_client:
                        # it's a copy or a move
                        for path in existed_filepaths_on_client:
                            if path in tree_diff['new_on_client']:
                                if self._make_move_on_client(path, filepath):
                                    tree_diff['new_on_client'].remove(path)
                                    break
                                else:
                                    self.stop(0, 'move failed on in SYNC: src_path: {}, dest_path: {}'.format(path,
                                                                                                              filepath))
                        # we haven't found files deleted on server so it's a copy
                        else:
                            if not self._make_copy_on_client(path, filepath):
                                self.stop(0,
                                          'copy failed on in SYNC: src_path: {}, dest_path: {}'.format(path, filepath))

                    # the daemon don't know filepath, i will search more updated timestamp
                    else:
                        if file_timestamp > local_timestamp:
                            # the files in server is more updated
                            sync_commands.append(('download', filepath))
                            # self.conn_mng.dispatch_request('download', {'filepath': filepath})
                        else:
                            # the client has deleted the file, so delete it on server
                            sync_commands.append(('delete', filepath))
                            # self.conn_mng.dispatch_request('delete', {'filepath': filepath})

                for filepath in tree_diff['modified']:
                    file_timestamp, md5 = server_dir_tree[filepath]

                    if file_timestamp < local_timestamp:
                        # the client has modified the file, so update it on server
                        sync_commands.append(('modify', filepath))
                        # self.conn_mng.dispatch_request('modify', {'filepath': filepath})
                    else:
                        # it's the worst case:
                        # we have a conflict with server,
                        # someone has modified files while daemon was down and someone else has modified
                        # the same file on server
                        conflicted_path = ''.join([filepath, '.conflicted'])
                        self._make_copy_on_client(filepath, conflicted_path)
                        sync_commands.append(('upload', conflicted_path))
                        # self.conn_mng.dispatch_request('upload', {'filepath': conflicted_path})

                for filepath in tree_diff['new_on_client']:
                    sync_commands.append(('upload', filepath))
                    # self.conn_mng.dispatch_request('upload', {'filepath': filepath})

        else:  # directory not modified
            if local_timestamp == server_timestamp:
                logger.debug('local_timestamp == server_timestamp and directory IS NOT modified')
                logger.debug(tree_diff)
                # it's the best case. Client and server are already synchronized
                for key in tree_diff:
                    assert not tree_diff[key], 'local_timestamp == server_timestamp but tree_diff is not empty!\n' \
                                               'tree_diff:\n{}'.format(tree_diff)
                sync_commands = []
            else:  # local_timestamp < server_timestamp
                logger.debug('local_timestamp < server_timestamp and directory IS NOT modified')
                logger.debug(tree_diff)
                assert local_timestamp <= server_timestamp, 'ERROR something bad happen during SYNC process, ' \
                                                            'local_timestamp > di server_timestamp'
                # the server has the command
                for filepath in tree_diff['new_on_server']:
                    timestamp, md5 = server_dir_tree[filepath]
                    existed_filepaths_on_client = _check_md5(self.client_snapshot, md5)
                    # If i found at least one path in client_snapshot with the same md5 of filepath this mean that
                    # in the past client_snapshot have stored one or more files with the same md5 but different paths.

                    if existed_filepaths_on_client:
                        # it's a copy or a move
                        for path in existed_filepaths_on_client:
                            if path in tree_diff['new_on_client']:
                                if self._make_move_on_client(path, filepath):
                                    tree_diff['new_on_client'].remove(path)
                                    break
                                else:
                                    self.stop(0, 'move failed on in SYNC: src_path: {}, dest_path: {}'.format(path,
                                                                                                              filepath))
                        # we haven't found files deleted on server so it's a copy
                        else:
                            if not self._make_copy_on_client(path, filepath):
                                self.stop(0,
                                          'copy failed on in SYNC: src_path: {}, dest_path: {}'.format(path, filepath))
                    else:
                        # it's a new file
                        sync_commands.append(('download', filepath))
                        # self.conn_mng.dispatch_request('download', {'filepath': filepath})

                for filepath in tree_diff['modified']:
                    self._make_delete_on_client(filepath)
                    sync_commands.append(('download', filepath))
                    # self.conn_mng.dispatch_request('download', {'filepath': filepath})

                for filepath in tree_diff['new_on_client']:
                    # files that have been deleted on server, so we have to delete them
                    self._make_delete_on_client(filepath)

        for filepath in shared_tree_diff['new_on_client']:
            # files deleted on server
            abs_filepath = self.absolutize_path(filepath)
            self.observer.skip(abs_filepath)
            try:
                os.remove(abs_filepath)
            except OSError as e:
                logger.warning('WARNING impossible delete file during SYNC on path: {}\n'
                               'Error occurred: {}'.format(abs_filepath, e))
            if self.shared_snapshot.pop(filepath, None):
                logger.info('Deleted file on client during SYNC.\nDeleted filepath: {}'.format(abs_filepath))
            else:
                logger.warning('WARNING inconsistency error during delete operation!\n'
                               'Impossible to find the following file in stored data (shared_snapshot):\n'
                               '{}'.format(abs_filepath))

        for filepath in shared_tree_diff['modified']:
            sync_commands.append(('download', filepath))

        for filepath in shared_tree_diff['new_on_server']:
            sync_commands.append(('download', filepath))

        return sync_commands

    def sync_with_server(self):
        """
        Makes the synchronization with server
        """
        response = self.conn_mng.dispatch_request('get_server_snapshot', '')
        if not response['successful']:
            self.stop(1, response['content'])

        server_timestamp = response['content']['server_timestamp']
        server_snapshot = response['content']['files']

        try:
            shared_files = response['content']['shared_files']
        except KeyError:
            shared_files = {}

        sync_commands = self._sync_process(server_timestamp, server_snapshot, shared_files)

        # Initialize the variable where we put the timestamp of the last operation we did
        last_operation_timestamp = server_timestamp

        # makes all synchronization commands
        for command, path in sync_commands:
            if command == 'delete':
                abs_path = self.absolutize_path(path)
                response = self.conn_mng.dispatch_request(command, {'filepath': path})
                if response['successful']:
                    last_operation_timestamp = response['content']['server_timestamp']
                    if self.client_snapshot.pop(path, 'ERROR') != 'ERROR':
                        logger.info('Deleted file on server during SYNC.\nDeleted filepath: {}'.format(abs_path))
                    else:
                        logger.warning('WARNING inconsistency error during delete operation!\n'
                                       'Impossible to find the following file in stored data (client_snapshot):\n'
                                       '{}'.format(abs_path))
                else:
                    self.stop(1, response['content'])

            elif command == 'modify' or command == 'upload':
                abs_path = self.absolutize_path(path)
                new_md5 = utils.hash_file(abs_path)
                response = self.conn_mng.dispatch_request(command, {'filepath': path, 'md5': new_md5})
                if response['successful']:
                    last_operation_timestamp = response['content']['server_timestamp']
                    cmd_type = ('Modified', 'Updated')[command == 'modify']
                    logger.info('{0} file on server during SYNC.\n{0} filepath: {1}'.format(cmd_type, abs_path))
                else:
                    self.stop(1, response['content'])

            else:  # command == 'download'
                abs_path = self.absolutize_path(path)
                # Skip next operation to prevent watchdog to see this download
                self.observer.skip(abs_path)
                response = self.conn_mng.dispatch_request(command, {'filepath': path})
                if response['successful']:
                    logger.info('Downloaded file from server during SYNC.\nDownloaded filepath: {}'.format(abs_path))
                    if utils.is_shared_file(path):
                        self.shared_snapshot[path] = shared_files[path]
                    else:
                        self.client_snapshot[path] = server_snapshot[path]
                else:
                    self.stop(1, response['content'])

        self.update_local_dir_state(last_operation_timestamp)

    @utils.is_directory
    def on_created(self, e):
        """
        Manage the create event observed from watchdog.
        The data about the event is collected and sent to the server with connection_manager module.
        After the result of connection is received with successful response the client_snapshot and local_dir_state
        will be updated.

        N.B: Sometime on_created event is a copy or modify event for erroneous survey,
        so the method check if this error has happened.
        :param e: event object with information about what has happened
        """
        def build_data(cmd, rel_new_path, new_md5, founded_path=None):
            """
            Prepares the data from event handler to be delivered to connection_manager.
            """
            data = {'cmd': cmd}
            if cmd == 'copy':
                data['file'] = {'src': founded_path,
                                'dst': rel_new_path,
                                'md5': new_md5}
            else:
                data['file'] = {'filepath': rel_new_path,
                                'md5': new_md5}
            return data

        new_md5 = utils.hash_file(e.src_path)
        rel_new_path = self.relativize_path(e.src_path)
        founded_path = self.search_md5(new_md5)
        # with this check i found the copy events
        if founded_path:
            abs_founded_path = self.absolutize_path(founded_path)
            logger.info('Copy event from path : {}\n to path: {}'.format(abs_founded_path, e.src_path))
            data = build_data('copy', rel_new_path, new_md5, founded_path)
        # this elif check that this create event aren't modify event.
        # Normally this never happen but sometimes watchdog fail to understand what has happened on file.
        # For example Gedit generate a create event instead modify event when a file is saved.
        elif rel_new_path in self.client_snapshot:
            logger.warning('WARNING this is modify event FROM CREATE EVENT!'
                           'Path of file already existent: {}'.format(e.src_path))
            data = build_data('modify', rel_new_path, new_md5)

        else:  # Finally we find a real create event!
            logger.info('Create event on path: {}'.format(e.src_path))
            data = build_data('upload', rel_new_path, new_md5)

        # Send data to connection manager dispatcher and check return value.
        # If all go right update client_snapshot and local_dir_state
        if utils.is_shared_file(rel_new_path):
            logger.warning('You are writing file in path: {}\n'
                           'This is a read-only folder, so it will not be synchronized with server'
                           .format(rel_new_path))
        else:
            response = self.conn_mng.dispatch_request(data['cmd'], data['file'])
            if response['successful']:
                event_timestamp = response['content']['server_timestamp']
                self.client_snapshot[rel_new_path] = [event_timestamp, new_md5]
                self.update_local_dir_state(event_timestamp)
                logger.debug('{} event completed.'.format(data['cmd']))
            else:
                self.stop(1, response['content'])

    @utils.is_directory
    def on_moved(self, e):
        """
        Manage the move event observed from watchdog.
        The data about the event is collected and sent to the server with connection_manager module.
        After the result of connection is received with successful response the client_snapshot and local_dir_state
        will be updated.

        N.B: Sometime on_move event is a copy event for erroneous survey, so the method check if this error has happened.
        :param e: event object with information about what has happened
        """
        logger.info('Move event from path : {}\n to path: {}'.format(e.src_path, e.dest_path))
        rel_src_path = self.relativize_path(e.src_path)
        rel_dest_path = self.relativize_path(e.dest_path)

        source_shared = utils.is_shared_file(rel_src_path)
        dest_shared = utils.is_shared_file(rel_dest_path)

        # this check that move event isn't modify event.
        # Normally this never happen but sometimes watchdog fail to understand what has happened on file.
        # For example Gedit generate a move event instead copy event when a file is saved.
        if not os.path.exists(e.src_path):
            cmd = 'move'
        else:
            logger.warning('WARNING this is COPY event from MOVE EVENT!')
            cmd = 'copy'

        if source_shared and not dest_shared:  # file moved from shared path to not shared path
            # upload the file
            new_md5 = utils.hash_file(e.dest_path)
            data = {
                'filepath': rel_dest_path,
                'md5': new_md5
            }

            response = self.conn_mng.dispatch_request('upload', data)
            if response['successful']:
                event_timestamp = response['content']['server_timestamp']
                self.client_snapshot[rel_dest_path] = [event_timestamp, new_md5]
                self.update_local_dir_state(event_timestamp)

                if cmd == 'move':
                    # force the re-download of the file at next synchronization
                    try:
                        self.shared_snapshot.pop(rel_src_path)
                    except KeyError:
                        pass
            else:
                self.stop(1, response['content'])

        elif source_shared and dest_shared:  # file moved from shared path to shared path
            if cmd == 'move':
                # force the re-download of the file moved at the next synchronization
                try:
                    self.shared_snapshot.pop(rel_src_path)
                except KeyError:
                    pass

            # if it has modified a file tracked by shared snapshot, then force the re-download of it
            try:
                self.shared_snapshot.pop(rel_dest_path)
            except KeyError:
                pass

        elif not source_shared and dest_shared:  # file moved from not shared path to shared path
            if cmd == 'move':
                # delete file on server
                response = self.conn_mng.dispatch_request('delete', {'filepath': rel_src_path})
                if response['successful']:
                    event_timestamp = response['content']['server_timestamp']
                    if self.client_snapshot.pop(rel_src_path, 'ERROR') == 'ERROR':
                        logger.warning('WARNING inconsistency error during delete operation!\n'
                                       'Impossible to find the following file in stored data (client_snapshot):\n'
                                       '{}'.format(rel_src_path))
                    self.update_local_dir_state(event_timestamp)
                else:
                    self.stop(1, response['content'])

            # if it has modified a file tracked by shared snapshot, then force the re-download of it
            try:
                self.shared_snapshot.pop(rel_dest_path)
            except KeyError:
                pass

        else:  # file moved from not shared path to not shared path (standard case)
            if not self.client_snapshot.get(rel_src_path)[1]:
                self.stop(1, 'WARNING inconsistency error during {} operation!\n'
                             'Impossible to find the following file in stored data (client_snapshot):\n'
                             '{}'.format(cmd, rel_src_path))
            md5 = self.client_snapshot[rel_src_path][1]
            data = {'src': rel_src_path,
                    'dst': rel_dest_path,
                    'md5': md5}
            # Send data to connection manager dispatcher and check return value.
            # If all go right update client_snapshot and local_dir_state
            response = self.conn_mng.dispatch_request(cmd, data)
            if response['successful']:
                event_timestamp = response['content']['server_timestamp']
                self.client_snapshot[rel_dest_path] = [event_timestamp, md5]
                if cmd == 'move':
                    # rel_src_path already checked
                    self.client_snapshot.pop(rel_src_path)
                self.update_local_dir_state(event_timestamp)
                logger.debug('{} event completed.'.format(cmd))
            else:
                self.stop(1, response['content'])

    @utils.is_directory
    def on_modified(self, e):
        """
        Manage the modify event observed from watchdog.
        The data about the event is collected and sent to the server with connection_manager module.
        After the result of connection is received with successful response the client_snapshot and local_dir_state
        will be updated.
        :param e: event object with information about what has happened
        """
        logger.info('Modify event on file: {}'.format(e.src_path))
        new_md5 = utils.hash_file(e.src_path)
        rel_path = self.relativize_path(e.src_path)
        data = {
            'filepath': rel_path,
            'md5': new_md5
        }
        if utils.is_shared_file(rel_path):
            # if it has modified a file tracked by shared snapshot, then force the re-download of it
            try:
                self.shared_snapshot.pop(rel_path)
            except KeyError:
                pass
        else:
            # Send data to connection manager dispatcher and check return value.
            # If all go right update client_snapshot and local_dir_state
            response = self.conn_mng.dispatch_request('modify', data)
            if response['successful']:
                event_timestamp = response['content']['server_timestamp']
                self.client_snapshot[rel_path] = [event_timestamp, new_md5]
                self.update_local_dir_state(event_timestamp)
                logger.debug('Modify event completed.')
            else:
                self.stop(1, response['content'])

    @utils.is_directory
    def on_deleted(self, e):
        """
        Manage the delete event observed from watchdog.
        The data about the event is collected and sent to the server with connection_manager module.
        After the result of connection is received with successful response the client_snapshot and local_dir_state
        will be updated.
        :param e: event object with information about what has happened
        """
        logger.info('Delete event on file: {}'.format(e.src_path))
        rel_path = self.relativize_path(e.src_path)
        if utils.is_shared_file(rel_path):
            # if it has modified a file tracked by shared snapshot, then force the re-download of it
            try:
                self.shared_snapshot.pop(rel_path)
            except KeyError:
                pass
        else:
            # Send data to connection manager dispatcher and check return value.
            # If all go right update client_snapshot and local_dir_state
            response = self.conn_mng.dispatch_request('delete', {'filepath': rel_path})
            if response['successful']:
                event_timestamp = response['content']['server_timestamp']
                if self.client_snapshot.pop(rel_path, 'ERROR') == 'ERROR':
                    logger.warning('WARNING inconsistency error during delete operation!\n'
                                    'Impossible to find the following file in stored data (client_snapshot):\n'
                                    '{}'.format(e.src_path))
                self.update_local_dir_state(event_timestamp)
                logger.debug('Delete event completed.')
            else:
                self.stop(1, response['content'])

    def _initialize_observing(self):
        """
        Intial operation for observing.
        We create the client_snapshot, load the information stored inside local_dir_state and create observer.
        """
        self.build_client_snapshot()
        self.build_shared_snapshot()
        self.load_local_dir_state()
        self.create_observer()
        self.observer.start()
        self.sync_with_server()

    def create_observer(self):
        """
        Create an instance of the watchdog Observer thread class.
        """
        self.observer = utils.SkipObserver()
        self.observer.schedule(self, path=self.sharing_dir, recursive=True)

    def _activation_check(self, cmd, data):
        """
        This method allow only registration and activation of user until this will be accomplished.
        In case of bad cmd this will be refused otherwise if the server response are successful
        we update the daemon_config and after activation of user start the observing.
        In case of login we do registration and activation together
        :param s: connection socket with client_cmdmanager
        :param cmd: received cmd from client_cmdmanager
        :param data: received data from client_cmdmanager
        """
        def store_registration_data():
            """
            update cfg with userdata
            """
            self.cfg['user'] = data[0]
            self.update_cfg()
            self.password = data[1]
            self._save_pass(data[1])

        def activate_daemon():
            """
            activate observing and update cfg['activate'] state at True in all loaded cfg
            """
            self.cfg['activate'] = True
            # Update the information about cfg into connection manager and cfg file
            self.conn_mng.load_cfg(self.cfg)
            self.update_cfg()
            # Now the client_daemon is ready to operate, we do the start activity
            self._initialize_observing()

        if cmd not in Daemon.ALLOWED_OPERATION:
            response = {'content': 'Operation not allowed! Authorization required.',
                        'successful': False}
        else:
            response = self.conn_mng.dispatch_request(cmd, data)
            if response['successful']:
                if cmd == 'register':
                    store_registration_data()
                elif cmd == 'activate':
                    activate_daemon()
                elif cmd == 'login':
                    store_registration_data()
                    activate_daemon()
        return response

    def start(self):
        """
        Starts the communication with the command_manager.
        """
        # If user is activated we can start observing.
        if self.cfg.get('activate'):
            self._initialize_observing()

        TIMEOUT_LISTENER_SOCK = 0.5
        BACKLOG_LISTENER_SOCK = 1
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind((self.cfg['cmd_address'], self.cfg['cmd_port']))
        self.listener_socket.listen(BACKLOG_LISTENER_SOCK)
        r_list = [self.listener_socket]
        self.daemon_state = 'started'
        self.running = 1
        polling_counter = 0
        try:
            while self.running:
                r_ready, w_ready, e_ready = select.select(r_list, [], [], TIMEOUT_LISTENER_SOCK)
                for s in r_ready:

                    if s == self.listener_socket:
                        # handle the server socket
                        client_socket, client_address = self.listener_socket.accept()
                        r_list.append(client_socket)
                    else:
                        # handle all other sockets
                        req = utils.get_cmdmanager_request(s)

                        if req:
                            for cmd, data in req.items():
                                # TODO is required to refact/reingeneering stop/shutdown for a clean closure
                                # now used an expedient (raise KeyboardInterrupt) to make it runnable
                                if cmd == 'shutdown':
                                    response = {'content': 'Daemon is shutting down', 'successful': True}
                                    utils.set_cmdmanager_response(s, response)
                                    raise KeyboardInterrupt

                                if not self.cfg.get('activate'):
                                    response = self._activation_check(cmd, data)
                                # this elif avoid login of user with already logged daemon
                                elif cmd == 'login':
                                    response = {'content': 'Warning! There is a user already authenticated on this '
                                                           'computer. Impossible to login account',
                                                'successful': False}
                                # client is already activated
                                else:
                                    try:
                                        response = self.INTERNAL_COMMANDS[cmd](data)
                                    except KeyError:
                                        response = self.conn_mng.dispatch_request(cmd, data)
                                        # for now the protocol is that for request sent by
                                        # command manager, the server reply with a string
                                        # so, to maintain the same data structure during
                                        # daemon and cmdmanager comunications, it rebuild a json
                                        # to send like response
                                        # TODO it's advisable to make attention to this assertion or refact the architecture

                                utils.set_cmdmanager_response(s, response)
                        else:  # it receives the FIN packet that close the connection
                            s.close()
                            r_list.remove(s)

                if self.cfg.get('activate'):
                    # synchronization polling
                    # makes the polling every 3 seconds, so it waits six cycle (0.5 * 6 = 3 seconds)
                    # maybe optimizable but now functional
                    polling_counter += 1
                    if polling_counter == 6:
                        polling_counter = 0
                        self.sync_with_server()

        except KeyboardInterrupt:
            self.stop(0)
        if self.cfg.get('activate'):
            self.observer.stop()
            self.observer.join()
        self.listener_socket.close()

    def _validate_path(self, path):

        if os.path.exists(''.join([self.sharing_dir, os.sep, path])):
            return True
        return False

    def _add_share(self, data):
        """
        handle the adding of shared folder with a user
        """
        shared_folder = data[0]
        if not self._validate_path(shared_folder):
            return '\'%s\' not exists' % shared_folder

        return self.conn_mng.dispatch_request('addshare', data)

    def _remove_share(self, data):
        """
        handle the removing of a shared folder
        """
        shared_folder = data[0]
        if not self._validate_path(shared_folder):
            return '\'%s\' not exists' % shared_folder

        return self.conn_mng.dispatch_request('removeshare', data)

    def _remove_shared_user(self, data):
        """
        handle the removing of an user from a shared folder
        """
        shared_folder = data[0]
        if not self._validate_path(shared_folder):
            return '\'%s\' not exists' % shared_folder

        return self.conn_mng.dispatch_request('removeshareduser', data)

    def stop(self, exit_status, exit_message=None):
        """
        Stop the Daemon components (observer and communication with command_manager).
        """
        if self.daemon_state == 'started':
            self.running = 0
            self.daemon_state = 'down'
            if self.local_dir_state:
                self.save_local_dir_state()
        if exit_message:
            logger.error(exit_message)
        exit(exit_status)


def is_valid_dir(parser, string):
    if os.path.isdir(string) or string in (utils.DEFAULT_CONFIG_FOLDER,
                                           utils.DEFAULT_SHARING_FOLDER):
        return string
    else:
        parser.error('The path "%s" does not be a valid directory!' % string)


def main():
    file_handler = create_log_file_handler()

    parser = argparse.ArgumentParser()
    parser.add_argument('-cfg', help='the configuration folder path.\n'
                                     'NOTE: You have to create it before use!',
                        type=lambda string: is_valid_dir(parser, string),
                        default=utils.DEFAULT_CONFIG_FOLDER,
                        dest='config_folder_path')
    parser.add_argument('-sh', help='the sharing folder path that we will observed.\n'
                                    'NOTE: You have to create it before use!',
                        type=lambda string: is_valid_dir(parser, string),
                        default=utils.DEFAULT_SHARING_FOLDER,
                        dest='sharing_folder_path')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='set console verbosity level to DEBUG (4) '
                             '[default: %(default)s]')
    parser.add_argument('--verbose', default=False, action='store_true',
                        help='set console verbosity level to INFO (3) [default: %(default)s].'
                             'Ignored if --debug option is set.')
    parser.add_argument('-v', '--verbosity', type=int, choices=range(5), nargs='?',
                        help='set console verbosity: 0=CRITICAL, 1=ERROR, 2=WARN, 3=INFO, 4=DEBUG. '
                             '[default: %(default)s]. Ignored if --verbose or --debug option is set.')
    parser.add_argument('-fv', '--file_verbosity', type=int, choices=range(5), nargs='?',
                        help='set file verbosity: 0=CRITICAL, 1=ERROR, 2=WARN, 3=INFO, 4=DEBUG. '
                             '[default: %(default)s].')
    parser.add_argument('-H', '--host', default='0.0.0.0',
                        help='set host address to run the server. [default: %(default)s].')

    args = parser.parse_args()

    if args.config_folder_path == args.sharing_folder_path:
        parser.error('Sharing folder and config folder must be different!')
    elif args.sharing_folder_path in args.config_folder_path:
        parser.error('Config folder must be outside Sharing folder!')
    levels = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
    if args.debug:
        # If set to True, win against verbosity and verbose parameter
        console_handler.setLevel(logging.DEBUG)
    elif args.verbose:
        # If set to True, win against verbosity parameter
        console_handler.setLevel(logging.INFO)
    elif args.verbosity:
        console_handler.setLevel(levels[args.verbosity])
    else:
        # Set default console lvl
        console_handler.setLevel(logging.WARNING)

    if args.file_verbosity:
        file_handler.setLevel(levels[args.file_verbosity])

    logger.info('Console logging level: {}'.format(console_handler.level))
    logger.info('File logging level: {}'.format(file_handler.level))

    # Start daemon
    daemon = Daemon(args.config_folder_path, args.sharing_folder_path)
    daemon.start()


if __name__ == '__main__':
    main()
else:
    # Silence logger in case of import
    console_handler.setLevel(logging.CRITICAL)
