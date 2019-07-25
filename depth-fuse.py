#!/usr/bin/env python3

import os
import sys
import stat

import calendar
import time
import logging
from datetime import datetime

from dptrp1.dptrp1 import DigitalPaper, ResolveObjectFailed
from fuse import FUSE, FuseOSError, Operations
from errno import *
from urllib.parse import quote_plus



class DptConn:
    log = logging.getLogger("fuse.dptconn")

    def __init__(self, dpt, auth):
        self.dpt = dpt
        self.auth = auth

    def _try_dpt(self, query):
        try:
            ret = query()
            if "ok" not in dir(ret) or ret.ok or ret.json()["message"] != "Authentication is required.":
                return ret
        except ResolveObjectFailed as e:
            if e.args[1] != "Authentication is required.":
                self.log.debug("dpt raised error ('%s')", "', '".join(e.args))
                raise
        self.log.info("_try_dpt(): dpt reauth")
        self.dpt.authenticate(*self.auth)
        return query()

# file cache:
# - create file: create empty remote file and local file
# - open file: check cache if anything changed
# - otherwise only change if cache updates for some other reason
# - write file: marks file 'dirty', until fsync / close we don't get remote changes
# - rename file: open handles stay the same b/c they only reference file ID
# - delete file: keep handles open but delete remote and don't update
#
# metadata that is also updated:
# - change (= create) time is never touched
# - atime never touched
# - modify time for files and directories


# file handles:
# - CREAT, EXCL, NOCTTY: handled by kernel
# - RDONLY, WRONLY, RDWR, EXEC, SEARCH: used by FS to check if op is permitted
# - O_TRUNC handled internally if atomic_o_trunc is *not* set
# - handle O_APPEND when writeback caching is disabled
# - writeback caching *should* be disabled; instead write-through should be used
class FileCache:

    log = logging.getLogger("fuse.filecache")

    def __init__(self, dptconn, tmpdir):
        self.directory = {}
        self.dptconn = dptconn
        self.tmpdir = tmpdir
        os.makedirs(self.tmpdir)
        self.blocksize = os.statvfs(self.tmpdir)['f_bsize']
        self.log.info("Initializing FileCache, tmpdir: %s", tmpdir)

    def create_file(self, path):
        """
        creates a file in /path/
        file must not exist.
        """
        remotepath = InfoCache._normalize_path(path)
        self.log.debug("create_file('%s') --> remote '%s'", path, remotepath)

        dirname, filename = remotepath.rsplit("/", 1)
        try:
            dir_id = self.dptconn._try_dpt(lambda: self.dptconn.dpt._get_object_id(dirname))
        except ResolveObjectFailed as e:
            if e.args[1] == "The designated resource was not found.":
                self.log.info("create_file('%s') raising ENOENT: underlying path not found", path)
                raise FuseOSError(ENOENT)
            self.log.exception("create_file('%s') raising EIO: _get_object_id('%s') raised unexpected error", path, dirname)
            raise FuseOSError(EIO)
        info = {
            'file_name': filename,
            'parent_folder_id': dir_id,
            'document_source': ''
        }
        response = self.dptconn._try_dpt(lambda: self.dptconn.dpt._post_endpoint("/documents2", data = info)).json()
        if "document_id" not in response:
            self.log.error("create_file('%s') raising EIO: "
                           "_post_endpoint({'file_name': '%s', 'parent_folder_id': '%s'}) returned bad response: %s",
                           path, filename, dir_id, response)
            raise FuseOSError(EIO)
        docid = response["document_id"]
        localfile = os.path.join(self.tmpdir, docid)
        self.log.debug("create_file('%s') creating local file %s", path, localfile)
        open(localfile, 'a').close()
        try:
            response = self._upload_file(localfile, filename, docid)
        except:
            try:
                os.remove(localfile)
            raise
        self.directory[docid] = {
            "id": docid,
            "localfile": localfile,
            "internal_filename": filename,
            "local_revision": response["file_revision"],
            "refcount": 1,
            "is_deleted": False,
            "is_dirty": False,
        }
        self.log.debug("create_file('%s') new entry %s", path, self.directory[docid])
        self.log.debug("create_file('%s') returning %s", path, (docid, localfile))
        return (docid, localfile)

    def notify_update(self, entry):
        """
        whenever cache is updated the notify_update() function is called by InfoCache.
        if the local revision is out of date and we have open handles and the local file is not dirty we pull the new file.
        """
        docid = entry['id']
        self.log.debug("notify_update('%s') (%s)", docid, entry['internal_path'])
        if docid not in self.directory:
            self.log.debug("notify_update('%s') ignoring (not in directory)", docid)
            return
        direntry = self.directory[docid]
        if direntry['is_dirty'] or direntry['is_deleted']:
            self.log.debug("notify_update('%s') ignoring (%s)", docid, 'dirty' if direntry['is_dirty'] else 'deleted')
            return
        if direntry['local_revision'] == entry['file_revision']:
            self.log.debug("notify_update('%s') ignoring (not changed)", docid)
            return
        if not direntry['refcount']:
            # remote file changed, but we kept it in cache without any connectins --> delete
            self.log.debug("notify_update('%s') deleting changed entry with zero references", docid)
            try:
                os.remove(direntry['localfile'])
            except:
                self.log.exception("notify_update('%s') removing file %s failed.", docid, direntry['localfile'])
            del self.directory[docid]
            return
        self.log.info("notify_update('%s') file changed, updating local file %s from remote %s",
                      docid, direntry['localfile'], entry['internal_path'])
        self._write_to_file(direntry['localfile'], entry['internal_path'])

    def _write_to_file(self, localfile, remotefile):
        self.log.debug("_write_to_file(localfile = '%s', remotefile = '%s')", localfile, remotefile)
        try:
            with open(localfile, 'wb') as fh:
                fh.write(self.dptconn._try_dpt(lambda: self.dptconn.dpt.download(remotefile)))
        except:
            self.log.exception("_write_to_file(localfile = '%s', remotefile = '%s'): dpt.download() raised an error",
                               localfile, remotefile)
            raise FuseOSError(EIO)
        self.log.debug("_write_to_file(localfile = '%s', remotefile = '%s') successful", localfile, remotefile)

    def _upload_file(self, localfile, filename, docid):
        targeturl = "/documents/{}/file".format(docid)
        self.log.debug("_upload_file(localfile = '%s', filename = '%s', docid = '%s') --> targeturl %s",
                       localfile, filename, docid, targeturl)
        with open(localfile, 'rb') as fh:
            files = {"file": (quote_plus(filename), fh, "rb")}
            response = self.dptconn._try_dpt(lambda: self.dptconn.dpt._put_endpoint(targeturl, files = files)).json()
        if response.get("completed", "no") != "yes" or "file_revision" not in response:
            self.log.error("_upload_file(localfile = '%s', filename = '%s', docid = '%s') raising EIO: "
                           "_put_endpoint('%s') returned bad response: %s",
                           localfile, filename, docid, targeturl, response)
            raise FuseOSError(EIO)
        self.log.debug("_upload_file(localfile = '%s', filename = '%s', docid = '%s') returning %s",
                       localfile, filename, docid, response)
        return response

    def open_file(self, entry):
        """
        open file, entry is a InfoCache cache entry
        """
        docid = entry['id']
        self.log.debug("open_file('%s') (%s)", docid, entry['internal_path'])
        if docid in self.directory:
            self.directory[docid]["refcount"] += 1
            self.log.debug("open_file('%s') found in directory; incrementing refcount to %s",
                           docid, self.directory[docid]["refcount"])
            self.log.debug("open_file('%s') returning %s",
                           docid, (docid, self.directory[docid]['localfile']))
            return (docid, self.directory[docid]['localfile'])
        localfile = os.path.join(self.tmpdir, docid)
        self._write_to_file(localfile, entry['internal_path'])
        self.directory[docid] = {
            "id": docid,
            "localfile": localfile,
            "internal_filename": entry["internal_filename"],
            "local_revision": entry["file_revision"],
            "refcount": 1,
            "is_deleted": False,
            "is_dirty": False
        }
        self.log.debug("open_file('%s') new entry %s", docid, self.directory[docid])
        self.log.debug("open_file('%s') returning %s", docid, (docid, localfile))
        return (docid, localfile)

    def close_file(self, docid):
        self.log.debug("close_file('%s')", docid)
        if docid not in self.directory:
            self.log.error("close_file('%s') raising EIO: closing file with no directory entry", docid)
            raise FuseOSError(EIO)
        direntry = self.directory[docid]
        if direntry['refcount'] <= 0:
            self.log.error("close_file('%s') raising EIO: closing file with non-positive refcount %s", docid, direntry['refcount'])
            raise FuseOSError(EIO)
        self.write_back(docid)
        direntry["refcount"] -= 1
        if direntry['refcount'] <= 0 and direntry['is_deleted']:
            self.log.debug("close_file('%s') refcount of deleted file reached zero --> deleting %s", docid, direntry['localfile'])
            try:
                os.remove(direntry['localfile'])
            except:
                self.log.exception("close_file('%s') removing file %s failed", docid, direntry['localfile'])
            del self.directory[docid]
        self.log.debug("close_file('%s') successful", docid)

    def mark_dirty(self, docid):
        """
        notify cache that we have written to the local copy of a file
        """
        self.log.debug("mark_dirty('%s')", docid)
        if docid not in self.directory:
            self.log.error("mark_dirty('%s') raising EIO: file has no directory entry", docid)
            raise FuseOSError(EIO)
        self.directory[docid]['is_dirty'] = True
        self.log.debug("mark_dirty('%s') successful", docid)

    def write_back(self, docid):
        """
        if the file is dirty, write it back
        """
        self.log.debug("write_back('%s')", docid)
        if docid not in self.directory:
            self.log.error("write_back('%s') raising EIO: file has no directory entry", docid)
            raise FuseOSError(EIO)
        direntry = self.directory[docid]
        if not direntry['is_dirty'] or direntry['is_deleted']:
            self.log.debug("write_back('%s') ignoring (%s)", docid, 'dirty' if direntry['is_dirty'] else 'deleted')
            return

        localfile = direntry['localfile']
        filename = direntry['internal_filename']
        response = self._upload_file(localfile, filename, docid)

        self.log.debug("write_back('%s') updating revision: %s -> %s", docid, direntry['local_revision'], response['file_revision'])
        direntry['local_revision'] = response['file_revision']
        direntry["is_dirty"] = False
        self.log.debug("write_back('%s') successful", docid)

    def clear_cache(self):
        """
        delete all local files with refcount 0
        """
        self.log.debug("clear_cache() at cache size: %s", len(self.directory))
        for docid, direntry in self.directory.items():
            if direntry['refcount'] <= 0:
                self.log.debug("clear_cache() removing file %s (docid %s)", direntry['localfile'], direntry['id'])
                try:
                    os.remove(direntry['localfile'])
                except:
                    self.log.exception("clear_cache() removing file %s failed (docid %s)", direntry['localfile'], direntry['id'])
            del self.directory[docid]
        self.log.debug("clear_cache() finished. Cache size: %s", len(self.directory))

    def delete_file(self, entry):
        docid = entry['id']
        self.log.debug("delete_file('%s') (%s)", docid, entry['internal_path'])
        if docid in self.directory:
            self.log.debug("delete_file('%s') has directory entry: setting 'is_deleted'", docid)
            self.directory[docid]['is_deleted'] = True
        if entry['is_folder']:
            endpoint = f"/folders/{docid}"
        else:
            endpoint = f"/documents/{docid}"
        response = self.dptconn._try_dpt(lambda: self.dptconn.dpt._delete_endpoint(endpoint))
        if not response.ok:
            self.log.error("delete_file('%s') raising EIO: deleting remote file failed. response %s", docid, response.json())
            raise FuseOSError(EIO)


class InfoCache:
    log = logging.getLogger("fuse.infocache")


    def __init__(self, dptconn, filecache, lifetime):
        """
        lifetime: cache invalidation time in seconds
        """
        self.log.info("Initializing InfoCache. lifetime: %s", lifetime)

        self.lifetime = lifetime
        self.cache = {}
        self.dptconn = dptconn
        self.attr_proto = {
            'st_ino': 0,
            'st_dev': 0,
            'st_nlink': 1,
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_size': 0
        }
        self.filecache = filecache

    def reset(self):
        self.log.debug("reset()")
        self.cache.clear()

    @staticmethod
    def _normalize_path(path, is_dir = False):
        """
        path is understood to be a whole path
        """
        if path and path[-1] == '/':
            if not is_dir:
                raise FuseOSError(EIO)
            path = path[:-1]
        if path and path[0] == '/':
            path = path[1:]
        if not is_dir:
            if path.lower().endswith("°.pdf"):
                path = path[:-5] + "°" + path[-5:]
            elif not path.lower().endswith(".pdf"):
                path = path + "°.pdf"
        return "Document/" + path

    @staticmethod
    def _denormalize_path(path, is_dir = False):
        """
        'path' may be a whole path or a filename
        """
        if "/" in path:
            if not path.startswith("Document/"):
                raise FuseOSError(EIO)
            path = path[9:]
        if not is_dir:
            if path.lower().endswith("°°.pdf"):
                path = path[:-5] + path[-4:]
            elif path.lower().endswith("°.pdf"):
                path = path[:-5]
        return path

    def invalidate_below(self, path):
        """
        invalidate cache for 'path' and all entries below 'path'.
        'path' must be a directory.
        """
        self.log.debug("invalidate_below('%s')", path)
        path = self._normalize_path(path, is_dir = True)
        for k in self.cache:
            if k.startswith(path):
                self.log.debug("invalidate_below('%s'): invalidating %s", path, k)
                del self.cache[k]
        pathbelow = os.path.dirname(path) + "/"
        if pathbelow in self.cache:
            del self.cache[pathbelow]

        self.log.debug("invalidate_below('%s') done", path)

    def invalidate(self, path, is_dir = False):
        """
        invalidate cache for 'path'
        """
        self.log.debug("invalidate('%s')", path)
        path = self._normalize_path(path, is_dir = is_dir)
        if path in self.cache:
            del self.cache[path]
        pathdir = path + "/"
        if pathdir in self.cache:
            del self.cache[pathdir]
        pathbelow = os.path.dirname(path) + "/"
        if pathbelow in self.cache:
            del self.cache[pathbelow]
        self.log.debug("invalidate('%s') done", path)

    @staticmethod
    def _date_to_epoch(timestring):
        return calendar.timegm(time.strptime(timestring, '%Y-%m-%dT%H:%M:%SZ'))

    def _file_to_cache_entry(self, json):
        """
        insert json reply into cache
        """
        self.log.debug("_file_to_cache_entry()")
        # > If a field is meaningless or semi-meaningless (e.g., st_ino) then it should be set to 0 or given a "reasonable" value.
        attr = self.attr_proto.copy()
        attr['st_size'] = int(json.get('file_size', '0')),
        attr['st_ctime'] = self._date_to_epoch(json['created_date'])
        attr['st_mtime'] = self._date_to_epoch(json.get('modified_date', json['created_date']))

        if 'reading_date' in json:
            attr['st_atime'] = self._date_to_epoch(json['reading_date'])
        else:
            attr['st_atime'] = attr['st_mtime']

        if json['entry_type'] == 'folder':
            attr['st_mode'] = 0o755 | stat.S_IFDIR
            attr['st_nlink'] = 2
        elif json['entry_type'] == 'normal':
            attr['st_mode'] = 0o644 | stat.S_IFREG
        else:
            raise FuseOSError(EIO)

        attr['st_blocks'] = (attr['st_size'] + self.filecache.blocksize - 1) // self.filecache.blocksize

        is_dir = json['entry_type'] == 'folder',
        entry = {
            'created': datetime.now(),
            'id': json['entry_id'],
            'internal_filename': json['entry_name'],
            'internal_path': json['entry_path'],
            'filename': self._denormalize_path(json['entry_name'], is_dir = is_dir),
            'path': self._denormalize_path(json['entry_path'], is_dir = is_dir),
            'is_folder': is_dir,
            'attr': attr,
            'file_revision': json.get('file_revision', '')
        }
        self.filecache.notify_update(entry)
        self.log.debug("_file_to_cache_entry(): returning %s", (json['entry_path'], {'id': entry['id'], 'is_folder': is_dir}))
        return (json['entry_path'], entry)

    def _dir_to_cache_entry(self, json_col, path):
        """
        insert dir listing into cache

        path must be normalized but not end with an '/'
        """
        path = path + "/"
        self.log.debug("_dir_to_cache_entry(path = '%s')", path)
        allpaths = [x['entry_path'] for x in json_col]
        allnames = [x['entry_name'] for x in json_col]
        is_dir = [x['entry_type'] == 'folder' for x in json_col]
        if not all(x.startswith(path) for x in allpaths):
            self.log.error("_dir_to_cache_entry(path = '%s') raising EIO: sub-paths found that do not start with path: %s",
                           path, [x for x in allpaths if not x.startswith(path)])
            raise FuseOSError(EIO)
        if any("/" in x[len(path):] for x in allpaths):
            self.log.error("_dir_to_cache_entry(path = '%s') raising EIO: sub-paths found that are not immediately below path: %s",
                           path, [x for x in allpaths if "/" in x[len(path):]])
            raise FuseOSError(EIO)

        entry = {
            'created': datetime.now(),
            'internal_content': allpaths,
            'internal_filenames': allnames,
            'content': [self._denormalize_path(x, is_dir = isd) for x, isd in zip(allpaths, is_dir)],
            'filenames': [self._denormalize_path(x, is_dir = isd) for x, isd in zip(allnames, is_dir)],
            'is_folder': is_dir
        }
        self.log.debug("_dir_to_cache_entry('%s'): returning %s", path, (path, {'filenames': entry['filenames']}))
        return (path, entry)

    def _fill_cache(self, path):
        self.log.debug("_fill_cache('%s')", path)
        try:
            objects = self.dptconn._try_dpt(lambda: self.dptconn.dpt.list_objects_in_folder(path))
        except ResolveObjectFailed as e:
            if e.args[1] == "The designated resource was not found.":
                self.log.info("_fill_cache('%s') raising ENOENT: path not found", path)
                raise FuseOSError(ENOENT)
            self.log.exception("_fill_cache('%s') raising EIO: list_objects_in_folder('%s') raised unexpected error", path, path)
            raise FuseOSError(EIO)
        self._dir_to_cache_entry(objects, path)
        for obj in objects:
            self._file_to_cache_entry(obj)
        self.log.debug("_fill_cache('%s') successful", path)

    def _get_cache_if_any(self, path):
        self.log.debug("_get_cache_if_any('%s')", path)
        if path not in self.cache:
            self.log.debug("_get_cache_if_any('%s'): not in cache", path)
            return None
        item = self.cache[path]
        age = (datetime.now() - item['created']).total_seconds()
        if age > self.lifetime:
            self.log.debug("_get_cache_if_any('%s'): "
                           "found in cache, but old entry created %s, %s seconds out of date when self.lifetime is %s",
                           path, item['created'], age, self.lifetime)
            del self.cache[path]
            return None
        self.log.debug("_get_cache_if_any('%s'): found in cache", path)
        return item

    def _get_cache(self, path, consider_refresh = True):
        self.log.debug("_get_cache('%s')", path)
        fromcache = self._get_cache_if_any(path)
        if fromcache is not None:
            self.log.debug("_get_cache('%s'): returning found cache entry", path)
            return fromcache
        if consider_refresh:
            basename = path.rsplit("/", 1)[0]
            self.log.debug("_get_cache('%s'): no cache entry found, so filling cache for directory '%s'", path, basename)
            self._fill_cache(basename)
            fromcache = self._get_cache_if_any(path)
            if fromcache is not None:
                self.log.debug("_get_cache('%s'): returning cache entry newly created by _fill_cache", path)
                return fromcache
        self.log.info("_get_cache('%s') raising ENOENT: cache entry not found even after querying remote", path)
        raise FuseOSError(ENOENT)

    def get_file(self, path, is_dir = False):
        self.log.debug("get_file('%s', is_dir = %s)", path, is_dir)
        if is_dir is None:
            self.log.debug("get_file('%s', is_dir = %s): first trying as directory", path, is_dir)
            normpath = self._normalize_path(path, is_dir = True)
            return self._get_cache(normpath)
            except FuseOSError as e:
                if e.errno != ENOENT:
                    self.log.debug("get_file('%s', is_dir = %s): re-raising error %s", path, is_dir, e)
                    raise
                self.log.debug("get_file('%s', is_dir = %s): retrying as file", path, is_dir)
                normpath = self._normalize_path(path, is_dir = is_dir)
                return self._get_cache(normpath, consider_refresh = False)
        normpath = self._normalize_path(path, is_dir = is_dir)
        return self._get_cache(normpath)


    def get_dir(self, path):
        self.log.debug("get_dir('%s')", path)
        path = self._normalize_path(path, is_dir = True) + "/"
        return self._get_cache(path)


class DptFs(Operations, LoggingMixIn):
    '''
    Access to the dpt-rp1 file system using FUSE

    tmpdir: directory where local copies of remote are saved
    '''
# notes:
# - delegate permissions checking to kernel by passing `default_permissions` to fuse_new
# - FUSE_CAP_HANDLE_KILLPRIV disabled: truncate must reset setuid / setgid bits
# functions:

# how things should work
# - open loads file to cache
# - everything maps to that cached file
# - fsync writes file to device (?)
# - release writes file to device, flags it as can-be-deleted
#   - delete lru file if using too much cache
# - destroy deletes cache
# - '°' as escape character: °°.pdf --> °.pdf, °.pdf --> removed

    dptlog = logging.getLogger("fuse.dptfs")
    def __init__(self, dptconn, filecache, infocache):
        self.dptconn = dptconn
        self.filecache = filecache
        self.infocache = infocache
        self.dptlog.info("initializing DptFs")

################################################
# FS Global Operations                         #
################################################

    def init(self, path):
        '''
        initialize filesystem
        - init: ESSENTIAL init filesystem; return value becomes `fuse_context->private_data`
        '''
        self.dptlog.info("init('%s'): clearing caches", path)
        self.filecache.clear_cache()
        self.infocache.reset()

    def destroy(self, path):
        '''
        Clean up filesystem. Called on FS exit.
        - destroy: ESSENTIAL filesystem exit
        '''
        self.dptlog.info("destroy('%s'): clearing caches", path)
        self.filecache.clear_cache()
        self.infocache.reset()

    def statfs(self, path):
        '''
        get file system statistics
        - statfs: get fs statistics; dictionary with keys from statvfs C structure
        '''
        self.dptlog.info("statfs('%s')", path)
        stat = os.statvfs(self.filecache.tmpdir)
        try:
            storage = self.dptconn._try_dpt(lambda: self.dptconn.dpt.get_storage())
        except ResolveObjectFailed as e:
            self.exception("statfs('%s') raising EIO: get_storage() failed", path)
            raise FuseOSError(EIO)

        try:
            numfiles = len(self.dptconn._try_dpt(lambda: self.dptconn.dpt.list_all()))
        except ResolveObjectFailed as e:
            self.exception("statfs('%s') raising EIO: list_all() failed", path)
            raise FuseOSError(EIO)

        stat.update({
            f_bavail: storage['available'] // stat['f_bsize'],  # free blocks available to unpriv. user
            f_bfree: storage['available'] // stat['f_bsize'],  # free blocks in fs
            f_blocks: storage['capacity'] // stat['f_bsize'],  # total blocks in fs
            f_files: numfiles,  # total file nodes / files in fs
            f_flags: 0,  # ignored
        })
        return stat

################################################
# File Operations                              #
################################################

    def create(self, path, mode, fi=None):
        '''
        Create & Open a File
        - create: ESSENTIAL create & open file. (can also be done by mkdnod() --> open() )
        '''
        self.dptlog.info("create('%s', mode = 0o%o, fi = %s)", path, mode, fi)
        # TODO: check that file does not exist
        info = self.filecache.create_file(path)
        return info + (open(info[1], "+b"),)


    def open(self, path, flags):
        '''
        open a file
        - open: ESSENTIAL: open file that exists; some flags handled by kernel

        file handles are triples: [docid, local file path, local opened file handle]
        '''
        # -> filecache: open_file
        self.dptlog.info("create('%s', flags = 0o%o)", path, flags)
        entry =  self.infocache.get_file(path, is_dir = None)
        if entry['is_folder']:
            self.dptlog.debug("create('%s', flags = 0o%o): Directory found: return None", path, flags)
            return None

        info = self.filecache.open_file(entry)

        return info + (open(info[1], "+b"),)


    def release(self, path, fh):
        '''
        release an open file after there are no more references
        - release: ESSENTIAL: no more references to file
        '''
        # -> filecache: close_file
        self.dptlog.info("release('%s', fh = %s)", path, fh)
        fh[2].close()
        self.filecache.close_file(fh[0])
        return 0

    def read(self, path, size, offset, fh):
        '''
        Read from File
        - read: read data from file
        '''
        self.dptlog.info("read('%s', %s, %s, fh = %s)", path, size, offset, fh)
        fh[2].seek(offset)
        return fh[2].read(size)


    def write(self, path, data, offset, fh):
        '''
        Write to File
        - write: write to an open file
        '''
        self.dptlog.info("write('%s', <data>[%s], %s, fh = %s)", path, len(data), offset, fh)
        self.filecache.mark_dirty(fh[0])
        fh[2].seek(offset)
        fh[2].write(data)
        return 0

    def rename(self, old, new):
        '''
        Rename file
        - rename: rename a file
        '''
        self.dptlog.info("rename('%s', '%s')", old, new)

        oldentry = self.infocache.get_file(old, is_dir = None)
        self.dptlog.debug("rename('%s', '%s') 'old' is a %s", old, new, "folder" if oldentry['is_folder'] else "file")
        try:
            newentry = self.infocache.get_file(old, is_dir = None)
            self.dptlog.debug("rename('%s', '%s') 'new' exists and is a %s", old, new, "folder" if newentry['is_folder'] else "file")
        except FuseOSError as e:
            if e.errno != ENOENT:
                self.dptlog.debug("rename('%s', '%s'): re-raising error %s", old, new, e)
                raise
            newentry = None

        target_dir = self.infocache.get_file(os.path.dirname(new), is_dir = True)
        if not target_dir['is_folder']:
            self.dptlog.warning("rename('%s', '%s') raising ENOTDIR: parent of 'new' is not a directory", old, new)
            raise FuseOSError(ENOTDIR)
        try:
            if oldentry['is_folder']:
                # 'old' is a folder
                targeturl = f"/folders/{oldentry['id']}"
                if newentry is not None:
                    if not newentry['is_folder']:
                        self.dptlog.warning("rename('%s', '%s') raising ENOTDIR: source is directory, target is not a directory",
                                            old, new)
                        raise FuseOSError(ENOTDIR)
                    if self.infocache.get_dir(new): # target is not empty
                        self.dptlog.warning("rename('%s', '%s') raising ENOTEMPTY: source is directory, target is non-empty directory",
                                            old, new)
                        raise FuseOSError(ENOTEMPTY)
                    self.dptlog.debug("rename('%s', '%s') deleting newentry", old, new)
                    self.filecache.delete_file(newentry)
            else:
                # 'old' is a file
                targeturl = f"/documents/{oldentry['id']}"
                if newentry is not None:
                    if newentry['is_folder']:
                        self.dptlog.warning("rename('%s', '%s') raising EISDIR: source is file, target is directory", old, new, e)
                        raise FuseOSError(EISDIR)
                    self.dptlog.debug("rename('%s', '%s') deleting newentry", old, new)
                    self.filecache.delete_file(newentry)


            new_name = os.path.basename(self.infocache._normalize_path(new, is_dir = olentry['is_folder']))
            data = self._copy_move_data(oldentry['id'], target_dir['id'], new_name)
            self.dptlog.debug("rename('%s', '%s') setting %s for item %s", old, new, data, targeturl)
            try:
                response = self.dptconn._try_dpt(lambda: self.dptconn.dpt._post_endpoint(targeturl, data=data))
                if not response.ok:
                    raise ResolveObjectFailed(path, response.json()["message"])
            except ResolveObjectFailed as e:
                self.exception("rename('%s', '%s') raising EIO: posting data %s to url %s failed", old, new, data, targeturl)
                raise FuseOSError(EIO)
        finally:
            # if oldpath is a directory, cache must be invalidated recursively
            if oldentry['is_folder']:
                self.dptlog.debug("rename('%s', '%s') recursively clearing cache", old, new)
                for x in [old, new]:
                    self.infocache.invalidate_below(x)
            else:
                self.dptlog.debug("rename('%s', '%s') clearing cache", old, new)
                for x in [old, new]:
                    self.infocache.invalidate(x)
        self.dptlog.debug("rename('%s', '%s'): success", old, new)
        return 0

    def unlink(self, path):
        '''
        Delete file
        - unlink: remove a file
        '''
        # delete_file
        # invalidate cache
        # TODO: check if unlink can be called for directory
        self.dptlog.info("unlink('%s')", path)
        entry = self.infocache.get_file(path)
        if entry['is_folder']:
            self.dptlog.error("unlink('%s') raising EIO: unlink directory", path)
            raise FuseOSError(EIO)
        self.filecache.delete_file(entry)
        return 0

    def truncate(self, path, length, fh=None):
        '''
        Set file length
        - truncate: ESSENTIAL: change size of a file.
        - ftruncate: same with open file --> truncate
        '''
        self.dptlog.info("truncate('%s', %s, fh = %s)", path, length, fh)
        # filecache: mark_dirty
        # TODO: check that this does not happen to directories
        mustclose = False
        if fh is None:
            self.dptlog.debug("truncate('%s', %s, fh = %s): no fh, so calling open()", path, length, fh)
            fh = self.open(path, os.O_WRONLY)
        try:
            self.filecache.mark_dirty(fh[0])
            fh[2].truncate(length)
        finally:
            if mustclose:
                self.dptlog.debug("truncate('%s', %s, fh = %s): no fh was given, so calling release()", path, length, fh)
                self.release(path, fh)
        return 0

#  - mknod: create nodes except dir, symlink, regular-file (if create() is given) --> EINVAL

################################################
# Sync and Lock                                #
################################################

    def fsync(self, path, datasync, fh):
        """
        - fsync: ESSENTIAL synch file contents; if (datasync) --> only user data, not metadata
        """
        self.dptlog.info("fsync('%s', fh = %s)", path, fh)
        fh[2].flush()
        self.filecache.write_back(fh[0])
        return 0

    def flush(self, path, fh):
        """
        - flush: ESSENTIAL called on each close(), may be called multiple times for each open()
        """
        self.dptlog.info("flush('%s', fh = %s)", path, fh)
        fh[2].flush()
        self.filecache.write_back(fh[0])
        return 0

#  - lock: ((ESSENTIAL)) file locking; done by kernel if not given

################################################
# Dir Operations                               #
################################################

    def mkdir(self, path, mode):
        '''
        - mkdir: create directory
        '''
        self.dptlog.info("mkdir('%s')", path)

        try:
            self.infocache.get_file(path, is_dir = None)
            self.log.warning("mkdir('%s') raising EEXIST: path exists", path)
            raise FuseOSError(EEXIST)
        except FuseOSError as e:
            if e.errno != ENOENT:
                self.log.exception("mkdir('%s'): re-raising error from get_dir()", path)
                raise
        target_dir = self.infocache.get_file(os.path.dirname(path), is_dir = True)
        if not target_dir['is_folder']:
            self.dptlog.warning("mkdir('%s') raising ENOTDIR: parent of 'path' is not a directory", path)
            raise FuseOSError(ENOTDIR)

        data = {"folder_name": os.path.basename(path), "parent_folder_id": target_dir['id']}
        ret = self.dptconn._try_dpt(lambda: self.dptconn.dpt._post_endpoint("/folders2", data = data))
        if not ret.ok or "folder_id" not in ret.json():
            self.dptlog.warning("mkdir('%s') raising EIO: _post_endpoint returned %s", path, ret.json())
            raise FuseOSError(EIO)
        self.dptlog.debug("mkdir('%s') successful", path)
        return 0


    def rmdir(self, path):
        '''
        - rmdir: remove directory
        should only succeed if the dir is empty
        '''
        self.dptlog.info("rmdir('%s')", path)
        if self.infocache.get_dir(path):
            self.dptlog.warning("rmdir('%s') raising ENOTEMPTY: not empty.", path)
            raise FuseOSError(ENOTEMPTY)
        self.infocache.invalidate_below(path)
        self.filecache.delete_file(newentry)
        return 0

#  - opendir: ESSENTIAL
#  - releasedir: ESSENTIAL: release directory

    def readdir(self, path, fh):
        '''
        return a list of names
        - readdir: read directory; can get complicated
        '''
        self.dptlog.info("readdir('%s', fh = %s)", path, fh)
        entries = ['.', '..'] + get_dir(path)['filenames']
        self.dptlog.debug("readdir('%s', fh = %s) files: %s", path, fh, entries)
        return entries

#  - fsyncdir: ESSENTIAL synch dir contents; if (datasync) --> only user data, not metadata

################################################
# Permissions                                  #
################################################

def chmod(self, path, uid, gid):
    '''
    - chmod: change permissions of file
    '''
    self.dptlog.warning("chmod('%s'): raising EPERM (permissions not supported)", path)
    raise FuseOSError(EPERM)

def chown(self, path, uid, gid):
    '''
    - chown: change owner / group of file
    '''
    self.dptlog.warning("chown('%s'): raising EPERM (permissions not supported)", path)
    raise FuseOSError(EPERM)

#  - access: ESSENTIAL check access permissions, not called if default_permissions mount option given

################################################
# Attributes                                   #
################################################

    def getattr(self, path, fh=None):
        '''
        get file attributes
        - getattr: get file attributes
        - fgetattr: same with open file --> getattr
        '''
        self.dptlog.info("getattr('%s', fh = %s)", path, fh)
        # if fh is not None we know we are dealing with a file. otherwise we first check if the directory exists
        if fh is None:
            entry = self.infocache.get_file(path, is_dir = None)
        else:
            self.dptlog.debug("getattr('%s', fh = %s): only trying as file", path, fh)
            entry = self.infocache.get_file(path, is_dir = False)
        self.dptlog.debug("getattr('%s', fh = %s): returning attributes", path, fh)
        return entry['attr']

    def utimens(self, path, times=None):
        '''
        Change file time
        - utime: superseded by utimens
        - utimens: change acces / modification times, supersedes utime(). `times` is (atime, mtime) tuple, or None--> current time
        '''
        # if times is None:
        #     mtime = time.time()
        # else:
        #     mtime = times[1]

        self.dptlog.info("utimens('%s', times = %s): not implenented", path, times)
        return 0


#  - getxattr: get file extended attributes. returns ENOTSUP by default, which is what we want.
#  - setxattr: set extended attributes. returns ENOTSUP by default, which is what we want.
#  - removexattr: remove extended attribute. returns ENOTSUP by default, which is what we want.
#  - listxattr: list extended attributes. returns empty list by default, which is what we want.

################################################
# Links                                        #
################################################

    def symlink(self, target, source):
        '''
        Create symbolic link (not supported)
        - symlink: create symbolic link
        '''
        self.dptlog.warning("symlink('%s'): raising EPERM (links not supported)", path)
        raise FuseOSError(EPERM)

    def link(self, target, source):
        '''
        Hardlink (not supported)
        - link: create hardlink
        '''
        self.dptlog.warning("link('%s'): raising EPERM (links not supported)", path)
        raise FuseOSError(EPERM)

    def readlink(self, path):
        '''
        Read symbolic link (not supported)
        - readlink: read target of a symbolic link
        '''
        self.dptlog.warning("readlink('%s'): raising EIO (links not supported)", path)
        raise FuseOSError(EIO)

################################################
# Others: Not Implemented                      #
################################################

#  - bmap: map block idx within file to device
# - ioctl: ioctl. apparently not used in python fuse
# - poll: poll for io readiness; should call fuse_notify_poll. apparently not used in python fuse


if __name__ == '__main__':
    clientidfile = '.dpt-client'
    clientkeyfile = '.dpt-key'
    address = sys.argv[1]
    dpt = DigitalPaper(address)
    auth = [open(clientidfile).readline().strip(),
            open(clientkeyfile).read()]

    mountpoint = "mount"
    tmpdir = "/tmp/mount"
    lifetime = 60

    dptconn = DptConn(dpt, auth)
    filecache = FileCache(dptconn, tmpdir)
    infocache = InfoCache(dptconn, filecache, lifetime)

    ops = DptFs(dpt, dptconn, filecache, infocache)

    FUSE(ops, mountpoint,
         foreground=True,
         nothreads=True,
         default_permissions = True,
         allow_other = True)
