import fsspec
import h5py
import inspect
import logging
import os
import s3fs

from botocore.exceptions import ClientError
from urllib.parse import urlparse
from functools import wraps

from hydra_base import config
from hydra_base.util import NullAdapter
from hydra_base.lib.storage.readers import group_reader_map

log = logging.getLogger(__name__)


def filestore_url(argname, rewrite_func="url_to_filestore_path"):
    """
      Decorator which rewrites the <argname> argument to the decorated function
      to include the appropriate path according to the <rewrite_func> argument.
    """
    def dfunc(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            fas = inspect.getfullargspec(func)
            idx = fas.args.index(argname)
            av = inspect.getargvalues(inspect.currentframe())
            """
              NB. self appears in args from getfullargspec, but not in
              getargvalues().locals(). Adjust the index to account for
              this difference.
            """
            idx -= 1
            pre_arg = av.locals["args"][idx]
            try:
                rw_func = getattr(self, rewrite_func)
            except AttributeError as ae:
                raise ValueError(f"Invalid rewrite_func '{rewrite_func}' for @filestore_url") from ae
            # rw_func is a bound method so may be called directly
            post_arg = rw_func(pre_arg)
            insert = (*av.locals["args"][:idx], post_arg, *av.locals["args"][idx+1:])
            return func(self, *insert, **kwargs)

        return wrapper
    return dfunc


class HdfStorageAdapter():
    """
       Utilities to describe and retrieve data from HDF storage
    """

    def __init__(self):
        self.fsspec_args = dict(
            mode='rb',
            anon=self._get_anon(),
            default_fill_cache=True
        )
        self.config = self.__class__.get_hdf_config()
        self.filestore_path = self.config.get("hdf_filestore")
        if self.filestore_path and not os.path.exists(self.filestore_path):
            os.makedirs(self.filestore_path, exist_ok=True)

    def _get_anon(self):
        """
            If there are AWS credentials present in the environment, 
            then assume that this user will be not anonymous
        """

        self.accesskeyid = os.getenv('AWS_ACCESS_KEY_ID')
        self.secretaccesskey = os.getenv('AWS_SECRET_ACCESS_KEY')
        if self.accesskeyid not in ('' , None) and self.secretaccesskey not in ('', None):
            return False

        home = os.getenv('HOME')
        default_credentials_path = os.path.join(home, '.aws', 'credentials')
        credentials_path = os.getenv('AWS_SHARED_CREDENTIALS_FILE', default_credentials_path)
        #If there is a credentials file, then assume the user is not anonymous
        if os.path.exists(credentials_path):
            return False

        return True

    @staticmethod
    def get_hdf_config(config_key="storage_hdf", **kwargs):
        numeric = ()
        boolean = ("disable_hdf", )
        hdf_keys = [k for k in config.CONFIG.options(config_key) if k not in config.CONFIG.defaults()]
        hdf_items = {k: config.CONFIG.get(config_key, k) for k in hdf_keys}
        for k in numeric:
            hdf_items[k] = int(hdf_items[k])
        for k in boolean:
            hdf_items[k] = hdf_items[k].lower() in ("true", "yes")

        return hdf_items

    @filestore_url("url")
    def open_hdf_url(self, url, **kwargs):
        try:
            with fsspec.open(url, **self.fsspec_args) as fp:
                return h5py.File(fp.fs.open(url), mode='r')
        except (ClientError, FileNotFoundError, PermissionError) as e:
            raise ValueError(f"Unable to access url: {url}") from e

    def url_to_filestore_path(self, url, do_raise=True, check_exists=False, **kwargs):
        u = urlparse(url)
        if u.path == "" and do_raise:
            raise ValueError(f"Invalid URL: {url}")
        if self.filestore_path and u.scheme in ("", "file"):
            relpath = u.path.lstrip("/")
            relurl = os.path.join(self.filestore_path, relpath)
        elif u.scheme == "path":
            relurl = u.path
        else:
            relurl = url

        return relurl

    def file_exists_at_url(self, url, **kwargs):
        try:
            url = self.url_to_filestore_path(url)
            with fsspec.open(url, **self.fsspec_args) as fp:
                return fp.fs.exists(url)
        except (ValueError, FileNotFoundError, PermissionError):
            return False

    @filestore_url("url")
    def get_group_info(self, url, groupname):
        """
          Returns a dict containing two keys:
            - index: a dict of 'name', 'length', 'dtype' for
                     the index of the <group> arg
            - series: an array of dicts, each containing 'name',
                      'length', 'dtype' for each column of the
                      <group> arg
        """
        reader = self.make_group_reader(url, groupname)
        index_info = reader.get_index_info()
        columns = reader.get_columns_of_group()
        series_info = [reader.get_series_info(c) for c in columns]

        return {
          "index": index_info,
          "series": series_info
        }

    @filestore_url("url")
    def get_index_info(self, url, groupname):
        reader = self.make_group_reader(url, groupname)
        return reader.get_index_info()

    @filestore_url("url")
    def get_index_range(self, url, groupname, start=0, end=None):
        reader = self.make_group_reader(url, groupname)
        return reader.get_index_range(start, end)

    @filestore_url("url")
    def get_series_info(self, url, groupname=None, columns=None):
        reader = self.make_group_reader(url, groupname)
        if isinstance(columns, str):
            """
              Assume any str arg represents a single column
              name which should have been passed as a single-
              element Sequence, *unless that string is empty*
              in which case it is equivalent to an empty
              container and represents *all* columns
            """
            columns = (columns,) if len(columns) > 0 else None
        if not columns:
            columns = reader.get_columns_of_group()
        return [reader.get_series_info(c) for c in columns]

    @filestore_url("url")
    def file_size(self, url, **kwargs):
        with fsspec.open(url, **self.fsspec_args) as fp:
            size_bytes = fp.fs.size(fp.path)
        return size_bytes

    @filestore_url("url")
    def get_hdf_groups(self, url, **kwargs):
        h5f = self.open_hdf_url(url)
        return [*h5f.keys()]

    @filestore_url("url")
    def get_group_columns(self, url, groupname):
        reader = self.make_group_reader(url, groupname)
        return reader.get_columns_of_group()

    @filestore_url("url")
    def get_group_index(self, url, groupname):
        reader = self.make_group_reader(url, groupname)
        return reader.get_index_range(start=0, end=None)

    @filestore_url("url")
    def get_group_shape(self, url, groupname):
        reader = self.make_group_reader(url, groupname)
        return reader.get_group_shape()

    def get_columns_as_dataframe(self, url, groupname=None, columns=None, start=None, end=None, **kwargs):
        json_opts = {"date_format": "iso"}

        reader = self.make_group_reader(url, groupname)
        if isinstance(columns, str):
            """
              Assume any str arg represents a single column
              name which should have been passed as a single-
              element Sequence, *unless that string is empty*
              in which case it is equivalent to an empty
              container and represents *all* columns
            """
            columns = (columns,) if len(columns) > 0 else None
        if not columns:
            columns = reader.get_columns_of_group()

        df = reader.get_columns_as_dataframe(columns, start=start, end=end)

        return df.to_json(**json_opts)

    def equivalent_local_path(self, url, **kwargs):
        u = urlparse(url)
        filesrc = f"{u.netloc}{u.path}"
        if u.scheme == "path":
            return os.path.dirname(filesrc), os.path.basename(filesrc)
        filedir = os.path.dirname(filesrc)

        if self.filestore_path is not None:
            destdir = os.path.join(self.filestore_path, filedir.strip('/'))
        else:
            destdir = filedir
        return destdir, os.path.basename(filesrc)

    def retrieve_s3_file(self, url, **kwargs):
        u = urlparse(url)
        if not u.scheme == "s3":
            return
        filesrc = f"{u.netloc}{u.path}"
        destdir, filename = self.equivalent_local_path(url)

        if not os.path.exists(destdir):
            try:
                os.makedirs(destdir)
            except OSError as err:
                raise OSError(f"Unable to create local path at {destdir}: {err}")

        destfile = os.path.join(destdir, filename)
        fs = s3fs.S3FileSystem(anon=self._get_anon())
        log.info(f"Retrieving {url} to {destfile} ...")
        fs.get(filesrc, destfile)
        file_sz = os.stat(destfile).st_size
        log.info(f"Retrieved {destfile} ({file_sz} bytes)")
        return destfile, file_sz

    def list_local_files(self):
        import glob
        files = {}
        pattern = os.path.join(self.filestore_path, "**")
        for p in glob.iglob(pattern, recursive=True):
            if not os.path.isfile(p):
                continue
            files[p] = os.stat(p).st_size
        return files

    def purge_local_file(self, filename):
        """
          This prevents directory traversal by:
            - relative path components
            - ~user path components
            - $ENV_VAR components
            - paths containing hard or symbolic links

          A valid target file must be all of:
            - a real absolute filesystem path
            - a subtree of the filestore
            - not a directory
            - not a link
            - not a device file or pipe
            - owned by the Hydra user

          In addition, the filestore_path may not be:
            - undefined
            - the root filesystem
            - the root of any mount point

          ValueError is raised if any of these conditions
          are not met.
        """
        real_fsp = os.path.realpath(self.filestore_path)
        if not self.filestore_path or real_fsp == '/' or os.path.ismount(real_fsp):
            raise ValueError(f"Invalid filestore configuration value '{self.filestore_path}'")

        expanded = os.path.expandvars(filename)
        if expanded != filename:
            raise ValueError(f"Invalid path '{filename}': Arguments may not contain variables")
        target = os.path.realpath(expanded)
        if os.path.commonprefix([target, self.filestore_path]) != self.filestore_path:
            raise ValueError(f"Invalid path '{filename}': Only filestore files may be purged")

        if not os.path.exists(target):
            raise ValueError(f"Invalid path '{filename}': File does not exist")

        # Tests for directories, device files and pipes, and existence again
        if not os.path.isfile(target):
            raise ValueError(f"Invalid path '{filename}': Only regular files may be purged")

        if os.getuid() != os.stat(target).st_uid:
            raise ValueError(f"Invalid path '{filename}': File is not owned by "
                             f"user {os.getlogin()} ({os.getuid()})")
        try:
            os.unlink(target)
        except OSError as oe:
            raise ValueError(f"Invalid path '{filename}': Unable to purge file") from oe

        return target

    def make_group_reader(self, url, groupname):
        """
        Returns an instance of the appropriate GroupReader subclass for the <groupname>
        argument in the file at <url>.
        The required type is first looked up by get_group_reader() and an instance of
        this is returned.
        """
        hf = self.open_hdf_url(url)
        if not groupname:
            # Use first group in file
            try:
                groupname = [*hf.keys()][0]
            except IndexError as ie:
                raise ValueError(f"Data source {url} contains no groups") from ie
        Reader = self.get_group_reader(url, groupname)
        return Reader(hf, groupname)

    def get_group_reader(self, url, groupname):
        """
        Returns the type of the appropriate GroupReader subclass for the <groupname>
        argument in the file at <url>.
        The internal format of the group used by Pandas is first identified by
        identify_group_format(), and a reader capable of handling this type is then
        looked up in the group_reader_map.
        """
        group_type = self.identify_group_format(url, groupname)
        try:
            Reader = group_reader_map[group_type]
        except KeyError:
            # Not-None group_type was returned, but we don't have a reader for it
            raise ValueError(f"No reader available for group of type {group_type}")

        return Reader

    def identify_group_format(self, url, groupname):
        """
        Returns a string identifying the Pandas format of the group <groupname> in
        the file at <url>.
        This string, stored as the `pandas_type` attribute on the HDF group, indicates
        the layout of datasets and indices for the group and therefore the type of
        GroupReader required.
        """
        hf = self.open_hdf_url(url)
        try:
            group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"File at {url} contains no group {groupname}") from ke

        try:
            pandas_type = group.attrs["pandas_type"]
        except KeyError as ke:
            raise ValueError(f"File at {url} has invalid format") from ke

        return pandas_type.decode()


"""
  If config defines [hdf_storage]::disable_hdf as "True",
  the above HdfStorageAdapter is replaced with a Null object.
"""
if HdfStorageAdapter.get_hdf_config().get("disable_hdf"):
    HdfStorageAdapter = NullAdapter
