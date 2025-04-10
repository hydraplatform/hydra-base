import logging
import numpy as np
import pandas as pd

from abc import ABC, abstractmethod
from datetime import datetime

log = logging.getLogger(__name__)

group_reader_map = {}


class GroupReader(ABC):
    """
      ABC for all HDF Group Reader types.
      Each subclass must define a class attribute of <subclass_type_key>
      which contains a string identifying the Group type handled by that
      reader.
      These attributes then automatically populate the group_reader_map
      when a subclass is defined.  This map is then used by the adapter
      to instantiate the correct reader for the intercal format of a
      given group.
    """

    subclass_type_key = "pandas_type"

    def __init_subclass__(cls, /, **kwargs):
        super().__init_subclass__(**kwargs)
        keyattr = __class__.subclass_type_key
        reader_type = getattr(cls, keyattr, None)
        if not reader_type or not isinstance(reader_type, str):
            raise NotImplementedError(f"Reader subclass {cls.__name__} does not define a '{keyattr}' attribute")
        group_reader_map[reader_type] = cls
        log.debug(f"Registered HDF Group reader '{cls.__name__}' for type '{reader_type}'")

    @abstractmethod
    def get_group_shape(self):
        ...

    @abstractmethod
    def get_columns_of_group(self):
        ...

    @abstractmethod
    def get_index_info(self):
        ...

    @abstractmethod
    def get_series_info(self, series):
        ...

    @abstractmethod
    def get_index_range(self, start=0, end=None):
        ...

    @abstractmethod
    def get_columns_as_dataframe(self, columns, start=0, end=None):
        ...


class FrameGroupReader(GroupReader):
    """
    Reader for groups where the `pandas_type` attribute of the HDF group is "frame".
    This corresponds to a group where the application-level DataFrame is decomposed
    into `axis` and `block` components which are described by a collection of
    attributes and datasets inside the HDF group.
    The base class abstract methods are overridden in this class to provide
    implementations which deal specifically with groups of this format.
    """
    pandas_type = "frame"

    def __init__(self, hf, groupname):
        self.hf = hf
        try:
            self.group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"Error: file {hf.filename} contains no group {groupname}") from ke

    def get_columns_of_group(self):
        try:
            columns_raw = [*self.group["axis0"]]
        except KeyError as ke:
            raise ValueError(f"Error: {self.group.name} has invalid format") from ke
        return [col.decode() if isinstance(col, bytes) else col for col in columns_raw]

    def get_columns_by_block(self):
        try:
            nblocks = self.group.attrs["nblocks"]
        except KeyError as ke:
            raise KeyError(f"Error: group {self.group.name} contains no blocks") from ke
        blocks_columns = []
        for block_idx in range(nblocks):
            columns_raw = [*self.group[f"block{block_idx}_items"]]
            blocks_columns.append([col.decode() if isinstance(col, bytes) else col for col in columns_raw])
        return blocks_columns

    def make_column_map_of_group(self, blocks):
        group_cols = self.get_columns_of_group()
        return [column_to_block_coord(column, blocks) for column in group_cols]

    def get_series_by_column_names(self, column_names, start=None, end=None):
        columns = self.get_columns_of_group()
        block_columns = self.get_columns_by_block()
        if not column_names:
            # Return all columns when <column_names> arg
            # is either None or any empty container
            column_names = columns

        column_map = self.make_column_map_of_group(block_columns)
        named_map = {cname: cmap for cname, cmap in zip(columns, column_map)}

        row_sz, _ = self.get_group_shape()

        start = start or 0
        end = end or row_sz
        if start < 0 or start >= row_sz or start >= end or end < 0 or end >= row_sz+1:
            raise ValueError(f"Invalid bounds ({start=}, {end=} for series of length {row_sz}")

        column_series = {}
        for column_name in column_names:
            try:
                block_idx, col_idx = named_map[column_name]
            except KeyError as ke:
                raise ValueError(f"Group {self.group.name} contains no series '{column_name}'") from ke
            block_values_name = f"block{block_idx}_values"
            block_values = self.group[block_values_name]
            section = np.array([row[col_idx] for row in block_values[start:end]])
            column_series[column_name] = section

        return column_series

    def find_index_axis_index(self):
        # If index is a Pandas type, an "index_class" attr
        # will be present to indicate the index
        for ent in self.group:
            try:
                index_class = self.group[ent].attrs["index_class"]
                return ent
            except KeyError:
                continue

        # No item has a designated index, assume axis1 is index,
        # as per non-Datetime/Period Pandas index
        if "axis1" in self.group:
            return "axis1"

        # Otherwise, no suitable index can be found
        raise ValueError(f"Group {self.group.name} of pandas_type "
                          "'frame' contains no index axis")

    def get_index_range(self, start=None, end=None):
        index_axis = self.find_index_axis_index()
        index = self.group[index_axis]
        try:
            index_class = index.attrs["index_class"].decode()
        except KeyError:
            index_class = ""

        start = start or 0
        end = end or len(index)
        if start < 0 or start >= len(index) or start >= end or end < 0 or end >= len(index)+1:
            raise ValueError(f"Invalid bounds ({start=}, {end=} for index of length {len(index)}")

        if index_class.lower() == "datetime":
            return [str(nscale(row)) for row in index[start:end]]
        else:
            return index[start:end]

    def get_columns_as_dataframe(self, columns, start=None, end=None):
        index_range = self.get_index_range(start, end)
        index_range = [i.decode() if isinstance(i, bytes) else i for i in index_range]
        column_series = self.get_series_by_column_names(columns, start, end)
        return make_pandas_dataframe(index_range, column_series)

    def get_group_shape(self):
        index_axis = self.find_index_axis_index()
        row_sz = len(self.group[index_axis])
        col_sz = len(self.get_columns_of_group())
        return (row_sz, col_sz)

    def get_index_info(self):
        index_axis = self.find_index_axis_index()
        index = self.group[index_axis]
        try:
            dtype = index.attrs["kind"].decode()
        except KeyError:
            dtype = ""
        try:
            name = index.attrs["name"].decode()
        except KeyError:
            name = ""

        return {
            "name": name,
            "length": len(index),
            "dtype": dtype
        }

    def get_series_info(self, column_name):
        columns = self.get_columns_of_group()
        block_columns = self.get_columns_by_block()

        column_map = self.make_column_map_of_group(block_columns)
        named_map = {cname: cmap for cname, cmap in zip(columns, column_map)}

        block_idx, col_idx = named_map[column_name]
        block_values_name = f"block{block_idx}_values"
        block_values = self.group[block_values_name]

        return {
            "name": column_name,
            "length": block_values.shape[0],
            "dtype": str(block_values.dtype)
        }


class FrameTableGroupReader(GroupReader):
    """
    Reader for groups where the `pandas_type` attribute of the HDF group is "frame_table".
    This category of group stores the description of the layout of a DataFrame over
    attributes of the group itself and of a `table` dataset contained in the group.
    The "index_cols" attribute of a group stores the set of column names and the
    `FIELD_x` and `values_block_x` attributes of the dataset indicate in which of the
    dtype-specific columns of the dataset a particular series can be found.
    The base class abstract methods are overridden in this class to provide
    implementations which deal specifically with groups of this format.
    """
    pandas_type = "frame_table"

    def __init__(self, hf, groupname):
        self.hf = hf
        try:
            self.group = hf[groupname]
        except KeyError as ke:
            raise ValueError(f"Error: file {hf.filename} contains no group {groupname}") from ke
        try:
            self.table = self.group["table"]
        except KeyError as ke:
            raise ValueError(f"Error: file {hf.filename} has invalid format, no 'table'") from ke

    def get_index_column_index(self):
        max_index_depth = 128
        try:
            index_cols_raw = self.group.attrs["index_cols"].decode().split('\n')[2:-2:2]
        except KeyError:
            # No index columns present
            return None
        index_cols = trim_x_first_y_rest(1, 2, index_cols_raw)
        index_name = index_cols[0]  # Assume single index

        for field_idx in range(max_index_depth):
            try:
                field_name = self.table.attrs[f"FIELD_{field_idx}_NAME"]
            except KeyError:
                # No field has the name of the stated index column
                return None
            if field_name.decode() == index_name:
                return field_idx

    def get_values_start_block_index(self):
        max_value_depth = 128
        try:
            value_cols_raw = self.group.attrs["values_cols"].decode().split('\n')[1:-1:2]
        except KeyError:
            # Group contains no value columns
            return None
        value_cols = trim_x_first_y_rest(1, 2, value_cols_raw)
        first_value_col = value_cols[0]

        for field_idx in range(max_value_depth):
            try:
                field_name = self.table.attrs[f"FIELD_{field_idx}_NAME"]
            except KeyError:
                # No more fields
                return None
            if field_name.decode() == first_value_col:
                return field_idx

    def get_index_range(self, start=None, end=None):
        start = start or 0
        end = end or len(self.table)-1
        if start < 0 or start >= len(self.table) or start >= end or end < 0 or end >= len(self.table):
            raise ValueError(f"Invalid bounds ({start=}, {end=} for series of length {len(self.table)}")

        index_idx = self.get_index_column_index()
        try:
            index_name = self.table.attrs[f"FIELD_{index_idx}_NAME"].decode()
            index_kind = self.table.attrs[f"{index_name}_kind"].decode()
        except KeyError:
            return []

        if index_idx is None:
            return []
        if index_kind.lower().find("datetime") != -1:
            return [str(nscale(row[index_idx])) for row in self.table[start:end]]
        else:
            return [row[index_idx] for row in self.table[start:send]]

    def get_columns_by_block(self):
        value_cols_raw = self.group.attrs["values_cols"].decode().split('\n')[1:-1:2]
        value_cols = trim_x_first_y_rest(1, 2, value_cols_raw)

        block_columns = []
        for field_idx, field_name in enumerate(value_cols, start=1):
            kind = self.table.attrs[f"{field_name}_kind"]
            kind_cols_raw = kind.decode().split('\n')[1:-1:2]
            kind_cols = trim_x_first_y_rest(1, 2, kind_cols_raw)
            block_columns.append(kind_cols)

        return block_columns

    def get_series_by_column_names(self, column_names, start=None, end=None):
        columns = self.get_columns_of_group()
        block_columns = self.get_columns_by_block()

        column_map = self.make_column_map_of_group(block_columns)
        named_map = {cname: cmap for cname, cmap in zip(columns, column_map)}

        if not column_names:
            # Return all columns when <column_names> arg
            # is either None or any empty container
            column_names = columns

        first_value_block_idx = self.get_values_start_block_index()

        start = start or 0
        end = end or len(self.table)
        if start < 0 or start >= len(self.table) or start >= end or end < 0 or end >= len(self.table)+1:
            raise ValueError(f"Invalid bounds ({start=}, {end=} for series of length {len(self.table)}")

        column_series = {}
        for column_name in column_names:
            block_idx, col_idx = named_map[column_name]
            block_rows = np.array([row[block_idx+first_value_block_idx] for row in self.table[start:end]])
            section = block_rows[:, col_idx]
            column_series[column_name] = section

        return column_series

    def get_columns_as_dataframe(self, columns, start=None, end=None):
        index_range = self.get_index_range(start, end)
        column_series = self.get_series_by_column_names(columns, start, end)
        return make_pandas_dataframe(index_range, column_series)

    def make_column_map_of_group(self, blocks):
        non_index_cols = self.get_columns_of_group()
        return [column_to_block_coord(column, blocks) for column in non_index_cols]

    def get_columns_of_group(self):
        non_index_cols_raw = self.group.attrs["non_index_axes"].decode().split("\n")[3:-3:2]
        return trim_x_first_y_rest(1, 2, non_index_cols_raw)

    def get_group_shape(self):
        row_sz = len(self.table)
        col_sz = len(self.get_columns_of_group())
        return (row_sz, col_sz)

    def get_index_info(self):
        index_idx = self.get_index_column_index()
        index_name = self.table.attrs[f"FIELD_{index_idx}_NAME"].decode()
        index_kind = self.table.attrs[f"{index_name}_kind"].decode()
        index_rows = self.table.attrs["NROWS"]

        return {
            "name": index_name,
            "length": index_rows,
            "dtype": index_kind
        }

    def get_series_info(self, column_name):
        columns = self.get_columns_of_group()
        block_columns = self.get_columns_by_block()

        column_map = self.make_column_map_of_group(block_columns)
        named_map = {cname: cmap for cname, cmap in zip(columns, column_map)}

        block_idx, col_idx = named_map[column_name]
        dtype = self.table.attrs[f"values_block_{block_idx}_dtype"]

        return {
            "name": column_name,
            "length": len(self.table),
            "dtype": dtype.decode()
        }


"""
  Auxiliary Functions
"""

def nscale(ts):
    """
      Transforms integers representing nanoseconds past the epoch
      into instances of datetime.timestamp
    """
    return datetime.fromtimestamp(ts/1e9)

def trim_x_first_y_rest(x, y, names):
    return [names[0][x:], *[name[y:] for name in names[1:]] ]

def column_to_block_coord(column, blocks):
    for block_idx, block in enumerate(blocks):
        try:
            col_idx = block.index(column)
        except ValueError:
            continue
        return (block_idx, col_idx)
    raise ValueError(f"Column {column} not found in any block")

def make_pandas_dataframe(index, series):
    return pd.DataFrame(
        {name: values for name, values in series.items()},
        index=pd.Index(index)
    )
