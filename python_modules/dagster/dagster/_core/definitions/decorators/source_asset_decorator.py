from typing import Mapping, Optional, Sequence, Union, overload

from dagster._core.definitions.events import AssetKey, CoercibleToAssetKeyPrefix
from dagster._core.definitions.metadata import (
    MetadataEntry,
    MetadataUserInput,
    PartitionMetadataEntry,
)
from dagster._core.definitions.partition import PartitionsDefinition
from dagster._core.definitions.resource_definition import ResourceDefinition
from dagster._core.definitions.source_asset import SourceAsset, SourceAssetObserveFunction
from dagster._core.storage.io_manager import IOManagerDefinition


@overload
def source_asset(observe_fn: SourceAssetObserveFunction) -> SourceAsset:
    ...

@overload
def source_asset(
    *,
    name: Optional[str] = ...,
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
    metadata: Optional[MetadataUserInput] = None,
    io_manager_key: Optional[str] = None,
    io_manager_def: Optional[IOManagerDefinition] = None,
    description: Optional[str] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    _metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
    group_name: Optional[str] = None,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
) -> "_SourceAsset":
    ...

def source_asset(
    observe_fn: Optional[SourceAssetObserveFunction] = None,
    *,
    name: Optional[str] = None,
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
    metadata: Optional[MetadataUserInput] = None,
    io_manager_key: Optional[str] = None,
    io_manager_def: Optional[IOManagerDefinition] = None,
    description: Optional[str] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    _metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
    group_name: Optional[str] = None,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
) -> Union[SourceAsset, "_SourceAsset"]:

    if observe_fn is not None:
        return _SourceAsset()(observe_fn)

    return _SourceAsset(
        name,
        key_prefix,
        metadata,
        io_manager_key,
        io_manager_def,
        description,
        partitions_def,
        _metadata_entries,
        group_name,
        resource_defs,
    )


class _SourceAsset:
    def __init__(
        self,
        name: Optional[str] = None,
        key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
        metadata: Optional[MetadataUserInput] = None,
        io_manager_key: Optional[str] = None,
        io_manager_def: Optional[IOManagerDefinition] = None,
        description: Optional[str] = None,
        partitions_def: Optional[PartitionsDefinition] = None,
        _metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
        group_name: Optional[str] = None,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
    ):
        self.name = name
        if isinstance(key_prefix, str):
            key_prefix = [key_prefix]
        elif key_prefix is None:
            key_prefix = []
        self.key_prefix = key_prefix
        self.metadata = metadata
        self.io_manager_key = io_manager_key
        self.io_manager_def = io_manager_def
        self.description = description
        self.partitions_def = partitions_def
        self._metadata_entries = _metadata_entries
        self.group_name = group_name
        self.resource_defs = resource_defs

    def __call__(self, observe_fn: SourceAssetObserveFunction) -> SourceAsset:
        source_asset_name = self.name or observe_fn.__name__
        source_asset_key = AssetKey([*self.key_prefix, source_asset_name])
        return SourceAsset(
            source_asset_key,
            self.metadata,
            self.io_manager_key,
            self.io_manager_def,
            self.description,
            self.partitions_def,
            self._metadata_entries,
            self.group_name,
            self.resource_defs,
            observe_fn,
        )
