from typing import Mapping, Optional, Sequence, Union

from dagster._core.definitions.events import CoercibleToAssetKey
from dagster._core.definitions.metadata import (
    MetadataEntry,
    MetadataUserInput,
    PartitionMetadataEntry,
)
from dagster._core.definitions.partition import PartitionsDefinition
from dagster._core.definitions.resource_definition import ResourceDefinition
from dagster._core.definitions.source_asset import SourceAsset, SourceAssetObserveFunction
from dagster._core.storage.io_manager import IOManagerDefinition


def source_asset(
    key: CoercibleToAssetKey,
    *,
    metadata: Optional[MetadataUserInput] = None,
    io_manager_key: Optional[str] = None,
    io_manager_def: Optional[IOManagerDefinition] = None,
    description: Optional[str] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    _metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
    group_name: Optional[str] = None,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
) -> "_SourceAsset":
    return _SourceAsset(
        key,
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
        key: CoercibleToAssetKey,
        metadata: Optional[MetadataUserInput] = None,
        io_manager_key: Optional[str] = None,
        io_manager_def: Optional[IOManagerDefinition] = None,
        description: Optional[str] = None,
        partitions_def: Optional[PartitionsDefinition] = None,
        _metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
        group_name: Optional[str] = None,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
    ):
        self.key = key
        self.metadata = metadata
        self.io_manager_key = io_manager_key
        self.io_manager_def = io_manager_def
        self.description = description
        self.partitions_def = partitions_def
        self._metadata_entries = _metadata_entries
        self.group_name = group_name
        self.resource_defs = resource_defs

    def __call__(self, observe_fn: SourceAssetObserveFunction) -> SourceAsset:
        return SourceAsset(
            self.key,
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
