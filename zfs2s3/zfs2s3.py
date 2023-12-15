#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import subprocess  # nosec
from dataclasses import dataclass, field
from io import BufferedIOBase
from typing import Optional

import boto3  # type: ignore
import tqdm  # type: ignore
import yaml
from botocore.awsrequest import AWSHTTPSConnection  # type: ignore

BUCKET = "zfs2s3"
STORAGE_CLASS = "STANDARD"

PROGRESS_BAR = None


def hijack_send_message_body(self: AWSHTTPSConnection, message_body):
    if message_body is not None:
        if PROGRESS_BAR is not None:
            buffer_ = bytearray(16 * 1024)
            while True:
                length = message_body.readinto(buffer_)
                if length == 0:
                    break
                self.send(buffer_[:length])
                PROGRESS_BAR.update(length)
            return
        self.send(message_body)


AWSHTTPSConnection._send_message_body = hijack_send_message_body


def get_snapshot_guid(snapshot_name: str) -> str:
    """Get the GUID for the ZFS snapshot."""
    cmd_args = ["zfs", "get", "-H", "-o", "value", "guid", snapshot_name]
    result = subprocess.run(
        cmd_args,
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )  # nosec
    return result.stdout.strip()


class ZFSSnapshot:
    def __init__(self, name: str):
        self.name: str = name
        self.guid: str = get_snapshot_guid(name)

    def verify_guid(self, guid: str) -> None:
        """Verify that the provided GUID matches the GUID of the snapshot, exit if not."""
        if self.guid != guid:
            raise AssertionError(
                f"GUID verification failed for snapshot {self.name}."
                "Expected {guid}, got {self.guid}"
            )

    @staticmethod
    def create(snapshot: SnapshotDto) -> ZFSSnapshot:
        result = ZFSSnapshot(snapshot.name)
        result.verify_guid(snapshot.guid)
        return result


class ZFSSendStream:
    def __init__(self, cmd: list[str]):
        self.offset: int = 0
        self.process: subprocess.Popen = subprocess.Popen(
            args=cmd,
            bufsize=1024**2,
            stdout=subprocess.PIPE,
        )  # nosec
        stdout = self.process.stdout
        assert isinstance(stdout, BufferedIOBase)  # nosec
        self.stdout: BufferedIOBase = stdout

    @staticmethod
    def create(config: ZFSSendStreamConfig) -> ZFSSendStream:
        cmd = ["zfs", "send"] + config.arguments()
        return ZFSSendStream(cmd)

    def seek(self, target: int) -> None:
        buffer_ = bytearray(1024**2)
        if target < self.offset:
            raise AssertionError(
                f"seek() failed: target offset {target} was before current offset {self.offset}."
            )
        if self.offset < target:
            print(f"Seeking from {self.offset} to {target}...")
        while self.offset < target:
            drop_bytes = min(target - self.offset, len(buffer_))
            if drop_bytes < len(buffer_):
                buffer_ = bytearray(drop_bytes)
            result = self.stdout.readinto(buffer_)
            self.offset = self.offset + result
            if result == 0 and self.offset != target:
                raise AssertionError(
                    f"seek() failed: 'zfs send' prematurely closed output stream at {self.offset}"
                )
        del buffer_

    def abort(self) -> None:
        self.process.terminate()

    def expect_end(self) -> None:
        extra_data = self.stdout.read()
        if extra_data:
            raise AssertionError(
                f"'zfs send' output extraneous data after the expected length: {extra_data!r}"
            )
        return_code = self.process.wait()
        if return_code != 0:
            raise AssertionError(f"zfs send exited with non-zero code {return_code}")

    def measure_size(self) -> int:
        print("Measuring length of 'zfs send'...")
        assert self.offset == 0  # nosec
        buffer_ = bytearray(1024**2)
        length = 0
        while True:
            result = self.stdout.readinto(buffer_)
            length = length + result
            if result == 0:
                return length

    def read(self, length: int) -> bytes:
        result = b""
        while len(result) < length:
            data = self.stdout.read(length - len(result))
            result += data
            # it's okay if the stream is closed if we finished reading
            if not data and len(result) != length:
                raise AssertionError(
                    "Error while reading: stream closed prematurely"
                    f"; expected to read {length}b at offset {self.offset}"
                    f" but only got {len(result)}"
                )
        if len(result) != length:
            raise AssertionError(
                f"Sanity violation error: read length {len(result)} didn't match request {length}"
            )
        self.offset = self.offset + length
        return result


class ZFSSendStreamConfig:
    def __init__(self, snapshot: ZFSSnapshot, base: Optional[ZFSSnapshot] = None):
        self.snapshot = snapshot
        self.base = base
        if base:
            key = f"{snapshot.name} <- {base.name}"
        else:
            key = f"{snapshot.name}"
        self.key: str = key

    def arguments(self) -> list[str]:
        # TODO use --replicate here?
        shared_args = ["--raw"]
        # If a base snapshot is provided, we're getting the size of an incremental stream
        if self.base:
            return shared_args + ["-i", self.base.name, self.snapshot.name]
        return shared_args + [self.snapshot.name]


def data_dir():
    xdg_data_home = os.environ.get(
        "XDG_DATA_HOME", os.path.expanduser("~/.local/share")
    )
    dir_ = os.path.join(xdg_data_home, "zfs2s3")
    if not os.path.exists(dir_):
        os.makedirs(dir_)
    return dir_


@dataclass
class SnapshotDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = "!SnapshotDto"

    name: str
    guid: str

    @staticmethod
    def create(snapshot: ZFSSnapshot) -> SnapshotDto:
        return SnapshotDto(snapshot.name, snapshot.guid)


@dataclass
class S3UploadScheduleDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = "!S3UploadScheduleDto"
    filename = "upload_schedule.yml"

    backup: S3BackupDto
    layout: S3UploadScheduleLayoutDto

    def save(self) -> None:
        file_ = os.path.join(data_dir(), self.filename)
        if os.path.exists(file_):
            raise AssertionError("There is already a pending upload schedule.")
        progress_file = os.path.join(data_dir(), self.filename)
        if os.path.exists(progress_file):
            raise AssertionError("There is an abandoned progress file!")
        with open(file_, "w", encoding="utf-8") as fd:
            yaml.dump(self, fd)

    @classmethod
    def load(cls) -> S3UploadScheduleDto:
        file_ = os.path.join(data_dir(), cls.filename)
        with open(file_, "r", encoding="utf-8") as fd:
            result = yaml.safe_load(fd)
            if not isinstance(result, cls):
                raise AssertionError(f"safe_load() failed: got instead {result}")
            return result

    @classmethod
    def delete(cls) -> None:
        file_ = os.path.join(data_dir(), cls.filename)
        os.remove(file_)


@dataclass
class S3UploadScheduleLayoutDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = "!S3UploadScheduleLayoutDto"

    size: int
    chunksize: int
    chunkcount: int


@dataclass
class S3BackupDto(yaml.YAMLObject):
    """A ZFS stream describing a snapshot that is stored on S3."""

    yaml_loader = yaml.SafeLoader
    yaml_tag = "!S3BackupDto"

    snapshot: SnapshotDto
    base: Optional[SnapshotDto]


@dataclass
class S3BackupStructureDto(yaml.YAMLObject):
    """All ZFS streams that have been stored on S3."""

    yaml_loader = yaml.SafeLoader
    yaml_tag = "!S3BackupStructureDto"
    filename = "stored_snapshots.yml"

    backups: list[S3BackupDto]

    def reachable(self, snapshot: ZFSSnapshot) -> bool:
        return any(backup.snapshot.guid == snapshot.guid for backup in self.backups)

    def add(self, added: S3BackupDto) -> S3BackupStructureDto:
        if any(a == added for a in self.backups):
            raise AssertionError("Tried to add already-known backup")
        return S3BackupStructureDto(self.backups + [added])

    def remove(self, removed: S3BackupDto) -> S3BackupStructureDto:
        if not any(a == removed for a in self.backups):
            raise AssertionError("Tried to remove unknown backup")
        backups = [backup for backup in self.backups if backup != removed]
        return S3BackupStructureDto(backups)

    def validate(self) -> None:
        """
        A backup structure is valid if for all backups that have a base,
        the base is also stored.
        Thus, we will be able to restore any backup.
        """
        known_guids = {backup.snapshot.guid: True for backup in self.backups}
        for backup in self.backups:
            if backup.base and backup.base.guid not in known_guids:
                raise AssertionError(f"Backup base is not reachable: {backup}")

    @classmethod
    def load(cls) -> S3BackupStructureDto:
        file_ = os.path.join(data_dir(), cls.filename)
        if not os.path.exists(file_):
            return cls(backups=[])
        with open(file_, "r", encoding="utf-8") as fd:
            result = yaml.safe_load(fd)
            if not isinstance(result, cls):
                raise AssertionError(f"safe_load() failed: got instead {result}")
            return result

    def save(self) -> None:
        self.validate()
        file_tmp = os.path.join(data_dir(), f".{self.filename}")
        file_ = os.path.join(data_dir(), self.filename)
        with open(file_tmp, "w", encoding="utf-8") as fd:
            yaml.dump(self, fd)
        os.rename(file_tmp, file_)


@dataclass
class S3UploadProgressDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = "!S3UploadProgressDto"
    filename = "upload_progress.yml"

    chunks_done: int
    upload_id: str
    etags: list[str] = field(default_factory=list)

    def finish_chunk(self, etag: str) -> None:
        self.chunks_done = self.chunks_done + 1
        self.etags.append(etag)
        self.save()

    def save(self) -> None:
        file_tmp = os.path.join(data_dir(), f".{self.filename}")
        file_ = os.path.join(data_dir(), self.filename)
        with open(file_tmp, "w", encoding="utf-8") as fd:
            yaml.dump(self, fd)
        os.rename(file_tmp, file_)

    def s3_parts(self):
        return [
            {"PartNumber": i + 1, "ETag": etag} for i, etag in enumerate(self.etags)
        ]

    @classmethod
    def load(cls) -> Optional[S3UploadProgressDto]:
        file_ = os.path.join(data_dir(), cls.filename)
        if not os.path.exists(file_):
            return None
        with open(file_, "r", encoding="utf-8") as fd:
            result = yaml.safe_load(fd)
            if not isinstance(result, cls):
                raise AssertionError(f"safe_load() failed: got instead {result}")
            return result

    @classmethod
    def delete(cls) -> None:
        file_ = os.path.join(data_dir(), cls.filename)
        os.remove(file_)


def s3_multipart_schedule_layout(size: int) -> S3UploadScheduleLayoutDto:
    # constraint constants
    max_chunks = 10000
    min_chunk_size = 75 * 1024**2  # 75 MiB
    max_upload = 5 * 1024**4  # 5 TiB

    if size > max_upload:
        raise AssertionError(f"upload size {size} exceeds S3 limit {max_upload}")

    # keep rounding up to ensure the final chunk is smaller than the rest
    initial_chunk_size = math.ceil(size / max_chunks)
    chunk_size = max(min_chunk_size, initial_chunk_size)
    chunk_count = math.ceil(size / chunk_size)

    return S3UploadScheduleLayoutDto(
        size=size, chunksize=chunk_size, chunkcount=chunk_count
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="ZFS snapshot tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_guid = subparsers.add_parser("guid", help="Get the GUID of a ZFS snapshot")
    parser_guid.add_argument(
        "snapshot_name", help='Name of the ZFS snapshot (e.g., "tank@snapshot")'
    )

    parser_stream_size = subparsers.add_parser(
        "stream_size", help="Get the size of a ZFS send stream"
    )
    parser_stream_size.add_argument(
        "-i",
        "--increment",
        dest="prev_snapshot_name",
        help="Previous snapshot name for incremental send (optional)",
    )
    parser_stream_size.add_argument(
        "snapshot_name", help="Name of the ZFS snapshot to send"
    )

    parser_upload_schedule = subparsers.add_parser(
        "prepare_upload_schedule",
        help="Generate an upload schedule and save it as a YAML file",
    )
    parser_upload_schedule.add_argument(
        "-i",
        "--increment",
        dest="prev_snapshot_name",
        help="Previous snapshot name for incremental send (optional)",
    )
    parser_upload_schedule.add_argument(
        "snapshot_name", help="Name of the ZFS snapshot to send"
    )

    subparsers.add_parser(
        "execute_upload",
        help="Begin or resume a multipart upload according to the prepared schedule",
    )

    subparsers.add_parser("abort_upload", help="Abort a running multi-part upload")

    parser_remove = subparsers.add_parser(
        "remove",
        help="Remove the S3 backup for a checkpoint",
    )
    parser_remove.add_argument(
        "-i",
        "--increment",
        dest="prev_snapshot_name",
        help="Previous snapshot name for incremental backup (optional)."
        " When the same checkpoint is backed up via multiple incremental paths,"
        " this selects the backup to remove.",
    )
    parser_remove.add_argument(
        "snapshot_name", help="Name of the ZFS snapshot to remove a backup for"
    )

    args = parser.parse_args()

    if args.command == "guid":
        snapshot = ZFSSnapshot(args.snapshot_name)
        print(f"GUID for snapshot '{args.snapshot_name}': {snapshot.guid}")
    elif args.command == "stream_size":
        snapshot = ZFSSnapshot(args.snapshot_name)
        prev_snapshot = (
            ZFSSnapshot(args.prev_snapshot_name) if args.prev_snapshot_name else None
        )
        config = ZFSSendStreamConfig(snapshot=snapshot, base=prev_snapshot)
        stream = ZFSSendStream.create(config)
        stream_size = stream.measure_size()
        print(f"Stream size for snapshot '{args.snapshot_name}': {stream_size}")
    elif args.command == "prepare_upload_schedule":
        snapshot = ZFSSnapshot(args.snapshot_name)
        prev_snapshot = (
            ZFSSnapshot(args.prev_snapshot_name) if args.prev_snapshot_name else None
        )
        backup = S3BackupDto(
            snapshot=SnapshotDto.create(snapshot),
            base=SnapshotDto.create(prev_snapshot) if prev_snapshot else None,
        )

        structure = S3BackupStructureDto.load()
        structure = structure.add(backup)
        structure.validate()

        config = ZFSSendStreamConfig(snapshot=snapshot, base=prev_snapshot)
        stream = ZFSSendStream.create(config)
        layout = s3_multipart_schedule_layout(stream.measure_size())
        schedule = S3UploadScheduleDto(backup=backup, layout=layout)
        schedule.save()
        print("Upload schedule saved!")
    elif args.command == "execute_upload":
        schedule = S3UploadScheduleDto.load()
        snapshot = ZFSSnapshot.create(schedule.backup.snapshot)
        base = (
            ZFSSnapshot.create(schedule.backup.base) if schedule.backup.base else None
        )

        structure = S3BackupStructureDto.load()
        structure = structure.add(schedule.backup)
        structure.validate()

        config = ZFSSendStreamConfig(snapshot, base=base)
        s3 = boto3.client("s3")
        progress = S3UploadProgressDto.load()
        if progress:
            print("Resuming multi-part upload...")
        else:
            print("Initiating multi-part upload...")
            result = s3.create_multipart_upload(
                Bucket=BUCKET,
                StorageClass=STORAGE_CLASS,
                Key=config.key,
            )
            progress = S3UploadProgressDto(chunks_done=0, upload_id=result["UploadId"])
            progress.save()
        stream = ZFSSendStream.create(config)
        layout = schedule.layout
        stream.seek(min(layout.size, layout.chunksize * progress.chunks_done))
        for chunk_idx in range(progress.chunks_done, layout.chunkcount):
            # last chunk may be smaller
            length = min(layout.chunksize, layout.size - layout.chunksize * chunk_idx)
            data = stream.read(length)
            print(f"Uploading chunk {chunk_idx + 1} / {layout.chunkcount}...")
            global PROGRESS_BAR
            PROGRESS_BAR = tqdm.tqdm(
                total=length, unit="B", unit_scale=True, leave=False
            )
            result = s3.upload_part(
                Bucket=BUCKET,
                Key=config.key,
                PartNumber=chunk_idx + 1,
                UploadId=progress.upload_id,
                Body=data,
            )
            PROGRESS_BAR = None
            progress.finish_chunk(result["ETag"])
        stream.expect_end()
        print("Finalizing...")
        s3.complete_multipart_upload(
            Bucket=BUCKET,
            Key=config.key,
            UploadId=progress.upload_id,
            MultipartUpload={"Parts": progress.s3_parts()},
        )
        print("Upload complete!")
        structure.save()
        S3UploadScheduleDto.delete()
        S3UploadProgressDto.delete()
    elif args.command == "abort_upload":
        progress = S3UploadProgressDto.load()
        if not progress:
            print("No upload in progress.")
            # This is *like* success. There is no upload.
            return

        schedule = S3UploadScheduleDto.load()
        snapshot = ZFSSnapshot.create(schedule.backup.snapshot)
        base = (
            ZFSSnapshot.create(schedule.backup.base) if schedule.backup.base else None
        )
        config = ZFSSendStreamConfig(snapshot=snapshot, base=base)

        s3 = boto3.client("s3")
        s3.abort_multipart_upload(
            Bucket=BUCKET, Key=config.key, UploadId=progress.upload_id
        )

        S3UploadScheduleDto.delete()
        S3UploadProgressDto.delete()
        print("Upload aborted.")
    elif args.command == "remove":
        snapshot = ZFSSnapshot(args.snapshot_name)
        prev_snapshot = (
            ZFSSnapshot(args.prev_snapshot_name) if args.prev_snapshot_name else None
        )
        backup = S3BackupDto(
            snapshot=SnapshotDto.create(snapshot),
            base=SnapshotDto.create(prev_snapshot) if prev_snapshot else None,
        )
        config = ZFSSendStreamConfig(snapshot=snapshot, base=prev_snapshot)

        structure = S3BackupStructureDto.load()
        structure = structure.remove(backup)
        structure.validate()

        s3 = boto3.client("s3")
        s3.delete_object(
            Bucket=BUCKET,
            Key=config.key,
        )
        structure.save()
        print(f"S3 key '{config.key}' removed.")


if __name__ == "__main__":
    main()
