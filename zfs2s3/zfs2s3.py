#!/usr/bin/env python3
from __future__ import annotations
import boto3  # type: ignore
import math
import subprocess
import argparse
from dataclasses import dataclass, field
import os
from io import BufferedIOBase, BytesIO
import sys
from typing import Optional
import yaml
import tqdm  # type: ignore
from botocore.awsrequest import AWSHTTPSConnection  # type: ignore

BUCKET='zfs2s3'
STORAGE_CLASS='DEEP_ARCHIVE'

progress_bar = None
def hijack_send_message_body(self: AWSHTTPSConnection, message_body):
    global progress_bar
    if message_body is not None:
        if progress_bar is not None:
            buffer_ = bytearray(16 * 1024)
            while True:
                length = message_body.readinto(buffer_)
                if length == 0:
                    break
                self.send(buffer_[:length])
                progress_bar.update(length)
            return
        self.send(message_body)

AWSHTTPSConnection._send_message_body = hijack_send_message_body

def get_snapshot_guid(snapshot_name: str) -> str:
    """Get the GUID for the ZFS snapshot."""
    try:
        cmd_args = ["zfs", "get", "-H", "-o", "value", "guid", snapshot_name]
        result = subprocess.run(
            cmd_args,
            stdout=subprocess.PIPE,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(cmd_args)}' failed: {e.stderr}", file=sys.stderr)
        sys.exit(1)


class ZFSSnapshot:
    def __init__(self, name: str):
        self.name: str = name
        self.guid: str = get_snapshot_guid(name)

    def verify_guid(self, guid: str) -> None:
        """Verify that the provided GUID matches the GUID of the snapshot, exit if not."""
        assert self.guid == guid, (f"GUID verification failed for snapshot {self.name}."
                "Expected {guid}, got {self.guid}")

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
        )
        stdout = self.process.stdout
        assert isinstance(stdout, BufferedIOBase)
        self.stdout: BufferedIOBase = stdout

    @staticmethod
    def create(config: ZFSSendStreamConfig) -> ZFSSendStream:
        cmd = ["zfs", "send"] + config.arguments()
        return ZFSSendStream(cmd)

    def seek(self, target: int) -> None:
        buffer_ = bytearray(1024**2)
        assert target >= self.offset, (
            f"seek() failed: target offset {target} was before current offset {self.offset}.")
        if self.offset < target:
            print(f"Seeking from {self.offset} to {target}...")
        while self.offset < target:
            drop_bytes = min(target - self.offset, len(buffer_))
            if drop_bytes < len(buffer_):
                buffer_ = bytearray(drop_bytes)
            result = self.stdout.readinto(buffer_)
            self.offset = self.offset + result
            assert result > 0 or self.offset == target, (
                f"seek() failed: 'zfs send' prematurely closed output stream at {self.offset}")
        del buffer_

    def abort(self) -> None:
        self.process.terminate()

    def expect_end(self) -> None:
        extra_data = self.stdout.read()
        assert not extra_data, (
            f"'zfs send' output extraneous data after the expected length: {extra_data!r}")
        return_code = self.process.wait()
        assert return_code == 0, f"zfs send exited with non-zero code {return_code}"

    def measure_size(self) -> int:
        print("Measuring length of 'zfs send'...")
        assert self.offset == 0
        buffer_ = bytearray(1024**2)
        length = 0
        while True:
            result = self.stdout.readinto(buffer_)
            length = length + result
            if result == 0:
                return length

    def read(self, length: int) -> bytes:
        result = b''
        while len(result) < length:
            data = self.stdout.read(length - len(result))
            result += data
            # it's okay if the stream is closed if we finished reading
            assert data or len(result) == length, ("Error while reading: stream closed prematurely"
                f"; expected to read {length}b at offset {self.offset} but only got {len(result)}")
        assert len(result) == length, ("Sanity violation error: "
            f"read length {len(result)} didn't match request {length}")
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
    xdg_data_home = os.environ.get("XDG_DATA_HOME", os.path.expanduser("~/.local/share"))
    dir_ = os.path.join(xdg_data_home, 'zfs2s3')
    if not os.path.exists(dir_):
        os.makedirs(dir_)
    return dir_

@dataclass
class SnapshotDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = u'!SnapshotDto'

    name: str
    guid: str

    @staticmethod
    def create(snapshot: ZFSSnapshot) -> SnapshotDto:
        return SnapshotDto(snapshot.name, snapshot.guid)

@dataclass
class S3UploadScheduleDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = u'!S3UploadScheduleDto'
    filename = 'upload_schedule.yml'

    backup: S3BackupDto
    layout: S3UploadScheduleLayoutDto

    def save(self) -> None:
        file_ = os.path.join(data_dir(), S3UploadScheduleDto.filename)
        assert not os.path.exists(file_), "There is already a pending upload schedule."
        progress_file = os.path.join(data_dir(), S3UploadProgressDto.filename)
        assert not os.path.exists(progress_file), "There is an abandoned progress file!"
        with open(file_, "w") as fd:
            yaml.dump(self, fd)

    @staticmethod
    def load() -> S3UploadScheduleDto:
        file_ = os.path.join(data_dir(), S3UploadScheduleDto.filename)
        with open(file_, "r") as fd:
            result = yaml.safe_load(fd)
            assert isinstance(result, S3UploadScheduleDto), (
                f"safe_load() failed: got instead {result}")
            return result

    @staticmethod
    def delete() -> None:
        file_ = os.path.join(data_dir(), S3UploadScheduleDto.filename)
        os.remove(file_)

@dataclass
class S3UploadScheduleLayoutDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = u'!S3UploadScheduleLayoutDto'

    size: int
    chunksize: int
    chunkcount: int

@dataclass
class S3BackupDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = u'!S3BackupDto'

    snapshot: SnapshotDto
    base: Optional[SnapshotDto]

@dataclass
class S3BackupStructureDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = u'!S3BackupStructureDto'
    filename = 'stored_snapshots.yml'

    backups: list[S3BackupDto]

@dataclass
class S3UploadProgressDto(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = u'!S3UploadProgressDto'
    filename = 'upload_progress.yml'

    chunks_done: int
    upload_id: str
    etags: list[str] = field(default_factory=list)

    def finish_chunk(self, etag: str) -> None:
        self.chunks_done = self.chunks_done + 1
        self.etags.append(etag)
        self.save()

    def save(self) -> None:
        file_tmp = os.path.join(data_dir(), f".{S3UploadProgressDto.filename}")
        file_ = os.path.join(data_dir(), S3UploadProgressDto.filename)
        with open(file_tmp, "w") as fd:
            yaml.dump(self, fd)
        os.rename(file_tmp, file_)

    def s3_parts(self):
        return [{'PartNumber': i + 1, 'ETag': etag} for i, etag in enumerate(self.etags)]

    @staticmethod
    def load() -> Optional[S3UploadProgressDto]:
        file_ = os.path.join(data_dir(), S3UploadProgressDto.filename)
        if not os.path.exists(file_):
            return None
        with open(file_, "r") as fd:
            result = yaml.safe_load(fd)
            assert isinstance(result, S3UploadProgressDto), (
                f"safe_load() failed: got instead {result}")
            return result

    @staticmethod
    def delete() -> None:
        file_ = os.path.join(data_dir(), S3UploadProgressDto.filename)
        os.remove(file_)

def s3_multipart_schedule_layout(size: int) -> S3UploadScheduleLayoutDto:
    # constraint constants
    MAX_CHUNKS = 10000
    MIN_CHUNK_SIZE = 75 * 1024 ** 2 # 75 MiB
    MAX_UPLOAD = 5 * 1024 ** 4 # 5 TiB

    assert size <= MAX_UPLOAD, "upload size exceeds S3 limit"

    # keep rounding up to ensure the final chunk is smaller than the rest
    initial_chunk_size = math.ceil(size / MAX_CHUNKS)
    chunk_size = max(MIN_CHUNK_SIZE, initial_chunk_size)
    chunk_count = math.ceil(size / chunk_size)

    return S3UploadScheduleLayoutDto(size=size, chunksize=chunk_size, chunkcount=chunk_count)

def main() -> None:
    parser = argparse.ArgumentParser(description='ZFS snapshot tool')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Subparser for the 'guid' command
    parser_guid = subparsers.add_parser('guid',
            help='Get the GUID of a ZFS snapshot')
    parser_guid.add_argument('snapshot_name',
            help='Name of the ZFS snapshot (e.g., "tank@snapshot")')

    # Subparser for the 'stream_size' command
    parser_stream_size = subparsers.add_parser('stream_size',
            help='Get the size of a ZFS send stream')
    parser_stream_size.add_argument('-i', '--increment', dest='prev_snapshot_name',
            help='Previous snapshot name for incremental send (optional)')
    parser_stream_size.add_argument('snapshot_name',
            help='Name of the ZFS snapshot to send')

    parser_upload_schedule = subparsers.add_parser('prepare_upload_schedule',
            help='Generate an upload schedule and save it as a YAML file')
    parser_upload_schedule.add_argument('-i', '--increment', dest='prev_snapshot_name',
            help='Previous snapshot name for incremental send (optional)')
    parser_upload_schedule.add_argument('snapshot_name',
            help='Name of the ZFS snapshot to send')

    parser_execute_upload = subparsers.add_parser('execute_upload',
            help='Begin or resume a multipart upload according to the prepared schedule')

    parser_abort_upload = subparsers.add_parser('abort_upload',
            help='Abort a running multi-part upload')

    args = parser.parse_args()

    if args.command == 'guid':
        snapshot = ZFSSnapshot(args.snapshot_name)
        print(f"GUID for snapshot '{args.snapshot_name}': {snapshot.guid}")
    elif args.command == 'stream_size':
        snapshot = ZFSSnapshot(args.snapshot_name)
        prev_snapshot = ZFSSnapshot(args.prev_snapshot_name) if args.prev_snapshot_name else None
        config = ZFSSendStreamConfig(snapshot=snapshot, base=prev_snapshot)
        stream = ZFSSendStream.create(config)
        stream_size = stream.measure_size()
        print(f"Stream size for snapshot '{args.snapshot_name}': {stream_size}")
    elif args.command == 'prepare_upload_schedule':
        snapshot = ZFSSnapshot(args.snapshot_name)
        prev_snapshot = ZFSSnapshot(args.prev_snapshot_name) if args.prev_snapshot_name else None
        config = ZFSSendStreamConfig(snapshot=snapshot, base=prev_snapshot)
        backup = S3BackupDto(
            snapshot=SnapshotDto.create(snapshot),
            base=SnapshotDto.create(prev_snapshot) if prev_snapshot else None,
        )
        stream = ZFSSendStream.create(config)
        layout = s3_multipart_schedule_layout(stream.measure_size())
        schedule = S3UploadScheduleDto(backup=backup, layout=layout)
        schedule.save()
        print("Upload schedule saved!")
    elif args.command == 'execute_upload':
        schedule = S3UploadScheduleDto.load()
        snapshot = ZFSSnapshot.create(schedule.backup.snapshot)
        base = ZFSSnapshot.create(schedule.backup.base) if schedule.backup.base else None
        config = ZFSSendStreamConfig(snapshot, base=base)
        s3 = boto3.client('s3')
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
            progress = S3UploadProgressDto(chunks_done=0, upload_id=result['UploadId'])
            progress.save()
        stream = ZFSSendStream.create(config)
        layout = schedule.layout
        stream.seek(min(layout.size, layout.chunksize * progress.chunks_done))
        for chunk_idx in range(progress.chunks_done, layout.chunkcount):
            # last chunk may be smaller
            length = min(layout.chunksize, layout.size - layout.chunksize * chunk_idx)
            data = stream.read(length)
            print(f"Uploading chunk {chunk_idx + 1} / {layout.chunkcount}...")
            hijacked_progress = 0
            global progress_bar
            progress_bar = tqdm.tqdm(total=length, unit='B', unit_scale=True, leave=False)
            result = s3.upload_part(
                Bucket=BUCKET,
                Key=config.key,
                PartNumber=chunk_idx + 1,
                UploadId=progress.upload_id,
                Body=data,
            )
            progress_bar = None
            progress.finish_chunk(result['ETag'])
        stream.expect_end()
        print("Finalizing...")
        s3.complete_multipart_upload(
            Bucket=BUCKET,
            Key=config.key,
            UploadId=progress.upload_id,
            MultipartUpload={'Parts': progress.s3_parts()},
        )
        print("Upload complete!")
        S3UploadScheduleDto.delete()
        S3UploadProgressDto.delete()
    elif args.command == 'abort_upload':
        progress = S3UploadProgressDto.load()
        if not progress:
            print("No upload in progress.")
            # This is *like* success. There is no upload.
            return

        schedule = S3UploadScheduleDto.load()
        snapshot = ZFSSnapshot.create(schedule.backup.snapshot)
        base = ZFSSnapshot.create(schedule.backup.base) if schedule.backup.base else None
        config = ZFSSendStreamConfig(snapshot=snapshot, base=base)

        s3 = boto3.client('s3')
        s3.abort_multipart_upload(
            Bucket=BUCKET,
            Key=config.key,
            UploadId=progress.upload_id
        )

        S3UploadScheduleDto.delete()
        S3UploadProgressDto.delete()
        print("Upload aborted.")

if __name__ == '__main__':
    main()
