# ZFS2S3

This tool allows you to incrementally back up your ZFS filesystem to Amazon S3.

It is designed for safety first and foremost. Incremental backups are risky because
if you have an incremental chain of 30 backups, and you delete one, all following backups
are unrecoverable. zfs2s3 tries to make this at least difficult.

Note that zfs2s3 is not a complete backup solution, it's just a tool to send zfs
snapshots to S3. You will need to script or organize your own snapshot schedule.

## Preparation

- Install the AWS tools, configure them with your keys.
- Create a bucket `zfs2s3` to which you have write permission,
  or set the environment variable `ZFS2S3_BUCKET`.
- If you want a storage class other than standard, also set `ZFS2S3_STORAGE_CLASS` to
  the desired S3 storage class, such as "STANDARD" or "DEEP_GLACIER".
- Install with `python3 setup.py install [--user]`.

## Commands

- `zfs2s3 upload dataset@snapshot [-i dataset@prev_snapshot]` uploads a dataset, optionally
  relative to another dataset.
  Note that once an upload has been started, it has to be completed or aborted
  before another upload can be performed.
- `zfs2s3 resume_upload` resumes a previous upload.
- `zfs2s3 abort_upload` aborts the currently running upload.
- `zfs2s3 remove dataset@snapshot [-i dataset_prev_snapshot]` removes the dataset previously
  uploaded with the given flags.
  Only use this command to remove saved datasets! If you manually delete S3 objects that
  this tool has uploaded, it can not ensure that all uploaded snapshots stay recoverable.

## Concept

When you complete an upload, the snapshot and base snapshot are saved in
`.local/share/zfs2s3/stored_snapshots.yml`. ZFS2S3 will not allow you to remove a snapshot
if doing so would abandon further downstream snapshots.

Another way to think about it is that `zfs2s3` will only allow you to render unavailable
exactly one backup per `zfs2s3 remove` call.

Example:

    A -> B -> C -> D

ZFS2S3 will allow you to delete D with `zfs2s3 remove dataset@D -i dataset@C`. This is valid,
because the only thing that is removed is D itself, which you explicitly asked for. However,
`zfs2s3 remove dataset@C -i dataset@B` will fail, because removing `B -> C` would abandon
`C -> D`. You have to remove `C -> D` first.

As another example, assume you have made daily backups for a month:

    Jan 1 -> Jan 2 -> ......... Jan 30 -> Jan 31 -> Feb 1 -> Feb 2...

You now want to replace the whole month with one backup, `Jan 1 -> Feb 1`, to save on space.
So you create it:

          /--------------------------------------->
    Jan 1 -> Jan 2 -> ......... Jan 30 -> Jan 31 -> Feb 1 -> Feb 2...

At this point, you will be able to remove `Jan 31 -> Feb 1` because doing so does not
leave `Feb 2` unsupported. Previously, doing so would have abandoned `Feb 2`, so you would
have gotten an error. So since we have the `Jan 1 -> Feb 1` "bypass", we can safely remove
the intermediate snapshots.
