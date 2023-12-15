from setuptools import find_packages, setup

setup(
    name="zfs2s3",
    version="0.1.0",
    author="@FeepingCreature",
    description="Back up ZFS send streams to Amazon S3",
    packages=find_packages(),
    entry_points={"console_scripts": ["zfs2s3=zfs2s3.zfs2s3:main"]},
    install_requires=[
        "boto3",
        "dataclasses",
        "pyyaml",
        "tqdm",
        "typing-extensions",
    ],
)
