import os

from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="easy-transfer",
    author="Henry Jones",
    author_email="henryivesjones@gmail.com",
    url="https://github.com/henryivesjones/easy-transfer",
    description="A tool for moving data between relational databases.",
    packages=["easy_transfer", "easy_transfer.connections"],
    package_dir={
        "easy_transfer": "easy_transfer",
        "easy_transfer.connections": "easy_transfer/connections",
    },
    package_data={
        "easy_transfer": ["py.typed"],
        "easy_transfer.connections": ["py.typed"],
    },
    include_package_data=True,
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
    ],
)
