import os
from setuptools import setup, find_packages


VERSION = '0.0.1'


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='mongo_observer',
    version=VERSION,
    packages=find_packages(exclude=['*test*', '*examples*']),
    url='https://github.com/diogommartins/mongo_observer',
    author='Diogo Magalhaes Martins',
    author_email='magalhaesmartins@icloud.com',
    keywords='collection mongodb oplog observer livecollection meteor',
    # Generated with: pandoc --from=markdown --to=rst --output=README.rst README.md
    long_description=read('README.rst')
)
