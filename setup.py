from setuptools import find_packages, setup

setup(
    name='archivist',
    package_dir={'archivist': ''},
    packages=[
        'archivist',
        'archivist.classes',
        'archivist.messenger',
        'archivist.utils'],
    url='https://github.com/jeanpaulrsoucy/archivist',
    version='0.2.0',
    description='Python-based digital archive tool currently powering the Canadian COVID-19 Data Archive.',
    author='Jean-Paul R. Soucy',
    author_email="<jeanpaul.r.soucy@gmail.com>",
    license='MIT',
    install_requires=['boto3', 'bs4', 'color-it', 'humanfriendly', 'pandas', 'pytz', 'requests', 'selenium'],
)
