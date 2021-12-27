from setuptools import find_packages, setup

setup(
    name='archivist',
    py_modules=['archivist'],
    packages=find_packages(),
    version='0.1.0',
    description='Python-based digital archive tool currently powering the Canadian COVID-19 Data Archive.',
    author='Jean-Paul R. Soucy',
    author_email="<jeanpaul.r.soucy@gmail.com>",
    license='MIT',
    install_requires=['boto3', 'bs4', 'color-it', 'pandas', 'pytz', 'requests', 'selenium'],
)
