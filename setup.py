from setuptools import setup

setup(
    name='sqlbeam',
    version='1.0.0',
    description='Module for using different sql engines with apache beam',
    author='Luis Miguel Salazar E',
    author_email='luissalazar@tupperware.com',
    url='https://github.com/tupmike/sqlbeam.git',
    packages=['sqlbeam'],
    python_requires='>=3.10',
    install_requires=[
        'apache-beam[gcp]',
        'pymssql',
        'pyodbc',
        'oracledb'
    ],
)