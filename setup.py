from setuptools import setup, find_packages

try:
    long_description = open("README.rst").read()
except IOError:
    long_description = ""

setup(
    name="dbtracker",
    version="1.0.0",
    description="Tracks ARC DBs",
    license="MIT",
    author="Bret Comnes",
    packages=find_packages(),
    install_requires=["psycopg2", "pymysql", "termcolor", "python-dateutil"],
    long_description=long_description,
    entry_points={
        'console_scripts': ['dbtracker=dbtracker:main'],
    }
)
