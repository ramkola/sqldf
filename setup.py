import os
import json
import glob
from setuptools import setup
from setuptools import find_packages

PACKAGE_NAME = 'sqldf'
MY_DIRECTORY = os.path.dirname(__file__)
PACKAGE_ROOT = os.path.join(MY_DIRECTORY, PACKAGE_NAME)

with open(os.path.join(MY_DIRECTORY, 'requirements.txt'), 'r') as f:
	dependencies = [
		entry.strip()
		for entry in f.read().strip().split('\n')
	]

with open(os.path.join(PACKAGE_ROOT, 'settings.json'), 'r') as f:
	settings = json.load(f)


def populate_extra_files():
	"""
	Creates a list of non-python data files to include in package distribution.
	"""

	globs = [
		'{}/**/*.json'.format(PACKAGE_ROOT),
		'{}/**/*.txt'.format(PACKAGE_ROOT),
		'{}/**/*.jinja2'.format(PACKAGE_ROOT)
		]

	return [
		item
		for glob_path in globs
		for item in glob.iglob(glob_path, recursive=True)
	]


setup(
	name=PACKAGE_NAME,
	version=settings['version'],
	description='Library for quering DataFrames using SQL',
	url='https://github',
	author='Rodrigo da Silva',
	author_email='rigogsilva@gmail.com',
	packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
	package_data={'': populate_extra_files()},
	include_package_data=True,
	zip_safe=False,
	classifiers=['Programming Language :: Python :: 3.6'],
	install_requires=dependencies,
	setup_requires=['pytest-runner'],
	tests_require=['pytest', 'pytest-cov', 'coverage']
)
