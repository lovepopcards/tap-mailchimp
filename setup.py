#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from os.path import basename, dirname, join, splitext
from glob import glob
from setuptools import setup, find_packages

def read(*names, encoding='utf8'):
    return open(join(dirname(__file__), *names), encoding=encoding).read()

setup(name='tap-mailchimp',
      version='0.2.1a',
      license='MIT',
      description='Singer.io tap for extracting data from the MailChimp API',
      long_description=read('README.rst'),
      author='Lovepop',
      author_email='hello@lovepopcards.com',
      url='https://github.com/lovepopcards/tap-mailchimp',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
      include_package_data=True,
      zip_safe=False,
      classifiers=[
          # complete classifier list:
          # http://pypi.python.org/pypi?%3Aaction=list_classifiers
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Environment :: Console',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python',
          'Topic :: Utilities'
      ],
      keywords=[],
      install_requires=['python-dateutil',
                        'requests',
                        'singer-python'],
      entry_points={'console_scripts': ['tap-mailchimp = tap_mailchimp:main']})
