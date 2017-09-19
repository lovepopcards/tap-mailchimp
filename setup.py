from setuptools import setup

setup(name='tap-mailchimp',
      version='0.1.a.dev',
      install_requires=['python-dateutil',
                        'singer-python',
                        'mailchimp3',
                        'requests'],
      entry_points={
          'console_scripts': ['tap-mailchimp = tap_mailchimp:main']
      })
