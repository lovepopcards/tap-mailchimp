from setuptools import setup

setup(name='tap-mailchimp',
      version='0.1.a.dev',
      install_requires=['singer-python >= 2.1.4',
                        'mailchimp3 >= 2.0.15'
                        'requests >= 2.18.3'],
      entry_points={
          'console_scripts': ['tap-mailchimp = tap_mailchimp:main']
      })
