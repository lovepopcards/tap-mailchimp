from setuptools import setup

setup(name='tap-mailchimp',
      version='0.1a1',
      description='Singer.io tap for extracting data from the MailChimp API',
      author='Lovepop',
      url='https://singer.io',
      install_requires=['python-dateutil',
                        'singer-python',
                        'mailchimp3',
                        'requests'],
      entry_points={
          'console_scripts': ['tap-mailchimp = tap_mailchimp:main']
      })
