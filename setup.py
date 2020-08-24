#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='derivative-recipe-generatorq',
      version='0.0.0',
      packages= find_packages(),
      install_requires=[
          'celery==4.1.0',
          'bagit==1.7.0',
          'requests==2.20.0',
          'pyyaml>=4.2b1',
	  'pillow==7.0.0',
          'boto3~=1.6',
	  'lxml',
	  'jinja2',
      ],
)
