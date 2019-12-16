#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='practiceq',
      version='0.0.0',
      packages= find_packages(),
      install_requires=[
          'celery==4.1.0',
          'bagit==1.5.4',
          'requests==2.20.0',
          'PyYAML==3.12',
	  'pillow==6.2.0',
          'boto3',
      ],
)
