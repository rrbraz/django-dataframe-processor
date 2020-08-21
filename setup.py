from os.path import join, dirname, abspath

from setuptools import find_packages, setup

here = abspath(dirname(__file__))
README = open(join(here, 'README.md')).read()


def load_requirements():
    return open(join(dirname(__file__), 'requirements.txt')).readlines()


setup(
    name='django-dataframe-processor',
    packages=find_packages(exclude=['tests']),
    version='0.1.2',
    description='A Django library for validating and process lines from a pandas DataFrame',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Rafael Rodrigues Braz',
    url='https://github.com/rrbraz/django-dataframe-processor/',
    license='MIT',
    install_requires=load_requirements(),
)
