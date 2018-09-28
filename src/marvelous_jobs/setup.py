from setuptools import setup, find_packages

setup(
    name='marvelous_jobs',
    version='0.1.0',
    packages=find_packages(),

    entry_points={
        'console_scripts': [
            'marvelous_jobs = marvelous_jobs.__main__:main'
        ]
    },

    install_requires=[
        'pyslurm'
    ],

    author='Niklas Mähler',
    author_email='niklas.mahler@gmail.com',
    description='Job manager for the MARVEL assembler',
    url='https://github.com/bschiffthaler/spruce-v3/tree/master/src/marvelous_jobs'
)
