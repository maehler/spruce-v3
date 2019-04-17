from setuptools import setup, find_packages

setup(
    name='marvelous_jobs',
    version='0.17.3',
    packages=find_packages(),

    entry_points={
        'console_scripts': [
            'marvelous_jobs = marvelous_jobs.__main__:main'
        ]
    },

    install_requires=[
        'cython',
        'pyslurm'
    ],

    test_suite='nose.collector',
    tests_require=['nose'],

    author='Niklas Mähler',
    author_email='niklas.mahler@gmail.com',
    description='Job manager for the MARVEL assembler',
    url='https://github.com/maehler/spruce-v3/tree/master/src/marvelous_jobs'
)
