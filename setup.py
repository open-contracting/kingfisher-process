from setuptools import setup, find_packages

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='ocdskingfisher',
    version='0.0.1',
    author='Open Contracting Partnership, Open Data Services, Iniciativa Latinoamericana por los Datos Abiertos',
    author_email='data@open-contracting.org',
    url='https://github.com/open-contracting/kingfisher-process',
    description="Store and pre-process data conforming to the Open Contracting Data Standard",
    license='BSD',
    packages=find_packages(),
    long_description=long_description,
    scripts=['ocdskingfisher-process-cli'],
    package_data={'ocdskingfisher': [
            'maindatabase/migrations/script.py.mako'
        ]},
    include_package_data=True,
    install_requires=[
        'alembic',
        'blinker',
        'Flask',
        'ocdskit',
        'ocdsmerge',
        'pgpasslib',
        'psycopg2',
        'redis',
        'sentry-sdk',
        'SQLAlchemy<1.3',  # 1.3 has issues with an identifier being too long
    ],
    extras_require={
        'test': [
            'coveralls',
            'flake8',
            'pytest',
            'pytest-cov',
            'Sphinx',
        ],
        'docs': [
            'Sphinx<2',
            'sphinx-autobuild',
            'sphinx_rtd_theme',
        ],
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6',
    ],
)
