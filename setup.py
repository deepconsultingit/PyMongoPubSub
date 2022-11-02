
from distutils.core import setup

setup(name='PyMongoPubSub',
    version='1.0',
    description='Pub-Sub library backed by MongoDB',
    author='Deep Consulting s.r.l',
    url='https://www.deepconsulting.it/',
    packages=['pymongopubsub'],
    install_requires=[
        'pymongo>=4.3.2',
    ],
)