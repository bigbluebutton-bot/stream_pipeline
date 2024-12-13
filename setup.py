from setuptools import setup, find_packages

# Read the requirements from the requirements.txt file
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='stream_pipeline',
    version='0.1',
    packages=find_packages(),
    package_data={'stream_pipeline': ['py.typed']},
    description='Scalable microservice pipeline for data stream processing',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/bigbluebutton-bot/stream_pipeline',
    author='Julian Kropp',
    author_email='62835694+JulianKropp@users.noreply.github.com>',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=requirements,
)
