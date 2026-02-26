from setuptools import setup, find_packages

setup(
    name="common_kafka",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "msgspec>=0.18.0",
        "kafka-python>=2.0.0",
        "redis>=5.0.0",
    ],
)
