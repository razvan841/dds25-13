from setuptools import setup

setup(
    name="common_kafka",
    version="0.1.0",
    # Explicitly list the package because the source files live alongside setup.py
    packages=["common_kafka"],
    package_dir={"common_kafka": "."},
    install_requires=[
        "msgspec>=0.18.0",
        "kafka-python>=2.0.0",
        "redis>=5.0.0",
    ],
)
