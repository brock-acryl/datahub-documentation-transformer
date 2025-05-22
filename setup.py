from setuptools import setup, find_packages

setup(
    name="custom-datahub-transformers",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "acryl-datahub"
    ]
)