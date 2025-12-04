from setuptools import setup

setup(
    name="custom-datahub-transformers",
    version="0.1.0",
    py_modules=["documentation_to_metadata_transformer"],  # Single file, not a package
    install_requires=[
        "acryl-datahub>=0.12.0"  # Pin a minimum version
    ],
    python_requires=">=3.8",
)