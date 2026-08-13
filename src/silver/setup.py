from setuptools import setup, find_packages

setup(
    name="silver-layer",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.4.0",
        "databricks-sdk>=0.18.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0"
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="Silver layer data processing for the data lake pipeline",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-organization/data-lake-pipeline",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)