#!/usr/bin/env python3
"""
Setup script for E-commerce Analytics Pipeline.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="ecommerce-analytics-pipeline",
    version="0.1.0",
    author="E-commerce Analytics Team",
    author_email="analytics@company.com",
    description="E-commerce analytics pipeline for processing click stream data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/company/ecommerce-analytics-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "ecommerce-pipeline=main:main",
        ],
    },
)