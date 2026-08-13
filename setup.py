from setuptools import setup, find_packages

setup(
    name="ecommerce-analytics-pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "pyarrow>=5.0.0",
        "pytest>=6.0.0",
    ],
    entry_points={
        'console_scripts': [
            'ecommerce-pipeline=src.main:main',
        ],
    },
    python_requires='>=3.8',
)