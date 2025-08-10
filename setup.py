"""Setup configuration for SQL Server Incremental Data Pipeline."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="dlt-sync",
    version="1.0.0",
    author="DLT Sync Pipeline",
    author_email="support@example.com",
    description="SQL Server Incremental Data Pipeline using DLT with support for partitioned, SCD Type 2, and static tables",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/dlt-sync",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "isort>=5.13.2",
            "mypy>=1.8.0",
        ],
        "cli": [
            "rich>=13.7.0",
            "click>=8.1.7",
        ],
    },
    entry_points={
        "console_scripts": [
            "dlt-sync=src.cli:main",
        ],
    },
)