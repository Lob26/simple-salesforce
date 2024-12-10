from setuptools import setup, find_packages

# Extract about
about = {}
with open("nsss/__version__.py") as f:
    exec(f.read(), about)

setup(
    name=about["__title__"],
    version=about["__version__"],
    description=about["__description__"],
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
    author=about["__author__"],
    author_email=about["__author_email__"],
    url="https://github.com/Lob26/not-so-simple-salesforce",
    packages=find_packages(exclude=["tests*", "examples*"]),
    include_package_data=True,
    python_requires=about["__python_version__"],
    install_requires=[
        "httpx[http2]",
        "more-itertools",
        "pydantic",
        "pyjwt[crypto]",
        "ruff",
        "zeep",
        # Optional if relevant
        "starlette",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-asyncio",
            "mypy",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP",
    ],
    project_urls={
        "Source": "https://github.com/Lob26/not-so-simple-salesforce",
        "Tracker": "https://github.com/Lob26/not-so-simple-salesforce/issues",
    },
    keywords=about["__keywords__"],
    license=about["__license__"],
    zip_safe=False,
)
