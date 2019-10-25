"""Setup file for sqltools."""
from setuptools import find_packages, setup

try:
    LONG_DESC = open("README.md").read()
except FileNotFoundError:
    LONG_DESC = ""

setup(
    name="frbtools",
    version="0.0.1",
    description="Python library for convenient SQL functionality",
    url="",
    author="Paulo S. Costa",
    author_email="pcosta@firstrepublic.com",
    maintainer="Paulo S. Costa",
    maintainer_email="pcosta@firstrepublic.com",
    license="See LICENSE.txt",
    keywords="sql python jupyter",
    packages=find_packages(),
    install_requires=["pyodbc", "pandas", "jinja2"],
    long_description=LONG_DESC,
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: Other/Proprietary License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    include_package_data=False,
)
