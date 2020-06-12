from setuptools import setup

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = [
    "pytest-spark"
]

setup(
    name="server_log_analysis",
    version='0.0.1',
    author="giorgecaique",
    author_email="giorgecaique@hotmail.com",
    description="Server log analysis with PySpark",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/giorgecaique/server_log_analysis",
    packages=[
        "eda", "etl", "etl.base", "utils", "tests"],
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)
