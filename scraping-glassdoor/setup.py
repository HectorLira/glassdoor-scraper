from setuptools import find_packages, setup

setup(
    name="scraping_glassdoor",
    packages=find_packages(exclude=["scraping_glassdoor_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
