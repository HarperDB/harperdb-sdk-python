import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="harperdb",
    version="0.3.0",
    author="Tom Young",
    author_email="shadetreemetalworks@gmail.com",
    maintainer="HarperDB",
    maintainer_email="hello@harperdb.io",
    description="Simple SDK for HarperDB.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/harperdb/harperdb-sdk-python",
    packages=setuptools.find_packages(),
    install_requires=['requests~=2.0'],
    tests_require=['responses~=0.10'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.4',
)
