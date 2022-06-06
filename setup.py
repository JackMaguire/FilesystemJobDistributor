#https://packaging.python.org/en/latest/tutorials/packaging-projects/

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="filesystemjd",
    version="0.0.1",
    author="Jack Maguire",
    author_email="maguire.jack@gene.com",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
