from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="tesouro-data",
    version="0.2.1",
    author="Baseado no GetTDData de Marcelo Perlin",
    description="Biblioteca Python para baixar dados do Tesouro Direto brasileiro",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seu-usuario/download_treasure_data",
    packages=["tesouro_data"],
    package_dir={"tesouro_data": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Office/Business :: Financial",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    keywords="tesouro direto, brasil, finance, dados financeiros",
)
