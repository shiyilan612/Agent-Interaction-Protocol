from setuptools import setup, find_packages

setup(
    name="atlink-aip",
    version="0.0.3",
    packages=find_packages(),
    install_requires=[
        "grpcio==1.73.1",
        "grpcio-tools==1.73.1",
        "tinydb==4.8.2",
        "aiohttp==3.12.14",
        "aiohttp-sse-client==0.2.1",
        "colorlog==6.9.0"
    ],
    author="Haixin Wang, Xiaomian Kang, Chunlin Leng",
    author_email="{haixin.wang & xiaomian.kang}@nlpr.ia.ac.cn, lengchunlin2023@ia.ac.cn",
    description="Distributed Interconnected Agent Communication Protocol",
    python_requires=">=3.10"
)