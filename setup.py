from setuptools import setup, find_packages

setup(
    name="atlink-aip",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["grpcio", "grpcio-tools", "tinydb", "aiohttp", "colorlog"],
    author="Haixin Wang, Xiaomian Kang, Chunlinm Leng",
    author_email="{haixin.wang & xiaomian.kang}@nlpr.ia.ac.cn, lengchunlin2023@ia.ac.cn",
    description="Distributed Interconnected Agent Communication Protocol",
    python_requires=">=3.10"
)