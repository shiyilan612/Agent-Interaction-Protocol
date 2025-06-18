from setuptools import setup, find_packages

setup(
    name="atlink-aip",
    version="0.0.2",
    packages=find_packages(),
    install_requires=["grpcio", "grpcio-tools", "tinydb", "aiohttp", "aiohttp-sse-client", "colorlog"],
    author="Haixin Wang, Xiaomian Kang, Chunlin Leng",
    author_email="{haixin.wang & xiaomian.kang}@nlpr.ia.ac.cn, lengchunlin2023@ia.ac.cn",
    description="Distributed Interconnected Agent Communication Protocol",
    python_requires=">=3.10"
)