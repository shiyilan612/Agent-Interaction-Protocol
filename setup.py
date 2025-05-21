from setuptools import setup, find_packages

setup(
    name="atlink",
    version="0.1.0",
    packages=find_packages(),
    package_data={"atlink": ["*.yaml"]},
    install_requires=["grpcio", "grpcio-tools", "tinydb", "pyyaml"],
    author="kxm, lcl, whx@.ia.ac.cn",
    description="Distributed Interconnected Agent Communication Protocol",
    python_requires=">=3.10"
)