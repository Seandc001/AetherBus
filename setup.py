from setuptools import setup, find_packages

setup(
    name="AetherBus",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "redis>=4.2.0",
        "python-dotenv",
    ],
    python_requires=">=3.8",
    description="AetherBus (formerly Agentic1 Redis Messaging Bus)",
    author="Your Name",
)
