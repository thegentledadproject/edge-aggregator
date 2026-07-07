from setuptools import setup
from setuptools.command.install import install


class InstallAndInitDB(install):
    """Standard install, then initialize dashboard_alpha.db so it exists before first run."""

    def run(self):
        install.run(self)
        from aggregator import CalibrationAndEdgeCore
        CalibrationAndEdgeCore()


setup(
    name="edge-aggregator",
    version="0.1.0",
    description="Freemium edge aggregator for Polymarket weather prediction markets",
    py_modules=["aggregator", "server", "weather_source", "dashboard"],
    install_requires=[
        "fastapi",
        "uvicorn",
        "requests",
    ],
    cmdclass={"install": InstallAndInitDB},
    python_requires=">=3.9",
)
