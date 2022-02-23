from setuptools import setup

setup(
    name = "vumonics_modules",
    version = "0.1",
    description= "Vumonics modules for Data Prepration",
    packages=['vumonics_modules'],
    package_dir = {'vumonics_modules':"vumonics_modules"},
    package_data= {'vumonics_modules':["./*.csv"]},
    include_package_data=True,
    zip_safe = False
)