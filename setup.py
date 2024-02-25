from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
   name='lib-unit-tests-dq',
   version='1.0',
   package_dir={'lib_unit_tests_dq': 'data_quality/src'},
   author='Juliano Xavier',
   install_requires=required,
)