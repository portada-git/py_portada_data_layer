from setuptools import setup

setup(name='py_portada_data_layer',
      version='0.0.4',
      description='Data layer for portada project for ETL process',
      author='PortADa team',
      author_email='jcbportada@gmail.com',
      license='MIT',
      url="https://github.com/portada-git/py_portada_data_layer",
      packages=['portada_data_layer'],
      py_modules=['delta_data_layer', 'data_lake_metadata_manager','traced_data_frame'],
      install_requires=[
        'pyspark==3.5.3',
        'delta-spark==3.2.1',
        'hdfs==2.7.0',
        'py4j',
      ],
      python_requires='>=3.12',
      zip_safe=False)
