#!/usr/bin/env python

from distutils.core import setup

setup(name='spotify-unwrapped',
      version='0.0',
      description='Analytics for spotify streaming history',
      author='Conor Diamond',
      packages=['spotify_unwrapped'],
      install_requires=[
          'matplotlib>=3.7',
          'pandas>=2.0',
          'seaborn>=0.12',
          'plotly>=5.15',
          'scikit-learn>=1.2',
          'tqdm'
      ]
      )
