# coding:utf-8
"""ugly-sql 数据库工具"""
from setuptools import setup, find_packages

__author__ = 'Memory_Leak<irealing@163.com>'
with open('README.md', 'r', encoding='utf-8') as readme:
    doc = readme.read()
setup(
    name="ugly_sql",
    version="0.0.6",
    author=__author__,
    description=__doc__,
    long_description=doc,
    long_description_content_type='text/markdown',
    packages=find_packages(),
)
