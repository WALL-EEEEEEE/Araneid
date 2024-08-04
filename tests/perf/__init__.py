"""
from unittest.loader import TestLoader
from unittest import TestLoader, TestSuite
from .araneid_performance_test import araneid_performance_test


def load_tests(loader: TestLoader, tests :TestSuite, pattern):
    test = loader.loadTestsFromTestCase(araneid_performance_test)
    tests.addTests(test)
    print(test)
    return tests 
"""