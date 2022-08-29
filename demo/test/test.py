import unittest
import airindex

class Tests(unittest.TestCase):

    def test_tune_diy_custom(self):
        expected = [1717987000, ['Linear', 'Step', 'Step'], [1024, 2048, 4096], [336, 29286, 6815744], 12345678]
        self.assertEqual(airindex.tune_diy_custom("test_dataset", True, 500, 2, ['Linear', 'Step', 'Step'], [1024, 2048, 4096]), expected)

    def test_tune_diy_btree(self):
            expected = [1717987000, ['Step', 'Linear', 'Linear'], [4096, 2048, 1024], [236, 19286, 7815744], 1234]
            self.assertEqual(airindex.tune_diy_btree("test_dataset", True, 500, 2), expected)

    def test_tune_airindex(self):
        expected = [1717987000, ['Linear', 'Step', 'Step'], [1024, 2048, 4096], [436, 39286, 9815744], 123]
        self.assertEqual(airindex.tune_airindex("test_dataset", True, 500, 2), expected)

if __name__ == '__main__':
    unittest.main()
