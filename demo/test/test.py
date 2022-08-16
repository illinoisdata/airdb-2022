import unittest
import airindex

class Tests(unittest.TestCase):

    def test_tune_data(self):
        param = {
            'func': ['linear', 'step', 'step'], 
            'delta': ['1024', '2048', '4096']
        }
        expected = {
            'func': ['linear', 'step', 'step'], 
            'delta': ['1024', '2048', '4096'],
            'data': ['336 B', '28.6 KB', '6.5 MB', '1.6 GB']
        }
        self.assertEqual(airindex.tune_data(param), expected)

if __name__ == '__main__':
    unittest.main()
