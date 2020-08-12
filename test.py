import os 
import csv
import time
import shutil

import unittest
import pyarrow as pa
import pyarrow.parquet as pq

from csv_to_parquet import is_valid_image, parse_product_catalog, write_datasets

HEADER = {
        "brand": pa.string(),
        "category_id": pa.string(),
        "comment": pa.string(),
        "currency": pa.string(),
        "description": pa.string(),
        "image": pa.string(),
        "year_release": pa.string(),
    }

TEST_IMG = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAHSSURBVDjLpZPbSxtREMbzL4pSpYj1VilUmmpQSCKCSLAgihFTo9Eg0TRZY5LdxJgbKYo+2AstLRbU1nfxrlmTdLPJ55nZ+OCjyYGPYQ8zv/PN7DkmAKZGZGoakN5OKSeqPfAPtk9Ca8ew+g4xvPIHQ94DWJZ/Y3DxJ94v/ID54zf0z32BbXFPpToGODZOKrPpUzxX5pmdCgPo5HrUN7kNBtjW/qKe1TORMwDW1SPeeJ0ucMzlcshms0gmkyhqVSQSCVzmdSiKAlmWEQ6HOa/TkakBxMBolUQyFRRIpQruijp/3xR0XN/ruMiXcXar4fRG4/yOsaQBoGmzpa08x0wmg1QqhVgsxoBoNMoQSZIQCATg9/s5r300YQDoV9HS9Cr+l6vspFRzQgBVOCE3j06uVJ3zX47EDIBl6RdvdG9ec6Se4/E4QqEQA4LBIO5FSz6fD16vFx6Ph/PabLIBMM9/541ypYpyzQWpqD2VKiB54YJEq2VowwC8m/tqTFU+50i9RyIR7pUK6WSKbrcbLpcLTqeT85ot6wagf3a/rnvQNCjh8S1Ib6Z3+Wb1fviMLkcWr8bTYsqbPKg2u4JWawQvhsPCdkicLEHUkCQGNPwaGwU8AG9RQVkc+5PeAAAAAElFTkSuQmCC"

class TestCsvToParquet(unittest.TestCase):
    def test_valid_image(self):

        invalid_strings = ["ThisIsNotAnImage", 25, "data:image/png;base64,kjDNkjcvkdjf"]

        for element in invalid_strings:
            self.assertEqual(is_valid_image(element), False)
        
        self.assertEqual(is_valid_image(TEST_IMG), True)
        

    def test_execution_time(self):
        
        os.mkdir('tests')

        TEST_DATA = (10**6)*[{"brand":"ASUS","category_id":None,"comment":None,"currency":"EUR","description" :"TEST","image": TEST_IMG,"year_release":2018}] 

        with open('tests/product_catalog.csv', 'w') as csvfile:
            fieldnames = list(HEADER.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for row in TEST_DATA:
                writer.writerow(row)


        start_time = time.time()

        metadata, datasets = parse_product_catalog(dataset='tests/product_catalog.csv', columns=HEADER)
        write_datasets(datasets, output_dir='tests', name='product_catalog')

        shutil.rmtree('tests')

        execution_time = time.time() - start_time

        self.assertTrue(execution_time <= 60)


if __name__ == "__main__":
    unittest.main()
