import os
import csv
import time
import base64
import argparse

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime
from PIL import Image, UnidentifiedImageError
from io import BytesIO

start_time = time.time()

def is_valid_csv(filename):
    """Naive file check - additionnal should be implemented here"""
    if not filename.endswith(".csv"):
        raise argparse.ArgumentTypeError("File does not seems to be a csv.")
    return filename

def is_valid_image(data, image_prefix="data:image/png;base64,"):
    """Try to open image to check if the string is a valid b64 image or not"""
    if type(data) != str :
        return False 

    if data.startswith(image_prefix):
        try:
            b64_data = data.replace(image_prefix, "")
            img = Image.open(BytesIO(base64.b64decode(b64_data)))

            return True
        except UnidentifiedImageError:
            return False
        except binascii.Error:
            return False

    return False

def parse_product_catalog(dataset, columns):
    """
        Returns a dictionary with two Pyarrow tables created by parsing the product catalog
        Also returns the associated metadata (products count)
    """

    # As we parse the product catalog, we will construct the valid and invalid datasets of products
    # Pyarrow needs the data to be stored in columns instead of rows, hence the following data structure :

    # products = {
    #     "valid" : {
    #         "brand" : [brand product 1, ... , product brand n],
    #         "category_id" : [category product 1, ... , category product n],
    #         ..
    #         "year_release": [year release product 1, ... , year release product n]
    #     },
    #     "invalid" : {
    #         ...
    #     }
    # }

    products = {
        product_type: {column: list() for column in columns.keys()}
        for product_type in ["valid", "error"]
    }

    count = {"valid": 0, "error": 0}

    with open(dataset, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        header = next(reader)

        if header != list(columns.keys()):
            raise ValueError(
                f"CSV file does not have a valid header. Should be {list(columns.keys())}"
            )

        for row in reader:
            # Considering we checked the CSV header we know that the image (if existing)
            # is in row n°5 therefor we do not have to handle any IndexError
            image = row[5]

            # Add the product to the right list
            product_type = "valid" if is_valid_image(image) else "error"
            count[product_type] += 1

            # We can know dispatch each product characteristics in the right column of the right dataframe
            for column, value in zip(header, row):
                products[product_type][column].append(value)


    # Convert all lists to the right data type
    schema = pa.schema(columns)

    dataframes = {
        product_type: pa.Table.from_pydict(
            dict(zip(schema.names, products[product_type].values())), schema=schema
        )
        for product_type in products.keys()
    }

    return count, dataframes

def write_datasets(data, output_dir='outputs', name='product_catalog'):
    today = datetime.now().strftime('%Y%m%d')

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # Write both files
    for product_type, table in data.items():
        filename = f"{output_dir}/{name}_{product_type}_{today}.parquet"

        # Overwrite file if necessary
        if os.path.exists(filename):
            os.remove(filename)

        pq.write_table(table, filename)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--file", help="file to process", type=is_valid_csv, required=True
    )
    args = parser.parse_args()

    columns = {
        "brand": pa.string(),
        "category_id": pa.string(),
        "comment": pa.string(),
        "currency": pa.string(),
        "description": pa.string(),
        "image": pa.string(),
        "year_release": pa.string(),
    }

    #We construct both dataframes 
    metadata, dataframes = parse_product_catalog(dataset=args.file, columns=columns)

    write_datasets(dataframes)

    print(
        f"""
        {sum(metadata.values())} lines processed
        {metadata['valid']} valid products
        {metadata['error']} errors
        Execution time : {time.time() - start_time}s
        """
    )
