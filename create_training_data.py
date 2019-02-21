import collections
import datetime
import json
import logging
import os
import zlib

from pathlib import Path
import sqlite3
from typing import Any, Dict, Iterable, Tuple

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

import ocr_text

class BundleAccessor:

    def __init__(self, path: Path):
        self.path = path

    def execute(self, query: str):
        for db_file in self.files:
            client = sqlite3.connect(str(db_file))
            yield from client.execute(query)
            client.close()

    @property
    def files(self):
        return sorted(self.path.glob('*.db'))


def _request_rows(client: BundleAccessor) -> Iterable[Tuple[dict, str]]:
    """
    Iterates over rows of training view in database. Instead of returning the request_id, it queries the
    database for the most recent monkey request that was performed based on this request. For every row,
    yields the most recent request_id, feedback, semantics and the XML tree of the OCR data.
    :param client: document-db client
    :return: Iterator over tuples containing request_id, feedback, semantics, ocr.
    """
    query = '''
        SELECT request_id, ocr_xml, feedback
        FROM request
        WHERE feedback IS NOT NULL
    '''
    for request_id, ocr_xml, feedback in client.execute(query):
        if feedback is None:
            continue
        ocr_xml = zlib.decompress(ocr_xml).decode('UTF-8', 'ignore')
        feedback = json.loads(feedback)
        yield {'request_id': request_id, 'feedback': feedback}, ocr_xml

def main():
	bundle_accessor = BundleAccessor(Path('../training_bundles'))

	X = []
	y = []

	for request_row, ocr in _request_rows(bundle_accessor):
		try:
			label = request_row['feedback']['_original']['country']['code3']
		except KeyError:
			continue

		text = ocr_text.convert(ocr)
		text = text.replace('\n','')
		text = text.replace('\t','')

		X.append(text)
		y.append(label)

	countries = list(set(y))

	df = pd.DataFrame()
	df['feature'] = X
	df['label'] = y

	train, test = train_test_split(df, test_size=0.2)

	train.to_csv('../train.tsv', sep='\t')
	test.to_csv('../test.tsv', sep='\t')

	with open('../countries.json', 'w') as outfile:
		json.dump(countries, outfile)

if __name__ == '__main__':
    main()