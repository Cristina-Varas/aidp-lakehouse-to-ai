import csv
from datetime import date
from pathlib import Path
import tempfile
import unittest

from aidp_lakehouse.csv_compat import compare_csv_terminators
from aidp_lakehouse.generator import Config, generate


class CsvCompatibilityTests(unittest.TestCase):
    def test_explicit_lf_in_every_generated_csv_and_reproducibility(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);config=Config(42,date(2026,10,28),100,10)
            generate(config,root/'a');generate(config,root/'b')
            names=[]
            for file in (root/'a').rglob('*.csv'):
                raw=file.read_bytes();names.append(file)
                self.assertNotIn(b'\r',raw);self.assertTrue(raw.endswith(b'\n'))
                self.assertEqual(raw,(root/'b'/file.relative_to(root/'a')).read_bytes())
                with file.open(newline='') as stream:rows=list(csv.reader(stream))
                self.assertEqual(raw.count(b'\n'),len(rows))
            self.assertEqual(len(names),6)

    def test_comparison_preserves_order_duplicates_and_field_whitespace(self):
        with tempfile.TemporaryDirectory() as tmp:
            a,b=Path(tmp)/'old.csv',Path(tmp)/'new.csv'
            rows=[['customer_name','row_version'],[' Example, Operations ','1 '],[' Example, Operations ','1 '],['Other','2']]
            for file,ending in [(a,'\r\n'),(b,'\n')]:
                with file.open('w',newline='') as stream:csv.writer(stream,lineterminator=ending).writerows(rows)
            result=compare_csv_terminators(a,b)
            self.assertEqual(result['data_rows'],3)
            self.assertTrue(result['field_whitespace_preserved'])
            for changed in [rows[:-1],list(reversed(rows)),[rows[0],['Example, Operations','1'],*rows[2:]]]:
                with b.open('w',newline='') as stream:csv.writer(stream,lineterminator='\n').writerows(changed)
                with self.assertRaises(ValueError):compare_csv_terminators(a,b)

    def test_quoted_field_crlf_cannot_be_silently_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            a,b=Path(tmp)/'old.csv',Path(tmp)/'new.csv'
            a.write_bytes(b'name,value\r\n"a\r\nb",1\r\n')
            b.write_bytes(a.read_bytes().replace(b'\r\n',b'\n'))
            with self.assertRaisesRegex(ValueError,'field whitespace differ'):compare_csv_terminators(a,b)


if __name__=='__main__':unittest.main()
