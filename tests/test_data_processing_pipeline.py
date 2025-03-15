import pandas as pd
import unittest
from ..src.pipeline import TweetProcessor

df=pd.read_csv("data/train.csv")

df_sample=df.head()

Processor=TweetProcessor(df_sample)
df_clean=Processor.process()

class TestDataProcessingPipeline(unittest.TestCase):
    def test_output(self):
        self.assertIsInstance(df_clean, pd.DataFrame)
    def test_number_of_columns(self):
        self.assertEqual(len(df_clean.columns.tolist()), 5)
    def test_valid_dataframe(self):
        self.assertIsNotNone(df_clean)


# if __name__=="__main__":
#     unittest.main()