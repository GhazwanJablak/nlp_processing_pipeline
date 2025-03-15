import pandas as pd
import contractions
import re, string
from autocorrect import Speller
import spacy
from typing import List, Dict
spell = Speller(lang='en')
translator = str.maketrans('', '', string.punctuation)
lang=spacy.load("en_core_web_sm")

class TweetProcessor:
    """tweets processing class"""

    def __init__(
            self, 
            df  ):
        """Initialize tweet processor class
        """

        self.df=df
    
    @staticmethod
    def remove_punctuation(col) -> str:
        """Remove punctuation from string columns
        Retrun cleaned column"""
        clean_text = col.translate(translator)
        return clean_text
   
    @staticmethod
    def remove_prefixes(col):
        """Remove URL, hastag and retweet symbols from string columns
        Retrun cleaned column"""
        return " ".join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",col).split())
    
    @staticmethod
    def expand_contractions(col):
        """Expand acrynoms in string columns
        Retrun cleaned column"""
        return contractions.fix(col)
    
    @staticmethod
    def lemmatize_text(col):
        """Returns words to their root
        Retrun cleaned column"""
        text=lang(col)
        cleaned=[token.lemma_ for token in text]
        return " ".join(cleaned)
    
    @staticmethod
    def correct_spelling(col):
        """Correct for spelling mistakes in string column
        Return cleaned column"""
        return spell(col)
    
    @staticmethod
    def remove_stop_words(col):
        """Remove stop words e.g. I, am, are from string column
        Return cleaned column"""
        text=lang(col)
        stopwords=lang.Defaults.stop_words
        words=[token.text for token in text]
        cleaned_words=[w for w in words if w not in stopwords]
        return " ".join(cleaned_words)
    
    def process(self):
        cols=["text", "location", "keyword"]
        df2=self.df.copy(deep=True)
        for col in cols:
            df2[col]=df2[col].fillna(" ")
            df2[col]=df2[col].str.lower()
            df2[col]=df2[col].apply(TweetProcessor.remove_punctuation)
            df2[col]=df2[col].apply(TweetProcessor.remove_prefixes)
            df2[col]=df2[col].apply(lambda x: TweetProcessor.correct_spelling(x))
            df2[col]=df2[col].apply(TweetProcessor.expand_contractions)
            df2[col]=df2[col].apply(TweetProcessor.lemmatize_text)
            df2[col]=df2[col].apply(TweetProcessor.remove_stop_words)
        return df2
