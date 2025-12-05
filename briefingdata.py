import yfinance as yf
import pandas as pd
import numpy as np
import logging
import time
from pathlib import Path


class DataRetriever:
    def __init__(self,file=None):
        self.file = file
        self.folder = Path('info')
        self.logger = logging.getLogger(__name__)
        
        if not self.folder.exists() or not self.folder.is_dir():
            self.folder.mkdir()
        
    def _get_info(self,symbol):
        info = None
        try:
            info = yf.Ticker(symbol).info
            self.logger.info(f' Fetching data for {symbol}')
        except Exception as e:
            self.logger.warning(f'  Could not load info for {symbol}\n')
        return info
        
    def _get_stocks(self):
        stocks = []
        try:
            with open(self.file,'r') as f:
                stocks = [i.strip() for i in f.readlines()]
                self.logger.info(f' {len(stocks)} stocks loaded.')
        except FileNotFoundError:
            self.logger.error(f'    {self.file} not found\n')
        return stocks
    
    def query(self):
        if self.file is None:
            self.logger.warning('   Watchlist file required.')
            return pd.DataFrame()
        
        try:
            path = Path(f'{self.folder}/{Path(self.file).stem}_info.csv')
        except Exception as e:
            self.logger.error(f'    Invalid File: {e}')
            return pd.DataFrame()
            
        if path.exists():
            self.logger.warning('   Filtered file already exists.')
            return pd.read_csv(path,index_col=0)
            
        metrics = [
            '52WeekChange',
            'beta',
            'priceToSalesTrailing12Months',
            'trailingPE',
            'forwardPE',
            'returnOnEquity',
            'debtToEquity',
            'ebitdaMargins',
            'dividendYield'
        ]
        
        df = pd.DataFrame(columns=metrics)
        
        stocks = self._get_stocks()
        
        for stock in stocks:
            time.sleep(0.5)
            data = self._get_info(stock)
            
            for i in metrics:
                try:
                    if i == 'debtToEquity':
                        df.loc[stock,i] = round(data[i]/100,2)
                    else:
                        df.loc[stock,i] = round(data[i],2)
                except KeyError:
                    df.loc[stock,i] = np.NaN
        
        df.sort_index(inplace=True)
        df.sort_values(by='priceToSalesTrailing12Months',inplace=True)
        df.to_csv(path)
        
        return df
