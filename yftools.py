import yfinance as yf
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import date
import time

class Data:
    def __init__(self,stock):
        self.logger = logging.getLogger(__name__)
        self.folder = Path('cache')
        self.stock = stock
        if not self.folder.exists() or not self.folder.is_dir():
            self.folder.mkdir()
    
    def ohlcv(self,interval='1wk'):
        path = Path(f'{self.folder}/{self.stock}_{interval}.csv')
        df = pd.DataFrame()
        if not path.exists():
            try:
                df = yf.download(self.stock,interval=interval,period='max',auto_adjust=True,progress=False)
                df.columns = ['Open','High','Low','Close','Volume']
                if not df.empty:
                    df.to_csv(path)
            except Exception as e:
                self.logger.warning(e)
        else:
            df = pd.read_csv(path,index_col=0,parse_dates=True)
        return df
        
    def fundamentals(self,freq='yearly'):
        path = Path(f'{self.folder}/{self.stock}_{freq}.csv')
        df = pd.DataFrame()
        if not path.exists():
            try:
                ticker = yf.Ticker(self.stock)
                income = ticker.get_incomestmt(freq=freq)
                balance = ticker.get_balancesheet(freq=freq)
                cashflow = ticker.get_cashflow(freq=freq)
                df = pd.concat([income,balance,cashflow],join='inner').T
                if freq == 'quarterly':
                    df.index = df.index.to_period('Q')
                else:
                    df.index = df.index.year
                df.sort_index(inplace=True)
                df = df.T
                if not df.empty:
                    df.to_csv(path)
            except Exception as e:
                self.logger.warning(e)
        else:
            df = pd.read_csv(path,index_col=0,parse_dates=True)
        return df
        

class Research:
    def __init__(self,symbols):
        self.logger = logging.getLogger(__name__)
        self.folder = Path('research')
        self.filename = None
        self.stocks = self._identify_symbols(symbols)
        if not self.folder.exists() or not self.folder.is_dir():
            self.folder.mkdir()
    
    def _identify_symbols(self,symbols):
        if isinstance(symbols,list):
            return symbols
        if isinstance(symbols,str):
            path = Path(symbols)
            if path.exists() and path.is_file():
                with open(path,'r') as f:
                    self.filename = path.stem
                    return [i.strip() for i in f.readlines()]
            return [symbols]           
        return []
        
    def research(self):
        if self.filename is not None:
            path = Path(f'{self.folder}/{self.filename}_research.csv')
        else:
            path = Path(f'{self.folder}/{date.today()}_research.csv')
        if path.exists():
            return pd.read_csv(path,index_col=0)
        df = pd.DataFrame(columns=[
        '52WeekChange',
        'forwardTargetDiff',
        'beta',
        'priceToSalesTrailing12Months',
        'trailingPE',
        'forwardPE',
        'returnOnEquity',
        'debtToEquity',
        'ebitdaMargins',
        'recommendation'
        ])
        if not self.stocks:
            return df
        for stock in self.stocks:
            try:
                time.sleep(np.random.uniform(1,3))
                ticker = yf.Ticker(stock)
                info = ticker.info
                for col in df.columns:
                    try:
                        if col == 'debtToEquity':
                            df.loc[stock,col] = np.round(info[col]/100,2)
                        elif col == 'forwardTargetDiff':
                            df.loc[stock,col] = np.round(
                                ticker.get_analyst_price_targets()['mean']/info['previousClose']-1,2)
                        elif col == 'recommendation':
                            rec = ticker.get_recommendations()
                            rec.set_index('period',inplace=True)
                            rec = rec.iloc[-1]
                            df.loc[stock,col] = rec.index[np.where(rec == rec.max())][0].strip().lower()
                        else:
                            df.loc[stock,col] = np.round(info[col],2)
                    except KeyError:
                        df.loc[stock,col] = np.NaN
            except Exception as e:
                self.logger.warning(e)
                continue
        df.to_csv(path)
        return df