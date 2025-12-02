import pandas as pd
import yfinance as yf
import time
import logging
from pathlib import Path


class Datahandler:
    def __init__(self,stocks=[]):
        self.folder = Path('cache')
        self.logger = logging.getLogger(__name__)
        self.stocks = self._get_stocks(stocks)
        
        if not self.folder.exists() or not self.folder.is_dir():
            self.folder.mkdir()
        
    def _get_stocks(self,obj):
        if isinstance(obj,float):
            return []
        
        try:
            if Path(obj).exists():
                with open(obj,'r') as file:
                    stocks = [i.strip() for i in file.readlines() if len(i.strip()) <= 5]
                    return stocks
        except TypeError:
            pass
                
        if isinstance(obj,(str,int)):
            stocks = [obj]
            return stocks
        
        return obj
         
        
    def get_fundamentaldata(self,freq='quarterly'):
        df = pd.DataFrame()
        if self.stocks:
            if freq not in ['quarterly','yearly']:
                self.logger.error('Freq has to be in "quarterly/yearly"') 
                return 
                
            for stock in self.stocks:
                path = Path(f'{self.folder}/{stock}_{freq}-fundamentaldata.csv')
                if not path.exists():
                    try:
                        time.sleep(0.35)
                        ticker = yf.Ticker(stock.upper())
                        cashflow = ticker.get_cashflow(freq=freq)
                        income = ticker.get_incomestmt(freq=freq)
                        balance = ticker.get_balancesheet(freq=freq)
                        
                        df = pd.concat([cashflow,balance,income],axis=0,join='inner').T
                        
                        if freq == 'quarterly':
                            df.index = df.index.to_period('Q')
                        else:
                            df.index = df.index.year
                        
                        df = df.sort_index(ascending=True)
                        df = df.T
                        
                        if not df.empty:
                            df.to_csv(path)
                            
                    except Exception as e:
                        self.logger.warning(f'Error downloading {stock}: {e}')
                        continue
                else:
                    df = pd.read_csv(path,index_col=0,parse_dates=True)
        else:
            self.logger.warning('Stocklist is empty')
            
        return df
    
    def get_pricedata(self,interval='1d'):
        df = pd.DataFrame()
        if self.stocks:
            for stock in self.stocks:
                path = Path(f'{self.folder}/{stock}_{interval}-pricedata.csv')
                if not path.exists():
                    try:
                        time.sleep(0.35)
                        df = yf.download(stock.upper(),interval=interval,period='max',progress=False,auto_adjust=True)
                        df.columns = ['Open','High','Low','Close','Volume']
                        df.to_csv(path)
                    except Exception as e:
                        self.logger.warning(f'Error downloading {stock}: {e}')
                        continue
                else:
                    df = pd.read_csv(path,index_col=0,parse_dates=True)
        else:
            self.logger.warning('Stocklist is empty')
            
        return df
            
    def get_info(self):
        if self.stocks:
            if len(self.stocks) > 1:
                frame = pd.DataFrame(columns=['52wk_change','price','ps_ttm','pe_ttm','pe_fw'])
                
                for stock in self.stocks:
                    try:
                        time.sleep(0.35)
                        info = yf.Ticker(stock.upper()).info
                        name = info['longName']
                        frame.loc[name,'52wk_change'] = round(info['52WeekChange']*100,2)
                        frame.loc[name,'price'] = round(info['currentPrice'],2)
                        frame.loc[name,'ps_ttm'] = round(info['priceToSalesTrailing12Months'],2)
                        frame.loc[name,'pe_ttm'] = round(info['trailingPE'],2)
                        frame.loc[name,'pe_fw'] = round(info['forwardPE'],2)
                    except Exception as e:
                        self.logger.warning(f'{stock} does not exist')
                        continue
                        
                return frame.sort_values(by='ps_ttm')
                
            else:
                info = yf.Ticker(self.stocks[0]).info
                name = info['longName']
                
                data = {
                '52wk_change':round(info['52WeekChange']*100,2),
                'price':round(info['currentPrice'],2),
                'ps_ttm':round(info['priceToSalesTrailing12Months'],2),
                'pe_ttm':round(info['trailingPE'],2),
                'pe_fw':round(info['forwardPE'],2)
                }
                
                return pd.Series(data=data,name=name)
        else:
            self.logger.warning('Stocklist is empty')
            return pd.Series(dtype='float')
