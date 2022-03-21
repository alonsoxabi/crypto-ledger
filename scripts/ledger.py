import pandas as pd
import os
from binance.client import Client
from binance.helpers import date_to_milliseconds
from kucoin.client import Market
from datetime import datetime


def get_days_between_dates(date1: str, date2: str) -> float:
    dif_ms = abs(date_to_milliseconds(date1) - date_to_milliseconds(date2))
    return dif_ms / (1000 * 60 * 60 * 24)


class PriceFeed:
    FALLBACK_CURRENCY = {'binance': 'BUSD',
                         'kucoin': 'USDT'}

    def __init__(self):
        self.clients = {'binance': Client(),
                        'kucoin': Market(url='https://api.kucoin.com')}
        exchange_info_binance = self.clients['binance'].get_exchange_info()
        exchange_info_kucoin = self.clients['kucoin'].get_symbol_list()
        self.symbols = {'binance': [s['symbol'] for s in exchange_info_binance['symbols']],
                        'kucoin': [s['symbol'] for s in exchange_info_kucoin]}
        self.symbol_delimiter = {'binance': '',
                                 'kucoin': '-'}

    def get_change_factor(self, asset: str, currency: str, date: str = None, die_on_failure: bool = True,
                          force_provider: str = None) -> float:
        for provider in self.clients.keys():
            if not force_provider or force_provider == provider:
                symbol = None
                if currency + self.symbol_delimiter[provider] + asset in self.symbols[provider]:
                    symbol = currency + self.symbol_delimiter[provider] + asset
                elif asset + self.symbol_delimiter[provider] + currency in self.symbols[provider]:
                    symbol = asset + self.symbol_delimiter[provider] + currency
                elif currency != self.FALLBACK_CURRENCY[provider] and asset != self.FALLBACK_CURRENCY[provider]:
                    asset_to_fallback = self.get_change_factor(asset, currency=self.FALLBACK_CURRENCY[provider],
                                                               date=date, die_on_failure=False, force_provider=provider)
                    fallback_to_currency = self.get_change_factor(self.FALLBACK_CURRENCY[provider], currency=currency,
                                                                  date=date, die_on_failure=False,
                                                                  force_provider="binance")
                    if asset_to_fallback != -1 and fallback_to_currency != -1:
                        return asset_to_fallback * fallback_to_currency
                if symbol:
                    if date:
                        symbol_price = self.get_historical_symbol_price(provider, symbol, date)
                    else:
                        symbol_price = self.get_current_symbol_price(provider, symbol)
                    if symbol_price:
                        if symbol.startswith(currency):
                            return 1 / symbol_price
                        else:
                            return symbol_price
                    else:
                        raise ValueError("API call did not return any values.")
        if die_on_failure:
            raise ValueError("No symbol defined for combination of {} and {}".format(asset, currency))
        else:
            return float(-1)

    def get_historical_symbol_price(self, provider: str, symbol: str, date: str) -> float:
        date_ms = date_to_milliseconds(date)
        start_date_ms = date_ms - 60000
        if provider == "binance":
            kline = self.clients[provider].get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, start_date_ms,
                                                                 date_ms)
        elif provider == "kucoin":
            kline = self.clients[provider].get_kline(symbol, '1min', startAt=start_date_ms // 1000,
                                                     endAt=date_ms // 1000)
        else:
            raise KeyError("Unknown provider {}!".format(provider))
        if kline:
            return float(kline[0][1])
        else:
            raise ValueError(
                "API call to {} did not return any values for symbol {} and date {}.".format(provider, symbol, date))

    def get_current_symbol_price(self, provider: str, symbol: str) -> float:
        if provider == "binance":
            symbol_overview = self.clients[provider].get_symbol_ticker(symbol=symbol)
        elif provider == "kucoin":
            symbol_overview = self.clients[provider].get_ticker(symbol)
        else:
            raise KeyError("Unknown provider {}!".format(provider))
        if symbol_overview:
            return float(symbol_overview["price"])
        else:
            raise ValueError("API call to {} did not return any values for symbol {}.".format(provider, symbol))


class CryptoLedger:
    CURRENCY = "EUR"
    LEDGER_COLUMNS = ["Date(UTC)", "Asset Amount", "Total (" + CURRENCY + ")"]
    OPEN_POSITION_COLUMNS = ["Date(UTC)", "Asset Amount", CURRENCY + "/Unit"]
    CLOSED_POSITION_COLUMNS = ["Date(UTC) of purchase", "Date(UTC) of sell", "Holding time", "Asset Amount",
                               "Profit/Loss (" + CURRENCY + ")", "Purchase price (" + CURRENCY + ")",
                               "Sell price (" + CURRENCY + ")"]
    DATE_SYNONYMS = ["Pair", "Date(UTC)", "time", "Date(UTC+1)", "tradeCreatedAt"]
    PAIR_SYNONYMS = ["Pair", "Price", "pair", "symbol"]
    ORDER_TYPE_SYNONYMS = ["Type", "type", "side"]
    TOTAL_AMOUNT_SYNONYMS = ["Filled", "Final Amount", "vol", "size"]
    TOTAL_PRICE_SYNONYMS = ["Total", "Amount", "cost", "funds"]
    STATUS_SYNONYMS = ["status", "Status"]
    TAX_ALLOWANCE = 600

    def __init__(self, asset: str, price_feed: PriceFeed):
        self.ASSET_NAME = asset
        self.buy_history = pd.DataFrame(columns=self.LEDGER_COLUMNS)
        self.sell_history = pd.DataFrame(columns=self.LEDGER_COLUMNS)
        self.open_positions = pd.DataFrame(columns=self.OPEN_POSITION_COLUMNS)
        self.closed_positions = pd.DataFrame(columns=self.CLOSED_POSITION_COLUMNS)
        self.price_feed = price_feed

    def get_asset_name(self) -> str:
        return self.ASSET_NAME

    def calculate_position_from_history(self):
        self.buy_history = self.buy_history.sort_values(by=self.LEDGER_COLUMNS[0], ignore_index=True)
        self.sell_history = self.sell_history.sort_values(by=self.LEDGER_COLUMNS[0], ignore_index=True)
        self.open_positions = pd.DataFrame(columns=self.OPEN_POSITION_COLUMNS)
        self.closed_positions = pd.DataFrame(columns=self.CLOSED_POSITION_COLUMNS)
        for row in self.buy_history.index:
            self.open_positions = self.open_positions.append(pd.DataFrame.from_dict({
                self.OPEN_POSITION_COLUMNS[0]: [self.buy_history[self.LEDGER_COLUMNS[0]][row]],
                self.OPEN_POSITION_COLUMNS[1]: [self.buy_history[self.LEDGER_COLUMNS[1]][row]],
                self.OPEN_POSITION_COLUMNS[2]: [self.buy_history[self.LEDGER_COLUMNS[2]][row] /
                                                self.buy_history[self.LEDGER_COLUMNS[1]][row]]}), ignore_index=True)
        for row in self.sell_history.index:
            sell_amount = self.sell_history[self.LEDGER_COLUMNS[1]][row]
            open_index = self.open_positions.index
            aggregator_index = -1
            asset_amount = 0
            while asset_amount < sell_amount:
                aggregator_index = aggregator_index + 1
                asset_amount = \
                    asset_amount + self.open_positions[self.OPEN_POSITION_COLUMNS[1]][open_index[aggregator_index]]
            remainder = asset_amount - sell_amount
            date_of_purchase = self.open_positions[self.OPEN_POSITION_COLUMNS[0]][open_index[aggregator_index]]
            date_of_sell = self.sell_history[self.LEDGER_COLUMNS[0]][row]
            purchase_price = sum([
                self.open_positions[self.OPEN_POSITION_COLUMNS[1]][open_index[i]] *
                self.open_positions[self.OPEN_POSITION_COLUMNS[2]][open_index[i]]
                if i < aggregator_index else
                (self.open_positions[self.OPEN_POSITION_COLUMNS[1]][open_index[i]] - remainder) *
                self.open_positions[self.OPEN_POSITION_COLUMNS[2]][open_index[i]] for i in range(aggregator_index + 1)])
            sell_price = self.sell_history[self.LEDGER_COLUMNS[2]][row]
            if remainder > 0:
                self.open_positions.loc[open_index[aggregator_index], self.OPEN_POSITION_COLUMNS[1]] = remainder
                self.open_positions = self.open_positions.drop(open_index[:aggregator_index])
            else:
                self.open_positions = self.open_positions.drop(open_index[:aggregator_index + 1])
            self.closed_positions = self.closed_positions.append(pd.DataFrame.from_dict({
                self.CLOSED_POSITION_COLUMNS[0]: [date_of_purchase],
                self.CLOSED_POSITION_COLUMNS[1]: [date_of_sell],
                self.CLOSED_POSITION_COLUMNS[2]: [get_days_between_dates(date_of_purchase, date_of_sell)],
                self.CLOSED_POSITION_COLUMNS[3]: [sell_amount],
                self.CLOSED_POSITION_COLUMNS[4]: [sell_price - purchase_price],
                self.CLOSED_POSITION_COLUMNS[5]: [purchase_price],
                self.CLOSED_POSITION_COLUMNS[6]: [sell_price]}), ignore_index=True)

    def import_from_file(self, path: str):
        if path.endswith(".xlsx"):
            df = pd.read_excel(io=path)
        elif path.endswith(".csv"):
            df = pd.read_csv(path)
        else:
            raise KeyError("File format of {} not supported for import.".format(path))
        columns = list(df.columns)
        pair_col_name = [name for name in self.PAIR_SYNONYMS if name in columns][0]
        date_col_name = [name for name in self.DATE_SYNONYMS if name in columns][0]
        date_col_offset = [2 if pair_col_name == date_col_name else 0][0]
        amount_col_name = [name for name in self.TOTAL_AMOUNT_SYNONYMS if name in columns][0]
        price_col_name = [name for name in self.TOTAL_PRICE_SYNONYMS if name in columns][0]
        type_col_name = [name for name in self.ORDER_TYPE_SYNONYMS if name in columns]
        status_col_name = [name for name in self.STATUS_SYNONYMS if name in columns]
        for row in df.index:
            pair = df[pair_col_name][row].replace("/", "").replace("XETHZ", "ETH") \
                .replace("XXBTZ", "BTC").split(" ")[-1]
            if self.ASSET_NAME not in pair or (status_col_name and df[status_col_name[0]][row] == "Canceled"):
                continue
            change_as = pair.replace('-', '').replace(self.ASSET_NAME, '')
            date_st = df[date_col_name][row + date_col_offset].split(".")[0]
            total_am = float(str(df[amount_col_name][row]).split(" ")[0]) if pair.startswith(self.ASSET_NAME) else \
                float(str(df[price_col_name][row]).split(" ")[0])
            total_pr = float(str(df[price_col_name][row]).split(" ")[0]) if pair.startswith(self.ASSET_NAME) else \
                float(str(df[amount_col_name][row]).split(" ")[0])
            if not type_col_name or (df[type_col_name[0]][row].upper() == "BUY" and pair.startswith(self.ASSET_NAME)) \
                    or (df[type_col_name[0]][row].upper() == "SELL" and pair.endswith(self.ASSET_NAME)):
                self.buy_history = self.add_order_to_history(self.buy_history, change_as, date_st, total_am, total_pr)
            elif (df[type_col_name[0]][row].upper() == "SELL" and pair.startswith(self.ASSET_NAME)) or \
                    (df[type_col_name[0]][row].upper() == "BUY" and pair.endswith(self.ASSET_NAME)):
                self.sell_history = self.add_order_to_history(self.sell_history, change_as, date_st, total_am, total_pr)
            else:
                raise ValueError("Unknown order type {}".format(df[type_col_name][row]))

    def add_order_to_history(self, order_book: pd.DataFrame, change_asset: str, date_str: str, total_amount: float,
                             total_price: float, ):
        if change_asset != self.CURRENCY:
            change_factor = self.price_feed.get_change_factor(change_asset, self.CURRENCY, date=date_str)
        else:
            change_factor = 1
        return order_book.append(pd.DataFrame.from_dict({self.LEDGER_COLUMNS[0]: [date_str],
                                                         self.LEDGER_COLUMNS[1]: [total_amount],
                                                         self.LEDGER_COLUMNS[2]: [total_price * change_factor]}),
                                 ignore_index=True)

    def get_active_amount(self) -> float:
        return self.buy_history["Asset Amount"].sum() - self.sell_history["Asset Amount"].sum()

    def get_current_value(self) -> float:
        return self.get_active_amount() * self.price_feed.get_change_factor(self.ASSET_NAME, self.CURRENCY)

    def get_potential_profit(self) -> float:
        purchase_price = sum([
            self.open_positions[self.OPEN_POSITION_COLUMNS[1]][i] *
            self.open_positions[self.OPEN_POSITION_COLUMNS[2]][i]
            for i in self.open_positions.index])
        total_amount = self.open_positions[self.OPEN_POSITION_COLUMNS[1]].sum()
        potential_profit = total_amount * self.price_feed.get_change_factor(self.ASSET_NAME,
                                                                            self.CURRENCY) - purchase_price
        print("Purchase price:", purchase_price)
        print("Amount:", total_amount)
        print("Factor:", self.price_feed.get_change_factor(self.ASSET_NAME, self.CURRENCY))
        print("Potential profit:", potential_profit)
        return potential_profit

    def get_taxable_profit(self, year: int) -> float:
        taxable_events = self.closed_positions.loc[
            self.closed_positions[self.CLOSED_POSITION_COLUMNS[1]].str.startswith(str(year)) &
            (self.closed_positions[self.CLOSED_POSITION_COLUMNS[2]] <= 365)]
        print("TAXABLE PROFIT {}".format(self.ASSET_NAME), taxable_events[self.CLOSED_POSITION_COLUMNS[4]].sum())
        return taxable_events[self.CLOSED_POSITION_COLUMNS[4]].sum()

    def get_tax_free_amount(self, profit_so_far: float) -> float:
        generally_tax_free = 0
        still_tax_free = 0
        new_profit = 0
        current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        factor = self.price_feed.get_change_factor(self.ASSET_NAME, self.CURRENCY)
        for row in self.open_positions.index:
            if get_days_between_dates(current_time, self.open_positions[self.OPEN_POSITION_COLUMNS[0]][row]) > 365:
                generally_tax_free = generally_tax_free + self.open_positions[self.OPEN_POSITION_COLUMNS[1]][row]
            else:
                purchase_factor = self.open_positions[self.OPEN_POSITION_COLUMNS[2]][row]
                purchase_price = self.open_positions[self.OPEN_POSITION_COLUMNS[1]][row] * purchase_factor

                total_amount = self.open_positions[self.OPEN_POSITION_COLUMNS[1]][row]
                potential_profit = total_amount * factor - purchase_price
                if profit_so_far + new_profit + potential_profit < self.TAX_ALLOWANCE:
                    still_tax_free = still_tax_free + total_amount
                    new_profit = new_profit + potential_profit
                else:
                    still_allowed = self.TAX_ALLOWANCE - profit_so_far - new_profit
                    still_tax_free = still_tax_free + still_allowed / (factor - purchase_factor)
                    new_profit = new_profit + still_allowed
                    break
        if generally_tax_free > 0:
            print("You own {} of {} that may be sold without taxes.".format(generally_tax_free, self.ASSET_NAME))
        if still_tax_free > 0:
            print("With a realized profit of {}, you may still sell {} of {} for {} profit ({} in total).".format(
                profit_so_far, still_tax_free, self.ASSET_NAME, new_profit, still_tax_free * factor))
        return still_tax_free


class LedgerContainer:

    def __init__(self, assets: list, exports: str):
        self.asset_names = assets
        self.asset_ledgers = dict()
        price_feed = PriceFeed()
        for a in self.asset_names:
            ledger = CryptoLedger(a, price_feed)
            for exp in os.listdir(his):
                if exp.endswith(".csv") or exp.endswith(".xlsx"):
                    ledger.import_from_file(os.path.join(exports, exp))
            ledger.calculate_position_from_history()
            self.asset_ledgers[a] = ledger

    def print_summary(self, year: int):
        pd.set_option('display.max_columns', None)
        pot_prof = 0
        real_prof = 0
        for a in self.asset_names:
            print('\n\n\n#### SUMMARY FOR ' + a + ' ####')
            print('\n## BUY OVERVIEW ##')
            print(self.asset_ledgers[a].buy_history.head(30))
            print('\n## SELL OVERVIEW ##')
            print(self.asset_ledgers[a].sell_history.head(30))
            print('\nActive amount: ', self.asset_ledgers[a].get_active_amount())
            print('\n## OPEN POSITIONS ##')
            print(self.asset_ledgers[a].open_positions.head(30))
            print('\n## CLOSED POSITIONS ##')
            print(self.asset_ledgers[a].closed_positions.head(30))
            print('\nPOTENTIAL PROFIT')
            pot_prof = pot_prof + self.asset_ledgers[a].get_potential_profit()
            real_prof = real_prof + self.asset_ledgers[a].get_taxable_profit(year)
        print("\n\n\n")
        print("TOTAL IMAGINARY PROFIT: ", pot_prof)
        print("TOTAL TAXABLE PROFIT: ", real_prof)

    def get_sum_of_taxable_profits(self, year: int) -> float:
        real_prof = 0
        for a in self.asset_names:
            real_prof = real_prof + self.asset_ledgers[a].get_taxable_profit(year)
        return real_prof

    def summarize_sell_options(self, year: int):
        prof = self.get_sum_of_taxable_profits(year)
        for a in self.asset_names:
            self.asset_ledgers[a].get_tax_free_amount(prof)

    def get_total_portfolio_value(self) -> float:
        total_value = 0
        for a in self.asset_names:
            total_value = total_value + self.asset_ledgers[a].get_current_value()
        return total_value

    def get_portfolio_composition(self) -> dict:
        composition = dict()
        total_value = self.get_total_portfolio_value()
        for a in self.asset_names:
            share = self.asset_ledgers[a].get_current_value() / total_value
            composition[a] = share
        return composition


if __name__ == "__main__":
    his = r'/Users/peterpanda/Repos/crypto-ledger/exports'
    asset_list = ['USDT', 'BUSD', 'ERG', 'ETH', 'BTC', 'HBAR', 'LINK', 'SOL', 'KSM', 'DOT', 'ALGO', 'UST', 'RUNE',
                  'CKB', 'ADA', 'ATOM']
    ledgers = LedgerContainer(asset_list, his)
    ledgers.print_summary(2022)
    ledgers.summarize_sell_options(2022)
    print('\n## TOTAL VALUE ##')
    print(ledgers.get_total_portfolio_value())
    print('\n## COMPOSITION ## ')
    print(ledgers.get_portfolio_composition())
