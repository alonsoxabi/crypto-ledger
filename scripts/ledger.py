import pandas as pd
import os
from binance.client import Client
from binance.helpers import date_to_milliseconds
from datetime import datetime


def get_days_between_dates(date1: str, date2: str) -> float:
    dif_ms = abs(date_to_milliseconds(date1) - date_to_milliseconds(date2))
    return dif_ms / (1000 * 60 * 60 * 24)


class CryptoLedger:
    CURRENCY = "EUR"
    FALLBACK_CURRENCY = "BUSD"
    LEDGER_COLUMNS = ["Date(UTC)", "Asset Amount", "Total (" + CURRENCY + ")"]
    OPEN_POSITION_COLUMNS = ["Date(UTC)", "Asset Amount", CURRENCY + "/Unit"]
    CLOSED_POSITION_COLUMNS = ["Date(UTC) of purchase", "Date(UTC) of sell", "Holding time", "Asset Amount",
                               "Profit/Loss (" + CURRENCY + ")"]
    DATE_SYNONYMS = ["Pair", "Date(UTC)", "time"]
    PAIR_SYNONYMS = ["Pair", "Price", "pair"]
    ORDER_TYPE_SYNONYMS = ["Type", "type"]
    TOTAL_AMOUNT_SYNONYMS = ["Filled", "Final Amount", "vol"]
    TOTAL_PRICE_SYNONYMS = ["Total", "Amount", "cost"]
    TAX_ALLOWANCE = 600

    def __init__(self, asset: str, symbols: list = None):
        self.ASSET_NAME = asset
        self.buy_history = pd.DataFrame(columns=self.LEDGER_COLUMNS)
        self.sell_history = pd.DataFrame(columns=self.LEDGER_COLUMNS)
        self.open_positions = pd.DataFrame(columns=self.OPEN_POSITION_COLUMNS)
        self.closed_positions = pd.DataFrame(columns=self.CLOSED_POSITION_COLUMNS)
        self.client = Client()
        if symbols:
            self.all_symbols = symbols
        else:
            exchange_info = self.client.get_exchange_info()
            self.all_symbols = [s['symbol'] for s in exchange_info['symbols']]

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
                self.open_positions = self.open_positions.drop(open_index[:aggregator_index+1])
            self.closed_positions = self.closed_positions.append(pd.DataFrame.from_dict({
                self.CLOSED_POSITION_COLUMNS[0]: [date_of_purchase],
                self.CLOSED_POSITION_COLUMNS[1]: [date_of_sell],
                self.CLOSED_POSITION_COLUMNS[2]: [get_days_between_dates(date_of_purchase, date_of_sell)],
                self.CLOSED_POSITION_COLUMNS[3]: [sell_amount],
                self.CLOSED_POSITION_COLUMNS[4]: [sell_price - purchase_price]}), ignore_index=True)

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
        for row in df.index:
            pair = df[pair_col_name][row].replace("/", "").replace("XETHZ", "ETH")\
                .replace("XXBTZ", "BTC").split(" ")[-1]
            if self.ASSET_NAME not in pair:
                continue
            change_as = pair.replace(self.ASSET_NAME, '')
            date_st = df[date_col_name][row + date_col_offset].split(".")[0]
            total_am = float(str(df[amount_col_name][row]).split(" ")[0])
            total_pr = float(str(df[price_col_name][row]).split(" ")[0])
            if not type_col_name or df[type_col_name[0]][row].upper() == "BUY":
                self.buy_history = self.add_order_to_history(self.buy_history, change_as, date_st, total_am, total_pr)
            elif df[type_col_name[0]][row].upper() == "SELL":
                self.sell_history = self.add_order_to_history(self.sell_history, change_as, date_st, total_am, total_pr)
            else:
                raise ValueError("Unknown order type {}".format(df[type_col_name][row]))

    def add_order_to_history(self, order_book: pd.DataFrame, change_asset: str, date_str: str, total_amount: float,
                             total_price: float,):
        if change_asset != self.CURRENCY:
            change_factor = self.get_change_factor(change_asset, date=date_str)
        else:
            change_factor = 1
        return order_book.append(pd.DataFrame.from_dict({self.LEDGER_COLUMNS[0]: [date_str],
                                                         self.LEDGER_COLUMNS[1]: [total_amount],
                                                         self.LEDGER_COLUMNS[2]: [total_price * change_factor]}),
                                 ignore_index=True)

    def get_change_factor(self, asset: str, currency: str = CURRENCY, date: str = None) -> float:
        if currency + asset in self.all_symbols:
            symbol = currency + asset
        elif asset + currency in self.all_symbols:
            symbol = asset + currency
        elif currency != self.FALLBACK_CURRENCY:
            asset_to_fallback = self.get_change_factor(asset, currency=self.FALLBACK_CURRENCY, date=date)
            return asset_to_fallback * self.get_change_factor(self.FALLBACK_CURRENCY, currency=currency, date=date)
        else:
            raise ValueError("No symbol defined for combination of {} and {}".format(asset, currency))
        if date:
            date_ms = date_to_milliseconds(date)
            start_date_ms = date_ms - 60000
            kline = self.client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, start_date_ms, date_ms)
            if kline:
                symbol_price = float(kline[0][1])
            else:
                raise ValueError("API call did not return any values for asset {} and date {}.".format(asset, date))
        else:
            symbol_overview = self.client.get_symbol_ticker(symbol=symbol)
            symbol_price = float(symbol_overview["price"])
        if symbol_price:
            if symbol.startswith(currency):
                return 1 / symbol_price
            else:
                return symbol_price
        else:
            raise ValueError("API call did not return any values.")

    def get_active_amount(self):
        return self.buy_history["Asset Amount"].sum() - self.sell_history["Asset Amount"].sum()

    def get_potential_profit(self) -> float:
        purchase_price = sum([
            self.open_positions[self.OPEN_POSITION_COLUMNS[1]][i] *
            self.open_positions[self.OPEN_POSITION_COLUMNS[2]][i]
            for i in self.open_positions.index])
        total_amount = self.open_positions[self.OPEN_POSITION_COLUMNS[1]].sum()
        potential_profit = total_amount * self.get_change_factor(self.ASSET_NAME) - purchase_price
        print("Purchase price:", purchase_price)
        print("Amount:", total_amount)
        print("Factor:", self.get_change_factor(self.ASSET_NAME))
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
        factor = self.get_change_factor(self.ASSET_NAME)
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
        for a in self.asset_names:
            ledger = CryptoLedger(a)
            for exp in os.listdir(his):
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
            print(self.asset_ledgers[a].buy_history.head(10))
            print('\n## SELL OVERVIEW ##')
            print(self.asset_ledgers[a].sell_history.head(10))
            print('\nActive amount: ', self.asset_ledgers[a].get_active_amount())
            print('\n## OPEN POSITIONS ##')
            print(self.asset_ledgers[a].open_positions.head(10))
            print('\n## CLOSED POSITIONS ##')
            print(self.asset_ledgers[a].closed_positions.head(10))
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


if __name__ == "__main__":
    his = r'C:\Users\maxst\Desktop\cryptoLedger\exports'
    asset_list = ['ETH', 'BTC', 'HBAR', 'LINK', 'SOL', 'KSM', 'DOT', 'ALGO']
    ledgers = LedgerContainer(asset_list, his)
    ledgers.print_summary(2021)
    ledgers.summarize_sell_options(2021)
