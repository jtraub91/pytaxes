import argparse
import csv
import json
import logging
import os
import time
import urllib.request
from datetime import datetime
from getpass import getpass

from pypdf import PdfReader, PdfWriter


log = logging.getLogger(__name__)


def create_consolidated_report(report_path: str):
    # BlockFi
    with open("data/blockfi_transaction_report_all.csv") as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
    blockfi_trade_rows = [row for row in rows if row[2] == "Trade"]
    blockfi_reformatted_rows = [
        [row[-1], row[0], row[1], "", "", "BlockFi"]
        for row in blockfi_trade_rows
        if row[0] != "DAI"
    ]
    blockfi_reformatted_rows = [
        [datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").isoformat()] + row[1:]
        for row in blockfi_reformatted_rows
    ]

    # Coinbase
    with open("data/Coinbase-alltime-transactions-2023-02-28.csv") as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
        rows = rows[7:]
    coinbase_reformatted_rows = [
        row + ["Coinbase"]
        for row in rows
        if row[1]
        in [
            "Convert",
            "Buy",
            "Advanced Trade Buy",
            "Advanced Trade Sell",
            "CardSpend",
            "Sell",
            "Card Spend",
            "Card Buy Back",
            # "Rewards Income",
            # "Learning Reward",
            "CardBuyBack",
        ]
        and row[2]
        in [
            "BTC",
            "LTC",
            "BCH",
            "ETC",
            "ETH",
            "SHIB",
            "DOGE",
            "ADA",
            "ATOM",
            "SOL",
            "DOT",
            "MATIC",
            "FIL",
            "LINK",
            "ZEC",
        ]
    ]
    coinbase_reformatted_rows = [
        [datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%SZ").isoformat()] + row[1:]
        for row in coinbase_reformatted_rows
    ]
    rows_prime = []
    for row in coinbase_reformatted_rows:
        if row[1] == "Buy":
            comment = row[9]
            words = comment.split()
            amount, asset, dollars = words[1], words[2], words[4][1:]
            rows_prime.append([row[0], asset, amount, row[5], dollars, row[-1]])
        elif row[1] == "Convert":
            comment = row[9]
            words = comment.split()
            amount_1, asset_1, amount_2, asset_2 = (
                words[1],
                words[2],
                words[4],
                words[5],
            )
            rows_prime.append([row[0], asset_1, -1 * float(amount_1), "", "", row[-1]])
            rows_prime.append([row[0], asset_2, amount_2, "", "", row[-1]])
        elif row[1] in ["CardSpend", "Card Spend"]:
            rows_prime.append(
                [
                    row[0],
                    row[2],
                    -1 * float(row[3]),
                    row[5],
                    -1 * float(row[3]) * float(row[5]),
                    row[-1],
                ]
            )
        elif row[1] in ["CardBuyBack", "Card Buy Back"]:
            rows_prime.append(
                [row[0], row[2], row[3], row[5], float(row[3]) * float(row[5]), row[-1]]
            )
        elif row[1] == "Advanced Trade Buy":
            rows_prime.append([row[0], row[2], row[3], row[5], row[7], row[-1]])
        elif row[1] == "Advanced Trade Sell":
            rows_prime.append(
                [
                    row[0],
                    row[2],
                    -1 * float(row[3]),
                    row[5],
                    -1 * float(row[7]),
                    row[-1],
                ]
            )
        elif row[1] == "Sell":
            rows_prime.append(
                [
                    row[0],
                    row[2],
                    -1 * float(row[3]),
                    row[5],
                    -1 * float(row[7]),
                    row[-1],
                ]
            )
        else:
            rows_prime.append(row)
    coinbase_reformatted_rows = rows_prime

    # coinbase pro
    with open("data/coinbase-pro-account-010117-031323.csv") as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
        rows = rows[1:]
    coinbase_pro_match_rows = [row for row in rows if row[1] == "match"]
    # group by order-id
    match_orders = {}
    for row in coinbase_pro_match_rows:
        if match_orders.get(row[8]):
            match_orders[row[8]] += [row]
        else:
            match_orders[row[8]] = [row]

    coinbase_pro_reformatted_rows = []
    for order in match_orders:
        reformatted_rows = []

        orders = match_orders[order]

        usd_match_rows = list(filter(lambda row: row[5] in ["USD", "USDT"], orders))
        crypto_match_rows = list(
            filter(lambda row: row[5] not in ["USD", "USDT"], orders)
        )
        if len(usd_match_rows) == 2 and len(crypto_match_rows) == 0:
            # wash
            log.info(
                f"match {usd_match_rows[0][3]} {usd_match_rows[0][5]} / {usd_match_rows[1][3]} {usd_match_rows[1][5]} is a wash. ignoring..."
            )
        elif len(crypto_match_rows):
            date_split = orders[0][2].split(".")[0]
            date = datetime.strptime(date_split + "Z", "%Y-%m-%dT%H:%M:%SZ").isoformat()

            # consolidate crypto match rows in case multiple rows
            crypto_match_rows_consolidated = []

            for row in crypto_match_rows:
                if matched_row := list(
                    filter(lambda r: r[5] == row[5], crypto_match_rows_consolidated)
                ):
                    # if crypto asset already in consolidated rows, consolidate
                    replacement_row = matched_row[0]
                    crypto_match_rows_consolidated.remove(matched_row[0])
                    replacement_row[3] = float(replacement_row[3]) + float(row[3])
                    crypto_match_rows_consolidated += [replacement_row]
                else:
                    crypto_match_rows_consolidated += [row]

            # consolidate usd match rows in case multiple rows
            usd_match_rows_consolidated = []
            for row in usd_match_rows:
                if not usd_match_rows_consolidated:
                    usd_match_rows_consolidated += [row]
                else:
                    usd_match_rows_consolidated[0][3] = float(
                        usd_match_rows_consolidated[0][3]
                    ) + float(row[3])

            if not usd_match_rows_consolidated:
                for row in crypto_match_rows_consolidated:
                    reformatted_rows.append(
                        [
                            date,
                            row[5],
                            float(row[3]),
                            "",
                            "",
                            "Coinbase Pro",
                        ]
                    )
            elif (
                len(usd_match_rows_consolidated) == 1
                and len(crypto_match_rows_consolidated) == 1
            ):
                usd_match_row = usd_match_rows_consolidated[0]
                crypto_match_row = crypto_match_rows_consolidated[0]
                symbol = crypto_match_row[5]
                amount = float(crypto_match_row[3])
                cost = -1 * float(usd_match_row[3])
                reformatted_rows.append(
                    [
                        date,
                        symbol,
                        amount,
                        abs(cost) / abs(amount),
                        cost,
                        "Coinbase Pro",
                    ]
                )
            else:
                raise ValueError(
                    f"usd_match_rows_consolidated:crypto_match_rows_consolidated not 1:1 for {order}"
                )
        else:
            raise ValueError(f"{order}: more than 2 usd rows and no crypto rows")
        coinbase_pro_reformatted_rows += reformatted_rows

    KRAKEN_ASSET_CODE_MAP = {
        "XXBT": "BTC",
        "XETH": "ETH",
        "XXMR": "XMR",
        "SOL": "SOL",
        "ADA": "ADA",
        "LTC": "LTC",
        "XXDG": "DOGE",
        "ATOM": "ATOM",
        "DOT": "DOT",
        "MATIC": "MATIC",
        "LUNA": "LUNA",
        "APE": "APE",
        "BCH": "BCH",
        "UST": "UST",
    }
    # kraken
    with open("data/kraken-ledgers-alltime.csv") as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
    kraken_trade_rows = [
        row
        for row in rows
        if row[3] == "trade" and row[6] not in ["ZUSD", "USDT", "LUNA2"]
    ]
    kraken_reformatted_rows = [
        [row[2], KRAKEN_ASSET_CODE_MAP[row[6]], row[7], "", "", "Kraken"]
        for row in kraken_trade_rows
    ]
    kraken_reformatted_rows = [
        [datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").isoformat()] + row[1:]
        for row in kraken_reformatted_rows
    ]

    with open("data/uphold-transaction-history-031423.csv") as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
        rows = rows[1:]
    uphold_rows = [row for row in rows if row[-1] != "out"]
    uphold_reformatted_rows = []
    for row in uphold_rows:
        date = datetime.strptime(row[0], "%a %b %d %Y %H:%M:%S GMT+0000").isoformat()
        dest_amount = float(row[2])
        dest_currency = row[3]
        origin_amount = float(row[8])
        origin_currency = row[9]
        if dest_currency == origin_currency:
            if dest_currency == "BAT":
                # assume earnings, add for calculation into cost basis
                uphold_reformatted_rows.append(
                    [date, dest_currency, dest_amount, "", "", "Uphold"]
                )
        elif row[-1] == "transfer":
            uphold_reformatted_rows.append(
                [date, origin_currency, -1 * origin_amount, "", "", "Uphold"]
            )
            if dest_currency not in ["USD", "USDC", "DAI"]:
                uphold_reformatted_rows.append(
                    [date, dest_currency, dest_amount, "", "", "Uphold"]
                )
        elif row[-1] == "in":
            assert (
                origin_currency == "USD"
            ), f"{date} origin currency not USD for 'in' row"
            uphold_reformatted_rows.append(
                [
                    date,
                    dest_currency,
                    dest_amount,
                    origin_amount / dest_amount,
                    origin_amount,
                    "Uphold",
                ]
            )

    consolidated_rows = (
        blockfi_reformatted_rows
        + coinbase_reformatted_rows
        + coinbase_pro_reformatted_rows
        + kraken_reformatted_rows
        + uphold_reformatted_rows
    )
    consolidated_rows = sorted(
        consolidated_rows, key=lambda elem: datetime.fromisoformat(elem[0]).timestamp()
    )

    # fill in un-filled spot prices from api
    baseurl = "https://api.coinranking.com/v2"
    with open(".apikey") as apikey_file:
        apikey = apikey_file.read()
    headers = {"x-access-token": apikey}

    req = urllib.request.Request(baseurl + "/coins?limit=5000", headers=headers)
    ret = urllib.request.urlopen(req)

    coins = json.load(ret)["data"]["coins"]

    coin_uuids: dict[str, str] = {}
    coin_histories: dict[str, list] = {}

    # retrieve spot price in rows where missing
    rows_prime = []
    for row in consolidated_rows:
        if row[3]:
            rows_prime.append(row)
            continue
        symbol = row[1]

        if coin_uuids.get(symbol):
            coin_uuid = coin_uuids.get(symbol)
        else:
            if symbol == "LUNA":
                symbol_prime = "WLUNA"
            else:
                symbol_prime = symbol
            coin_data = next(filter(lambda c: c["symbol"] == symbol_prime, coins))
            coin_uuid = coin_data["uuid"]
            coin_uuids[symbol] = coin_uuid
        if coin_histories.get(symbol):
            coin_history = coin_histories.get(symbol)
        else:
            req = urllib.request.Request(
                baseurl + f"/coin/{coin_uuid}/history?timePeriod=5y", headers=headers
            )
            ret = urllib.request.urlopen(req)
            coin_history = json.load(ret)["data"]["history"]
            coin_histories[symbol] = coin_history
        timestamp = int(datetime.fromisoformat(row[0]).timestamp())

        price_filter_search = filter(lambda h: h["timestamp"] < timestamp, coin_history)
        price = next(price_filter_search)["price"]
        while not price:
            # weird case where price = None
            price = next(price_filter_search)["price"]

        row_prime = row[:3] + [price] + row[4:]
        rows_prime.append(row_prime)
    consolidated_rows = rows_prime

    # loop thru rows calculate total cost
    rows_prime = []
    for row in consolidated_rows:
        if not row[4] and row[3]:
            row_prime = row[:4] + [float(row[2]) * float(row[3])] + row[5:]
            rows_prime.append(row_prime)
        else:
            rows_prime.append(row)
    consolidated_rows = rows_prime

    # consolidate into single csv
    with open(os.path.join(report_path, "consolidated.csv"), "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "Date",
                "CryptoAsset",
                "Amount",
                "Spot Price (USD)",
                "Total Cost (USD)",
                "Source",
            ]
        )
        writer.writerows(consolidated_rows)

    return


def calculate_pnl(report_path: str):
    """
    Calculate PNL and generate 8949.csv
    """
    with open(os.path.join(report_path, "consolidated.csv")) as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
    rows = rows[1:]

    max_unaccounted_profit = 0

    pnl_rows = []
    cost_basis_pools = {}
    for i, row in enumerate(rows):
        # loop thru rows which are chronologically sorted
        date = row[0]
        symbol = row[1]
        amount = float(row[2].replace(",", ""))
        spot_price = float(row[3])
        total_cost = float(row[4])

        if amount >= 0:
            # add to cost basis pool
            if not cost_basis_pools.get(symbol):
                cost_basis_pools[symbol] = []
            cost_basis_pools[symbol] += [[i, date, amount, spot_price]]
        else:
            # pop from stack in hifo manner, and record to 8949 pnl
            if not cost_basis_pools.get(symbol):
                max_unaccounted_profit += abs(amount) * spot_price
                log.warning(
                    f"no cost basis {date}. skipping impact on pnl, {amount} {symbol} for ${abs(amount) * spot_price}"
                )
                continue
            hifo_sorted_asset_pool = sorted(
                cost_basis_pools[symbol],
                key=lambda elem: elem[-1],  # spot price
                reverse=True,
            )
            hifo_index = 0
            hifo_element = hifo_sorted_asset_pool[hifo_index]

            remaining = abs(amount)
            while remaining > 0:
                remaining -= hifo_element[2]
                if remaining > 0:
                    cost_basis_pools[symbol].remove(hifo_element)
                    pnl_rows.append(
                        [
                            f"{round(hifo_element[2], 8)} {symbol}",
                            hifo_element[1],
                            row[0],
                            hifo_element[2] * abs(total_cost) / abs(amount),
                            hifo_element[2] * hifo_element[3],
                            (hifo_element[2] * abs(total_cost) / abs(amount))
                            - (hifo_element[2] * hifo_element[3]),
                        ]
                    )
                    hifo_index += 1
                    try:
                        hifo_element = hifo_sorted_asset_pool[hifo_index]
                    except IndexError:
                        max_unaccounted_profit += remaining * spot_price
                        log.warning(
                            f"no cost basis {date} for remaining {remaining} {symbol}. "
                            + f"skipping impact on pnl, sale of {remaining} {symbol} for ${remaining * spot_price}"
                        )
                        break
                elif remaining < 0:
                    amount = remaining + hifo_element[2]
                    # replace element in cost basis pool minus sold amoint
                    replacement_element = hifo_element
                    replacement_element[2] = abs(remaining)
                    for i, elem in enumerate(cost_basis_pools[symbol]):
                        if elem == hifo_element:
                            cost_basis_pools[symbol][i] = replacement_element
                    pnl_rows.append(
                        [
                            f"{round(amount, 8)} {symbol}",
                            hifo_element[1],
                            row[0],
                            amount * spot_price,
                            amount * hifo_element[3],
                            (amount * spot_price) - (amount * hifo_element[3]),
                        ]
                    )
                else:  # remaining == 0
                    cost_basis_pools[symbol].remove(hifo_element)
                    pnl_rows.append(
                        [
                            f"{round(hifo_element[2], 8)} {symbol}",
                            hifo_element[1],
                            row[0],
                            hifo_element[2] * spot_price,
                            hifo_element[2] * hifo_element[3],
                            (hifo_element[2] * spot_price)
                            - (hifo_element[2] * hifo_element[3]),
                        ]
                    )

    log.debug(f"cost basis pools remaining: {cost_basis_pools}")

    with open(os.path.join(report_path, "8949.csv"), "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "Description",
                "Date Acquired",
                "Date Sold",
                "Proceeds",
                "Cost",
                "Gains or losses",
            ]
        )
        writer.writerows(pnl_rows)

    pnl = 0
    for row in pnl_rows:
        if row[2].startswith("2022"):
            pnl += row[-1]
    print(f"2022 Gain/Loss: ${pnl}")
    print(f"max unaccounted profit: ${max_unaccounted_profit}")


def generate_pdf(csv_report_filename: str, report_path: str, tax_year: str = "2022"):
    with open(csv_report_filename) as csv_file:
        reader = csv.reader(csv_file)
        pnl_rows = [row for row in reader]
        pnl_rows = pnl_rows[1:]

    name = input("Name(s) shown on return: ")
    ssn = getpass("Social security number or taxpayer identification number: ")

    pdf_reader = PdfReader("templates/f8949.pdf")

    text_fields = pdf_reader.get_form_text_fields()
    transaction_fields = {
        key: value for key, value in text_fields.items() if key.startswith("f1")
    }
    transaction_fields.pop("f1_1[0]")
    transaction_fields.pop("f1_2[0]")

    totals_field_names = [
        "f1_115[0]",
        "f1_116[0]",
        "f1_117[0]",
        "f1_118[0]",
        "f1_119[0]",
    ]
    for field in totals_field_names:
        transaction_fields.pop(field)

    # form transaction_rows_fields list which contain the field name corresponding
    # to row for transaction in pdf table, e.g.
    # [
    #   ['f1_3[0]', 'f1_4[0]', 'f1_5[0]', 'f1_6[0]', 'f1_7[0]', 'f1_8[0]', 'f1_9[0]', 'f1_10[0]'],
    #   ['f1_11[0]', 'f1_12[0]', 'f1_13[0]', 'f1_14[0]', 'f1_15[0]', 'f1_16[0]', 'f1_17[0]', 'f1_18[0]'],
    #   ...
    # ]
    transaction_rows_fields = []
    row_fields = []
    for i, field in enumerate(transaction_fields, start=1):
        row_fields.append(field)
        if not i % 8:
            transaction_rows_fields.append(row_fields)
            row_fields = []

    # fill pdf
    pdf_writers = []  # multiple PDFs may be needed
    pdf_writer = PdfWriter()
    pdf_writer.add_page(pdf_reader.pages[0])
    pdf_writer.update_page_form_field_values(
        pdf_writer.pages[0],
        {
            "f1_1[0]": name,
            "f1_2[0]": ssn,
        },
    )

    tax_rows = [row for row in pnl_rows if row[2].startswith(tax_year)]
    total_proceeds = 0
    total_cost = 0
    total_gain_loss = 0

    row_index = 0
    for row in tax_rows:
        try:
            tr_fields = transaction_rows_fields[row_index]
        except IndexError as error:
            log.debug(f"index error caught, {error}. new pdf to be filled")
            pdf_writer.update_page_form_field_values(
                pdf_writer.pages[0],
                {
                    "f1_115[0]": total_proceeds,
                    "f1_116[0]": total_cost,
                    "f1_119[0]": total_gain_loss,
                },
            )
            pdf_writers.append(pdf_writer)

            total_proceeds = 0
            total_cost = 0
            total_gain_loss = 0

            row_index = 0

            pdf_writer = PdfWriter()
            pdf_writer.add_page(pdf_reader.pages[0])
            pdf_writer.update_page_form_field_values(
                pdf_writer.pages[0],
                {
                    "f1_1[0]": name,
                    "f1_2[0]": ssn,
                },
            )

            tr_fields = transaction_rows_fields[row_index]

        date_acquired = datetime.fromisoformat(row[1]).strftime("%m/%d/%y")
        date_sold = datetime.fromisoformat(row[2]).strftime("%m/%d/%y")
        proceeds = round(float(row[3]))
        cost = round(float(row[4]))
        pdf_writer.update_page_form_field_values(
            pdf_writer.pages[0],
            {
                tr_fields[0]: row[0],
                tr_fields[1]: date_acquired,
                tr_fields[2]: date_sold,
                tr_fields[3]: proceeds,
                tr_fields[4]: cost,
                tr_fields[7]: proceeds - cost,
            },
        )
        total_proceeds += proceeds
        total_cost += cost
        total_gain_loss += proceeds - cost
        row_index += 1

    pdf_writer.update_page_form_field_values(
        pdf_writer.pages[0],
        {
            "f1_115[0]": total_proceeds,
            "f1_116[0]": total_cost,
            "f1_119[0]": total_gain_loss,
        },
    )
    pdf_writers.append(pdf_writer)

    for i, pdf_writer in enumerate(pdf_writers):
        with open(os.path.join(report_path, f"f8949_{i}.pdf"), "wb") as pdf_file:
            pdf_writer.write(pdf_file)
    log.info(f"8949_i.pdf reports generated in {report_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pnl", action="store_true", help="calculate pnl")
    parser.add_argument(
        "--no-pdf",
        default=False,
        action="store_true",
        help="don't output pdf in addition to csv when calculating --pnl",
    )

    args = parser.parse_args()

    report_subdir = f"reports/{int(1000 * time.time())}"
    if not os.path.exists(report_subdir):
        os.makedirs(report_subdir)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s] %(message)s")
    fh = logging.FileHandler(os.path.join(report_subdir, "pytaxes.log"))
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    log.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.addHandler(sh)

    create_consolidated_report(report_subdir)
    if args.pnl:
        calculate_pnl(report_subdir)
        if not args.no_pdf:
            generate_pdf(os.path.join(report_subdir, "8949.csv"), report_subdir)


if __name__ == "__main__":
    main()
