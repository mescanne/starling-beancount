#!/usr/bin/env python3

import sys
from time import sleep
from datetime import date, timedelta, datetime
from decimal import Decimal
from pathlib import Path
from pprint import pprint
from typing import Any, Optional, Union

import httpx
import typer
import yaml
from beancount.core.amount import Amount
from beancount.core.data import Balance, Note, Posting, Transaction, new_metadata
from beancount.core.flags import FLAG_OKAY
from beancount.ingest.extract import print_extracted_entries  # type: ignore[import]
from beancount.utils.date_utils import parse_date_liberally  # type: ignore[import]

VALID_STATUS = ["PENDING", "SETTLED"]


def echo(it: str) -> None:
    print(it, file=sys.stderr)


def log(it: Any) -> None:
    print("\n\n\n")
    pprint(it, stream=sys.stderr)
    print()


class Config:
    def __init__(self, config_path: Path) -> None:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        self.base = config["base"]
        self.cps = config["cps"]
        self.joint = config["jointAccs"]
        self.users = config["userIds"]
        self.fasterPayments = config.get("fasterPayments",{})


def cleanup_string(s):
    s = s.strip()
    s = s.replace('_', ' ')
    s = ''.join([w.capitalize() for w in s.split(' ') if not w.isdigit()])
    s = ''.join([w for w in s if w.isalnum()])
    return s


class Account:
    def __init__(
        self,
        config_path: Path,
        acc: str,
        token_path: Path,
        verbose: bool = False,
    ) -> None:
        self.last_request = None
        self.acc = acc
        self.verbose = verbose
        self.conf = Config(config_path=config_path)
        self.account_name = ":".join((w.capitalize() for w in self.acc.split("_")))
        self.token = token_path.read_text().strip()
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.uid = self.get_uid()
        self.today = date.today()
        self.tomorrow = self.today + timedelta(days=1)
        self.start = self.today
        self.spaces = self.spaces()


    def _httpx_get(self, *args, **kwargs):
        if (self.last_request is not None and
            (datetime.now() - self.last_request) < timedelta(microseconds=200000)):
            sleep((datetime.now() - self.last_request).total_seconds())
        self.last_request = datetime.now()
        return httpx.get(*args, **kwargs)

    def get_uid(self) -> str:
        url = "/api/v2/accounts"
        r = self._httpx_get(self.conf.base + url, headers=self.headers)
        data = r.json()
        try:
            uid = str(data["accounts"][0]["accountUid"])
        except KeyError:
            log(data)
            sys.exit(1)
        if self.verbose:
            log(f"{uid}")
        return uid

    def get_balance_data(self) -> (str, Decimal):
        url = f"/api/v2/accounts/{self.uid}/balance"
        r = self._httpx_get(self.conf.base + url, headers=self.headers)
        data = r.json()
        if self.verbose:
            log(data)
        bal = Decimal(data["effectiveBalance"]["minorUnits"]) / 100
        return data["effectiveBalance"]["currency"], bal

    def balances(self, display: bool = False) -> Balance:
        balances = []

        for category, info in self.spaces.items():
            amt = Amount(info["balance"], info["ccy"])
            meta = new_metadata("starling-api", 999)

            bal = Balance(meta, self.tomorrow, self.account_name + info["name"], amt, None, None)
            balances.append(bal)

        if display:
            print_extracted_entries(balances, file=sys.stdout)

        return balances

    def note(self) -> Note:
        meta_end = new_metadata("starling-api", 998)
        note_end = Note(meta_end, self.today, self.account_name, "end bean-extract")
        return note_end

    def spaces(self) -> dict[str,str]:

        # get spaces
        spaces_categories = {}
        url = f"/api/v2/account/{self.uid}/spaces"
        r = self._httpx_get(self.conf.base + url, headers=self.headers)
        data = r.json()
        if "error" in data:
            echo(f"Error: {data['error_description']}")
            sys.exit(1)
        for sp in data["savingsGoals"]:
            spaces_categories[sp["savingsGoalUid"]] = {
                'name': sp["name"],
                'ccy': sp["totalSaved"]["currency"],
                'balance': Decimal(sp["totalSaved"]["minorUnits"]) / 100,
            }
        for sp in data["spendingSpaces"]:
            spaces_categories[sp["spaceUid"]] = {
                'name': sp["name"],
                'ccy': sp["balance"]["currency"],
                'balance': Decimal(sp["balance"]["minorUnits"]) / 100,
            }

        # get default category
        url = "/api/v2/accounts"
        r = self._httpx_get(self.conf.base + url, headers=self.headers)
        uid = r.json()["accounts"][0]["defaultCategory"]
        default_ccy, default_balance = self.get_balance_data()
        spaces_categories[uid] = {
           'name': '',
           'balance': default_balance,
           'ccy': default_ccy,
        }

        if self.verbose:
            log(spaces_categories)

        return spaces_categories

    def get_transaction_data(self, since: date) -> list[dict]:

        all_data = []
        for category, info in self.spaces.items():
            url = f"/api/v2/feed/account/{self.uid}/category/{category}/transactions-between"
            params = {
                "minTransactionTimestamp": f"{since}T00:00:00.000Z",
                "maxTransactionTimestamp": "2100-01-01T00:00:00.000Z",
            }
            r = self._httpx_get(
                self.conf.base + url,
                params=params,
                headers=self.headers,
            )
            data = r.json()
            feedItems = data["feedItems"]
            for f in feedItems:
                f["space"] = info["name"]
            all_data.extend(feedItems)
        return sorted(all_data, key=lambda x: str(x["transactionTime"]))

    def get_counter_account(self, item) -> str:

        # Try faster payments
        if (item["source"] == 'FASTER_PAYMENTS_IN' or
             item["source"] == 'FASTER_PAYMENTS_OUT'):

            if 'counterPartySubEntityIdentifier' in item:
                acct = (item['counterPartySubEntityIdentifier'] + '-' +
                        item['counterPartySubEntitySubIdentifier'])

                if acct in self.conf.fasterPayments:
                    return self.conf.fasterPayments[acct]

        # Try internal transfers
        if item["source"] == "INTERNAL_TRANSFER":
            return self.account_name + cleanup_string(item["counterPartyName"])

        # Try spending category
        if item["spendingCategory"] in self.conf.cps:
            return self.conf.cps[item["spendingCategory"]]

        # Default category
        cp = self.conf.cps["DEFAULT"]
        cp = cp.replace('<CATEGORY>',
                   cleanup_string(item["spendingCategory"]))
        return cp


    def transactions(self, since: date, display: bool = False) -> list[Transaction]:
        tr = self.get_transaction_data(since)
        txns = []
        for i, item in enumerate(tr):

            if self.verbose:
                log(item)

            # If internal and it's a space, skip it
            if item["source"] == "INTERNAL_TRANSFER" and item["space"] != "":
                continue

            # Check status
            if item["status"] not in VALID_STATUS:
                continue

            try:
                date = parse_date_liberally(item["transactionTime"])
                payee = item.get("counterPartyName", "FIXME")
                ref = " ".join(item.get("reference","").split())
                amt = Decimal(item["amount"]["minorUnits"]) / 100
                amt = amt if item["direction"] == "IN" else -amt

                user = item.get("transactingApplicationUserUid", None)
                if user and self.acc in self.conf.joint:
                    user = self.conf.users[user]
                else:
                    # must add this to not get unwanted UIDs returned
                    user = None
                
                cp = self.get_counter_account(item)
            except Exception as e:
                print(f'failed on {item}: {e}', file=sys.stderr)
                raise e

            extra_meta = {"user": user} if user else None
            meta = new_metadata("starling-api", i, extra_meta)
            p1 = Posting(self.account_name + item["space"], Amount(amt, "GBP"), None, None, None, None)
            p2 = Posting(cp, None, None, None, None, None)  # type: ignore[arg-type]
            txn = Transaction(
                meta=meta,
                date=date,
                flag=FLAG_OKAY,
                payee=payee,
                narration=ref,
                tags=set(),
                links=set(),
                postings=[p1, p2],
            )
            txns.append(txn)

        if len(txns) > 0:
            self.start = txns[0].date

        if display:
            print(f"* {self.acc} - {self.today}")
            print_extracted_entries(txns, sys.stdout)
        return txns


def extract(
    config_path: Path,
    acc: str,
    token_path: Path,
    since: date,
) -> list[Union[Transaction, Balance, Note]]:
    """bean-extract entrypoint"""
    account = Account(config_path=config_path, acc=acc, token_path=token_path)
    txns = account.transactions(since)
    bals = account.balances()
    note = account.note()
    return [*txns, *bals, note]


def main(
    config_path: Path,
    acc: str,
    token_path: Path,
    since: Optional[str] = None,
    balance: bool = False,
    verbose: bool = False,
) -> None:
    """CLI entrypoint"""
    if not since and not balance:
        echo("Need to provide a 'since' date for transactions")
        return

    account = Account(
        config_path=config_path, acc=acc, token_path=token_path, verbose=verbose
    )
    if balance:
        account.balance(display=True)
    else:
        assert since is not None
        since_dt = date.fromisoformat(since)
        account.transactions(since_dt, display=True)


def cli() -> None:
    typer.run(main)


if __name__ == "__main__":
    cli()
