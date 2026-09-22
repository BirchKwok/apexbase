"""Scenario 03: transaction ledger + point lookups (HTAP serving OLTP and OLAP from one database)

Business context
----------------
An internal settlement system has to do two very different things on one dataset:

* **OLTP side**: open accounts, deposit, withdraw, reverse a posting. It needs an
  O(1) point lookup of the live balance by account primary key, and the balance
  must be updated immediately after every posting.
* **OLAP side**: end-of-day reconciliation. It has to scan the whole ledger, sum
  debits and credits per account, compare that against the balance table account
  by account, and verify that debits and credits balance globally.

The traditional answer is "MySQL for bookkeeping + a warehouse for reconciliation
+ daily ETL", a chain that has latency and drifts. ApexBase is an embedded HTAP
engine: in one process and one file it serves primary-key point lookups and
full-table aggregates alike, so the reconciliation SQL reads exactly the ledger
rows that were just written, with no synchronisation window.

ApexBase features demonstrated
------------------------------
1. ``client.retrieve(id)``: point lookup by internal ``_id``, returning the whole
   row as a dict.
2. ``client.replace(id, {...})``: in-place update of an account balance by
   ``_id``; the Python-level ``replace`` / ``delete`` are safe in practice (the
   same-named Rust interface has a known defect, but the Python wrapper does not
   take that path).
3. ``client.store([...])``: batch-append ledger entries (a single dict also works).
4. Explicit ``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` transactions: demonstrating
   "visible inside the transaction, gone after rollback".
5. **Reconciliation aggregation SQL**: ``CASE WHEN`` folds the direction into a
   signed amount, ``GROUP BY`` sums it, and a ``JOIN`` against the balance table
   compares the two; ``SUM(CASE ...)`` proves global debit/credit balance.

A key design decision: why ``_id`` equals ``account_id``
--------------------------------------------------------
``retrieve(id)`` uses the internal primary key ``_id`` (auto-increment from 1, in
write order). This example opens accounts in ``account_id = 1..5`` order, so
``_id`` happens to equal ``account_id`` and every point lookup is a genuine
primary-key query. To make that assumption **verifiable** rather than a
convention, the code asserts ``row["account_id"] == account_id`` and
``row["_id"] == account_id`` after each lookup; if the write order ever changed,
the assertion would fail immediately.

Capability boundaries (this example follows the supported forms)
----------------------------------------------------------------
1. ``IN`` requires a column on the left-hand side; an expression such as
   ``UPPER(x)`` is rejected.
2. **Do not use ``COUNT(*)`` to assert visibility inside a transaction**:
   ``COUNT(*)`` takes the row-count metadata fast path and cannot see rows that
   are uncommitted in the transaction. To decide whether a row is visible inside
   the transaction, ``SELECT`` it explicitly.
3. **Inside a transaction, do not bind float columns with ``?`` in an INSERT**:
   in practice, after ``BEGIN`` a value bound through ``VALUES (?, ...)`` is
   written as ``0.0`` (numeric literals behave normally). This example therefore
   inlines the amounts into the SQL -- all values are controlled by the code, so
   there is no injection risk; in production, prefer ``store()`` outside a
   transaction and keep only the DDL / simple writes that need atomicity inside.
4. Time fields do not use ``DATE()`` / ``strftime``; the posting day ``day`` is
   persisted directly as a ``YYYY-MM-DD`` string column.

How to run
----------
    python py03_transaction_ledger.py

Output: each step prints to the console; the database file lives in
``_out/py_03/db``.
"""

from __future__ import annotations

import os

from apexbase import ApexClient
from _demo_env import assert_close, section, show, work_dir

SLUG = "03"

ACCOUNTS_TABLE = "accounts"
LEDGER_TABLE = "ledger"

# Account numbers deliberately start at 1 and stay contiguous so that
# _id == account_id and point lookups hit the real primary key.
EXTERNAL_ACCOUNT = 5          # External funding pool: the other leg of every deposit/withdrawal
EXTERNAL_OWNER = "EXTERNAL"
DAY = "2024-03-31"

ACCOUNTS = [
    (1, "alice"),
    (2, "bob"),
    (3, "carol"),
    (4, "dave"),
    (EXTERNAL_ACCOUNT, EXTERNAL_OWNER),
]
OWNERS = {acct: owner for acct, owner in ACCOUNTS}


def open_accounts(client: ApexClient) -> None:
    """Open accounts: write all of them at once, each with an opening balance of 0.

    The balance is the "materialised current state" while the ledger is the
    "immutable fact". Reconciliation exists precisely to prove the two stay
    consistent -- which is also why balance updates go through ``replace`` instead
    of being recomputed.
    """
    client.create_table(
        ACCOUNTS_TABLE,
        {
            "account_id": "int64",
            "owner": "string",
            "balance": "float64",
            "status": "string",
        },
    )
    client.store(
        [
            {"account_id": acct, "owner": owner, "balance": 0.0, "status": "open"}
            for acct, owner in ACCOUNTS
        ]
    )


def create_ledger(client: ApexClient) -> None:
    """Create the ledger table: append-only, with ``entry_id`` as the business key."""
    client.create_table(
        LEDGER_TABLE,
        {
            "entry_id": "int64",
            "txn_id": "string",
            "account_id": "int64",
            "direction": "string",
            "amount": "float64",
            "day": "string",
            "memo": "string",
        },
    )


class Ledger:
    """A very thin bookkeeping helper: double-entry legs, materialised balances, ledger appends.

    The reason for a class instead of scattered function calls: double-entry
    bookkeeping has one inviolable constraint -- **the credits of a transaction
    must equal its debits**. Confining that constraint to one method means every
    posting validates it automatically and the invariant has an obvious home.

    A trap worth knowing: ``client.store()`` and ``client.replace()`` act on the
    **currently selected table**; ApexBase does not infer the target table. This
    class therefore calls ``use_table()`` explicitly before every operation --
    without that step the ledger rows would be written into the accounts table.
    """

    def __init__(self, client: ApexClient) -> None:
        self.client = client
        self.balances = {acct: 0.0 for acct, _ in ACCOUNTS}
        self.entry_id = 0
        self.txn_seq = 0

    def next_txn_id(self, tag: str) -> str:
        self.txn_seq += 1
        return f"T{self.txn_seq:04d}-{tag}"

    def post(self, txn_id: str, legs: list[tuple[int, str, float]], memo: str) -> None:
        """Post one double-entry transaction.

        Args:
            txn_id: Transaction id, shared by all legs of the transaction so they
                can be aggregated afterwards.
            legs: ``[(account_id, direction, amount), ...]``; ``direction`` is
                ``credit`` (the account balance increases) or ``debit`` (the
                account balance decreases).
            memo: Description; a reversal records the original transaction id here.
        """
        # Invariant 1: debits and credits must balance. This assertion is a
        # business rule rather than data validation, so it must stop the posting
        # before anything reaches storage.
        credit = sum(amount for _, direction, amount in legs if direction == "credit")
        debit = sum(amount for _, direction, amount in legs if direction == "debit")
        assert abs(credit - debit) < 1e-9, (
            f"{txn_id} is unbalanced: credits {credit} vs debits {debit}"
        )

        # Step one: update the materialised balance (the OLTP point write).
        self.client.use_table(ACCOUNTS_TABLE)
        for acct, direction, amount in legs:
            delta = amount if direction == "credit" else -amount
            self.balances[acct] = round(self.balances[acct] + delta, 2)
            # replace is an in-place update: the read-modify-write happens on the
            # Python side and storage sees a single overwrite.
            ok = self.client.replace(
                acct,
                {
                    "account_id": acct,
                    "owner": OWNERS[acct],
                    "balance": self.balances[acct],
                    "status": "open",
                },
            )
            assert ok, f"balance update for account {acct} failed"

        # Step two: append the ledger entries (append-only).
        self.client.use_table(LEDGER_TABLE)
        rows = []
        for acct, direction, amount in legs:
            self.entry_id += 1
            rows.append(
                {
                    "entry_id": self.entry_id,
                    "txn_id": txn_id,
                    "account_id": acct,
                    "direction": direction,
                    "amount": round(amount, 2),
                    "day": DAY,
                    "memo": memo,
                }
            )
        self.client.store(rows)

    def deposit(self, acct: int, amount: float, memo: str) -> str:
        """Deposit: external funding pool -> target account."""
        txn_id = self.next_txn_id("DEP")
        self.post(
            txn_id,
            [(acct, "credit", amount), (EXTERNAL_ACCOUNT, "debit", amount)],
            memo,
        )
        return txn_id

    def withdraw(self, acct: int, amount: float, memo: str) -> str:
        """Withdraw: target account -> external funding pool.

        Insufficient funds are rejected outright (``ValueError``) and **no ledger
        entry is written**, which guarantees the ledger can never contain an
        "overdrawn but unrecorded" state.
        """
        if self.balances[acct] < amount:
            raise ValueError(
                f"account {acct} has insufficient funds: available {self.balances[acct]}, requested {amount}"
            )
        txn_id = self.next_txn_id("WDR")
        self.post(
            txn_id,
            [(acct, "debit", amount), (EXTERNAL_ACCOUNT, "credit", amount)],
            memo,
        )
        return txn_id

    def transfer(self, src: int, dst: int, amount: float, memo: str) -> str:
        """Internal transfer: move money between accounts."""
        if self.balances[src] < amount:
            raise ValueError(
                f"account {src} has insufficient funds: available {self.balances[src]}, requested {amount}"
            )
        txn_id = self.next_txn_id("TRF")
        self.post(
            txn_id,
            [(dst, "credit", amount), (src, "debit", amount)],
            memo,
        )
        return txn_id

    def reverse(self, original_txn_id: str, memo_suffix: str = "REVERSAL") -> str:
        """Reverse a posting: create a new transaction with every leg flipped to offset the original.

        Why not ``DELETE``? The ledger is an append-only record of facts, and
        deleting entries would break the audit chain. A rollback in a financial
        system is always a **compensating transaction**: the original and the
        reversal both stay in the ledger, so an audit can see the whole story of
        "posted wrongly, then reversed".
        """
        # Filter out the original transaction's legs with WHERE, flip the
        # directions, and post them again.
        self.client.use_table(LEDGER_TABLE)
        legs_rows = self.client.execute(
            """
            SELECT account_id, direction, amount
            FROM ledger
            WHERE txn_id = ?
            ORDER BY entry_id
            """,
            params=[original_txn_id],
        ).to_dict()
        assert legs_rows, f"original transaction {original_txn_id} not found"
        flipped = [
            (
                row["account_id"],
                "debit" if row["direction"] == "credit" else "credit",
                float(row["amount"]),
            )
            for row in legs_rows
        ]
        txn_id = self.next_txn_id("REV")
        self.post(txn_id, flipped, f"{memo_suffix} of {original_txn_id}")
        return txn_id


def point_lookup(client: ApexClient, acct_id: int) -> dict:
    """OLTP point lookup: fetch the whole account row by its ``_id`` primary key.

    ``retrieve()`` always acts on the **currently selected table**, so the accounts
    table is selected explicitly first. The ``_id == account_id`` assertion is
    deliberate: once that invariant breaks, the write order has changed and the
    point-lookup semantics no longer hold, so it must fail loudly instead of
    quietly returning the wrong row.
    """
    client.use_table(ACCOUNTS_TABLE)
    row = client.retrieve(acct_id)
    assert row is not None, f"account {acct_id} does not exist"
    assert row["_id"] == acct_id, f"internal _id {row['_id']} does not match account_id {acct_id}"
    assert row["account_id"] == acct_id, (
        f"account_id should be {acct_id}, got {row['account_id']}"
    )
    return row


def reconcile(client: ApexClient, balances: dict[int, float]) -> list[dict]:
    """End-of-day reconciliation: one SQL statement balances "balance table" against "ledger".

    ``SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END)`` is the
    standard accounting formulation: credits positive, debits negative, so the sum
    is the account's net movement. For every account that net movement must equal
    the materialised balance **exactly**.
    """
    client.use_table(ACCOUNTS_TABLE)
    return client.execute(
        """
        SELECT
            a.account_id                                        AS account_id,
            a.owner                                             AS owner,
            a.balance                                           AS stored_balance,
            ROUND(SUM(CASE WHEN l.direction = 'credit'
                           THEN l.amount ELSE -l.amount END), 2) AS ledger_net,
            COUNT(l.entry_id)                                   AS entries
        FROM accounts a
        JOIN ledger l ON a.account_id = l.account_id
        GROUP BY a.account_id, a.owner, a.balance
        ORDER BY a.account_id
        """
    ).to_dict()


def main() -> None:
    base = work_dir(SLUG)

    with ApexClient(os.path.join(base, "db")) as client:
        section("Step 1: open accounts (write the account table, opening balance 0)")
        open_accounts(client)
        client.use_table(ACCOUNTS_TABLE)
        show("account count", client.count_rows())
        show("account list", client.execute("SELECT * FROM accounts ORDER BY account_id").to_dict())

        # Ledger table: append-only, entry_id is the business key.
        create_ledger(client)
        client.use_table(ACCOUNTS_TABLE)
        book = Ledger(client)

        section("Step 2: point lookup (retrieve) verifying _id aligns with account_id")
        # The point lookup goes through the _id primary key, an O(1) row fetch --
        # this is the OLTP half of HTAP.
        row = point_lookup(client, 1)
        show("retrieve(1)", row)
        print("[OK] retrieve(1) hit exactly account_id = 1 (_id aligned with the business key)")

        section("Step 3: deposit / withdraw / transfer (every posting updates the materialised balance)")
        t_dep1 = book.deposit(1, 10_000.00, "initial funding")
        t_dep2 = book.deposit(2, 4_500.50, "initial funding")
        t_dep3 = book.deposit(3, 2_200.00, "initial funding")
        t_dep4 = book.deposit(4, 800.00, "initial funding")
        show("deposit transaction ids", [t_dep1, t_dep2, t_dep3, t_dep4])

        t_wdr = book.withdraw(4, 300.00, "supplier payment")
        show("withdrawal transaction id", t_wdr)

        t_trf = book.transfer(1, 3, 1_500.25, "internal settlement")
        show("transfer transaction id", t_trf)

        # Point lookups confirm the balances took effect immediately (they read
        # back exactly what the preceding replace wrote).
        # Note: book.post() leaves the ledger selected, so each lookup must select
        # the accounts table again first.
        for acct, _ in ACCOUNTS:
            row = point_lookup(client, acct)
            show(f"account {acct}({row['owner']}) live balance", row["balance"])
        assert_close(point_lookup(client, 1)["balance"], book.balances[1], 1e-9, "account 1 lookup balance")

        # Business-rule check: insufficient funds must be rejected and must leave
        # no ledger entry behind.
        entries_before = client.execute(
            "SELECT COUNT(*) AS n FROM ledger"
        ).scalar()
        try:
            book.withdraw(4, 999_999.00, "unauthorised payment")
        except ValueError as exc:
            show("insufficient funds rejected", str(exc))
        else:
            raise AssertionError("a withdrawal with insufficient funds should raise ValueError")
        entries_after = client.execute("SELECT COUNT(*) AS n FROM ledger").scalar()
        assert entries_before == entries_after, "a rejected transaction must not write any ledger entry"
        print("[OK] the over-limit withdrawal was rejected and the ledger entry count is unchanged (no dirty data)")

        section("Step 4: rollback part one -- business reversal (compensating transaction)")
        # Simulate a human error such as "the cashier entered 3000 instead of 300":
        # reverse the original transaction, then re-post it correctly.
        wrong_txn = book.withdraw(3, 3_000.00, "mistake: wrong amount entered")
        show("mistaken transaction id", wrong_txn)
        bal_after_wrong = point_lookup(client, 3)["balance"]
        show("account 3 balance after the mistake", bal_after_wrong)

        rev_txn = book.reverse(wrong_txn)
        bal_after_reverse = point_lookup(client, 3)["balance"]
        show("reversal transaction id / balance after reversal", (rev_txn, bal_after_reverse))
        # The reversal must restore the balance exactly to its pre-mistake state.
        assert_close(
            bal_after_reverse,
            round(bal_after_wrong + 3_000.00, 2),
            1e-9,
            "account 3 balance after the reversal",
        )

        section("Step 5: rollback part two -- SQL transaction BEGIN / ROLLBACK")
        # This demonstrates a storage-engine transaction: rows written inside it
        # are visible to the same connection and disappear completely after
        # ROLLBACK, never reaching the persistent file.
        #
        # Two measured behaviours (this example follows the supported forms so
        # readers avoid the traps):
        #   1. **Do not use COUNT(*) to test in-transaction visibility.** COUNT(*)
        #      takes the row-count metadata fast path and cannot see uncommitted
        #      rows; SELECT the target row explicitly instead.
        #   2. **Do not bind float columns with ? in an in-transaction INSERT.**
        #      After BEGIN, a float bound through ``VALUES (?, ...)`` is written
        #      as 0.0 (numeric literals behave normally). The amounts below are
        #      therefore inlined into the SQL -- all values are controlled by the
        #      code, so there is no injection risk; real systems should use
        #      parameters plus writes outside the transaction.
        client.execute("BEGIN")
        client.execute(
            "INSERT INTO ledger "
            "(entry_id, txn_id, account_id, direction, amount, day, memo) VALUES "
            f"(90001, 'T9001-TMP', 1, 'credit', 1.00, '{DAY}', 'in-transaction probe')"
        )
        inside = client.execute(
            "SELECT entry_id, amount, memo FROM ledger WHERE txn_id = ?",
            params=["T9001-TMP"],
        ).to_dict()
        show("visible row from an explicit SELECT inside the transaction", inside)
        assert len(inside) == 1, "the row should be visible inside the transaction"
        assert_close(inside[0]["amount"], 1.00, 1e-9, "amount literal written inside the transaction")
        client.execute("ROLLBACK")
        after = client.execute(
            "SELECT entry_id FROM ledger WHERE txn_id = ?", params=["T9001-TMP"]
        ).to_dict()
        show("query after ROLLBACK", after)
        assert after == [], "the row must disappear after ROLLBACK"
        print("[OK] BEGIN -> INSERT -> ROLLBACK: visible inside the transaction, gone after rollback")

        section("Step 6: rollback part three -- SQL transaction BEGIN / COMMIT")
        # An internal fee: 1.00 from account 1 to the external funding pool, with
        # both legs committed in the same transaction.
        client.execute("BEGIN")
        client.execute(
            "INSERT INTO ledger "
            "(entry_id, txn_id, account_id, direction, amount, day, memo) VALUES "
            f"(90002, 'T9002-CMT', {EXTERNAL_ACCOUNT}, 'debit', 1.00, '{DAY}', 'fee')"
        )
        client.execute(
            "INSERT INTO ledger "
            "(entry_id, txn_id, account_id, direction, amount, day, memo) VALUES "
            f"(90003, 'T9002-CMT', 1, 'credit', 1.00, '{DAY}', 'fee')"
        )
        client.execute("COMMIT")
        committed = client.execute(
            "SELECT entry_id, amount FROM ledger WHERE txn_id = ? ORDER BY entry_id",
            params=["T9002-CMT"],
        ).to_dict()
        show("rows visible after COMMIT", committed)
        assert len(committed) == 2, "both rows should be persisted after COMMIT"
        assert all(abs(r["amount"] - 1.00) < 1e-9 for r in committed), (
            "amounts must survive the commit intact (literal form)"
        )
        print("[OK] BEGIN -> two INSERTs -> COMMIT: both rows and their amounts persisted intact")
        # These two rows were inserted directly by the transaction, bypassing
        # Ledger.post, so the balance table was not updated automatically. To keep
        # the later reconciliation valid, apply the same movement to the
        # materialised balances here.
        book.balances[1] = round(book.balances[1] + 1.00, 2)
        book.balances[EXTERNAL_ACCOUNT] = round(book.balances[EXTERNAL_ACCOUNT] - 1.00, 2)
        client.use_table(ACCOUNTS_TABLE)
        for acct in (1, EXTERNAL_ACCOUNT):
            client.replace(
                acct,
                {
                    "account_id": acct,
                    "owner": OWNERS[acct],
                    "balance": book.balances[acct],
                    "status": "open",
                },
            )
        print("[OK] BEGIN -> two INSERTs -> COMMIT: both rows persisted")

        section("Step 7: reconciliation SQL -- ledger net movement vs materialised balance")
        client.use_table(ACCOUNTS_TABLE)
        recon = reconcile(client, book.balances)
        for row in recon:
            status = "OK" if abs(row["stored_balance"] - row["ledger_net"]) < 1e-9 else "MISMATCH"
            show(f"account {row['account_id']}({row['owner']}) {status}", row)

        assert len(recon) == len(ACCOUNTS), "every account should appear in the reconciliation result"
        for row in recon:
            assert_close(
                row["ledger_net"],
                row["stored_balance"],
                1e-9,
                f"account {row['account_id']} ledger net vs balance",
            )
            # Also verify the Python-side expectation matches the materialised
            # database value (three-way agreement).
            assert_close(
                row["stored_balance"],
                book.balances[row["account_id"]],
                1e-9,
                f"account {row['account_id']} database balance vs in-memory expectation",
            )

        section("Step 8: global debit/credit balance + conservation of funds")
        trial = client.execute(
            """
            SELECT
                ROUND(SUM(CASE WHEN direction = 'credit' THEN amount ELSE 0 END), 2) AS total_credit,
                ROUND(SUM(CASE WHEN direction = 'debit'  THEN amount ELSE 0 END), 2) AS total_debit,
                COUNT(*) AS entries,
                COUNT(DISTINCT txn_id) AS txns
            FROM ledger
            """
        ).to_dict()[0]
        show("trial balance", trial)
        assert_close(trial["total_credit"], trial["total_debit"], 1e-9, "global credits vs debits")
        print(
            f"[OK] global debit/credit balance: credits {trial['total_credit']} == debits {trial['total_debit']} "
            f"({trial['entries']} entries, {trial['txns']} transactions)"
        )

        # A corollary of double-entry bookkeeping: counting the external funding
        # pool as well, all account balances sum to exactly 0.
        net_zero = client.execute("SELECT ROUND(SUM(balance), 2) AS s FROM accounts").scalar()
        show("sum of all account balances (including the external pool)", net_zero)
        assert_close(net_zero, 0.0, 1e-9, "sum of all account balances under double-entry bookkeeping")

        section("Step 9: aggregate by transaction to see the flow of funds (OLAP drill-down)")
        # Order by the real column ``txn_id``: it embeds a zero-padded sequence
        # number, so this yields a stable, reproducible transaction order for the
        # flow report instead of relying on engine-default tie ordering.
        flow = client.execute(
            """
            SELECT
                l.txn_id                                  AS txn_id,
                MIN(l.entry_id)                           AS first_entry,
                l.day                                     AS day,
                MIN(l.memo)                               AS memo,
                COUNT(*)                                  AS legs,
                ROUND(SUM(CASE WHEN l.direction = 'credit'
                               THEN l.amount ELSE 0 END), 2)  AS credit,
                ROUND(SUM(CASE WHEN l.direction = 'debit'
                               THEN l.amount ELSE 0 END), 2)  AS debit
            FROM ledger l
            GROUP BY l.txn_id, l.day
            ORDER BY l.txn_id
            """
        ).to_dict()
        for row in flow:
            show(f"transaction {row['txn_id']}", row)
        # Every transaction must also balance on its own -- the ledger's strongest invariant.
        for row in flow:
            assert_close(row["credit"], row["debit"], 1e-9, f"transaction {row['txn_id']} balance")
        print(f"[OK] all {len(flow)} transactions balance individually")

        section("Step 10: convert results to Pandas for an audit report")
        df = client.execute(
            """
            SELECT
                l.account_id                              AS account_id,
                a.owner                                   AS owner,
                ROUND(SUM(CASE WHEN l.direction = 'credit'
                               THEN l.amount ELSE 0 END), 2) AS total_credit,
                ROUND(SUM(CASE WHEN l.direction = 'debit'
                               THEN l.amount ELSE 0 END), 2) AS total_debit,
                a.balance                                 AS balance
            FROM ledger l
            JOIN accounts a ON a.account_id = l.account_id
            GROUP BY l.account_id, a.owner, a.balance
            ORDER BY l.account_id
            """
        ).to_pandas()
        print(df.to_string(index=False))
        show("DataFrame shape", df.shape)
        assert (df["total_credit"] >= 0).all() and (df["total_debit"] >= 0).all()

        print(f"\n=== Scenario 03 transaction ledger + point lookups (HTAP) done ===\ndatabase file under: {base}")


if __name__ == "__main__":
    main()
