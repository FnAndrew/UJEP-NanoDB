from datetime import date
from decimal import Decimal

from nanodb import Column, ColumnType, DataType, Table
from simple_table import SimpleTable


def main() -> None:
    """Create sample tables, insert data, and demonstrate inner join."""
    customer = Table(
        "customer",
        [
            Column("id", ColumnType(DataType.INT, not_null=True, unique=True)),
            Column("name", ColumnType(DataType.TEXT, not_null=True)),
            Column("birth_date", ColumnType(DataType.DATE)),
        ],
        primary_key=("id",),
    )

    # Vytvoření nové tabulky pomocí konstruktoru Table z nanodb
    # název je "order", a přebírá seznam sloupečků třídy Column
    # každý sloupeček má název, typ a případně další vlastnosti (not_null, unique)
    # tabulce se definuje primární klíč pomocí parametru primary_key, který je n-ticí názvů sloupečků tvořících primární klíč
    order_tbl = Table(
        "order",
        [
            Column("id", ColumnType(DataType.INT, not_null=True, unique=True)),
            Column("customer_id", ColumnType(DataType.INT)),
            Column("total", ColumnType(DataType.DECIMAL, not_null=True)),
            Column("created", ColumnType(DataType.DATE, not_null=True)),
        ],
        primary_key=("id",),
    )

    # vložení dat do tabulky customer
    # metoda insert přijímá n-tici s názvy sloupečků a s hodnotami pro ně
    # hodnoty musí být ve správném formátu a pořadí
    customer.insert(
        ("id", "name", "birth_date"),
        (1, "Alice", date(1995, 5, 17)),
    )
    customer.insert(
        ("id", "name", "birth_date"),
        (2, "Bob", None), # None lze použít pouze u sloupečků, kde není nastaveno not_null=True
    )
    customer.insert(
        ("id", "name", "birth_date"),
        (3, "Cyril", date(2001, 1, 10)),
    )

    order_tbl.insert(
        ("id", "customer_id", "total", "created"),
        (100, 1, Decimal("120.50"), date(2026, 3, 1)),
    )
    order_tbl.insert(
        ("id", "customer_id", "total", "created"),
        (101, 1, Decimal("75.00"), date(2026, 3, 2)),
    )
    order_tbl.insert(
        ("id", "customer_id", "total", "created"),
        (102, 2, Decimal("33.30"), date(2026, 3, 3)),
    )
    order_tbl.insert(
        ("id", "customer_id", "total", "created"),
        (103, None, Decimal("9.99"), date(2026, 3, 4)),
    )
    order_tbl.insert(
        ("id", "customer_id", "total", "created"),
        (104, 999, Decimal("15.00"), date(2026, 3, 5)),
    )

    print("CUSTOMER")
    print(customer.to_text())
    print()

    print("ORDER")
    print(order_tbl.to_text())
    print()

    joined = customer.inner_join(order_tbl, ("customer_id",))

    print("INNER JOIN: customer ⨝ order")
    print(joined.to_text())
    print()

    print("Rows in customer:", len(customer))
    print("Rows in order:", len(order_tbl))
    print("Rows in join:", len(joined))
    print()

    print("Iterating over joined rows:")
    for row in joined:
        print(row)

    ac = customer.where(lambda row: row["name"].startswith("A"))
    print(ac.to_text())

def main_store():
    """Demostrace užití NanoDB pro obchod"""

    class Store():
        def __init__(self, name: str):
            self._name = name

            self.customers = SimpleTable("customers", ["name", "email"])
            self.orders = SimpleTable("orders", ["customer_id", "value"])
        
        def add_customer(self, name: str, email: str) -> bool:
            return self.customers.add_row(name, email)
        
        def get_customer_id(self, email: str):
            rows = list(self.customers.iter_rows_as_dict())
            my_customer = [c for c in rows if c.get("email") == email][0]
            return my_customer.get("_id")

            # cw = self.customers.where(lambda row: row["email"] == email)

        def add_order(self, customer_email: str, value: str) -> bool:
            c_id = self.get_customer_id(customer_email)
            return self.orders.add_row(str(c_id), value)
        
        def print_customer_orders(self, email: str):
            joined = self.customers.inner_join(self.orders, ["customer_id"])
            joined.where(lambda row: row["email"] == email)
            print(joined)

        def print(self):
            print(self.customers.to_text())
            print(self.orders.to_text())

    my_store = Store("U Vočka")
    my_store.add_customer("Bart", "bart@simpson.bbc")
    my_store.add_customer("Lísa", "lisa@simpson.bbc")
    # my_store.get_customer_id("bart@simpson.bbc")
    my_store.add_order("bart@simpson.bbc", '15.5')
    
    # my_store.print()
    my_store.print_customer_orders("bart@simpson.bbc")



if __name__ == "__main__":
    main_store()