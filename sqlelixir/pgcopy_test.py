from datetime import date, datetime
from enum import Enum
from io import StringIO


from attrs import define
from sqlelixir import pgcopy


class Parity(Enum):
    EVEN = "even"
    ODD = "odd"


@define
class Item:
    logical: bool | None
    integer: int | None
    floating: float | None
    day: date | None
    moment: datetime | None
    parity: Parity | None


def test():
    now = datetime.utcnow()
    today = now.date()

    input = [
        Item(True, 1, 1.5, today, now, Parity.EVEN),
        Item(False, -1, -1.5, date.min, datetime.min, Parity.ODD),
        Item(None, None, None, None, None, None),
    ]

    with StringIO() as fp:
        pgcopy.dump(fp, input)
        fp.seek(0)
        output = list(pgcopy.load(fp, Item))
        assert output == input
