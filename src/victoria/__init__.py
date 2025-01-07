import time
import json
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass, field, asdict
from typing import Generator

import httpx
from tqdm import tqdm


@dataclass
class Result:
    token_in: str
    token_out: str
    amount_in: Decimal
    provider: str
    elapsed: float
    time: datetime = None
    amount_out: int = 0
    amount_out_min: int = 0
    error: Exception|str = ''
    btc_fee: int = 0


@dataclass
class Point:
    time: datetime
    name: str
    value: float
    labels: dict[str, str]


async def push_results_to_victoria(results: list[Result]):
    await push_to_victoria([p for r in results for p in result_to_points(r)])


async def push_to_victoria(points: list[Point]):
    if os.getenv('SYMBMON_PRINT_POINTS'):
        for p in points:
            print(p)
    logging.debug("pushing to victoria %d points", len(points))
    data = '\n'.join(json.dumps(point_to_victoria(p)) for p in points)
    url = os.getenv('VICTORIAMETRICS_URL')
    assert(url is not None)
    async with httpx.AsyncClient() as client:
        resp = await client.post(url+"/import", data=data)
        resp.raise_for_status()


def result_to_points(r: Result) -> Generator[Point]:
    yield result_to_point('quote_amount_out', r.amount_out, r)
    yield result_to_point('quote_elapsed', r.elapsed, r)
    if r.btc_fee != 0:
        yield result_to_point('quote_btc_fee', r.btc_fee, r)
    if r.amount_out_min != 0:
        yield result_to_point('quote_amount_out_min', r.amount_out_min, r)


def result_to_point(metric: str, value: float, r: Result) -> Point:
    return Point(
        name=metric,
        time=r.time,
        value=float(value),
        labels=result_to_labels(r),
    )


def point_to_victoria(p: Point) -> dict:
    return dict(
        metric=dict(
            __name__=p.name,
            **p.labels,
        ),
        values=[p.value],
        timestamps=[int(p.time.timestamp() * 1000)],
    )


def result_to_labels(r: Result):
    return {
        'amount_in': str(r.amount_in),
        'token_in': r.token_in,
        'token_out': r.token_out,
        'provider': r.provider,
    }


class Victoria:
    def __init__(self, limit):
        self.points = []
        self.limit = limit
        self.t = tqdm(total=self.limit, desc="Victoria buffer")

    async def push(self, point: Point):
        self.points.append(point)
        self.t.update(1)
        if len(self.points) == self.limit:
            await push_to_victoria(self.points)
            self.points = []
            self.t.reset()

    @asynccontextmanager
    @staticmethod
    async def use(limit=1000):
        v = Victoria(limit)
        yield v
        await push_to_victoria(v.points)
