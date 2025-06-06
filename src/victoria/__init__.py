import json
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass
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
    time: datetime|None = None
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
    await Victoria().push_points([p for r in results for p in result_to_points(r)])


def result_to_points(r: Result) -> Generator[Point]:
    yield result_to_point('quote_amount_out', r.amount_out, r)
    yield result_to_point('quote_elapsed', r.elapsed, r)
    if r.btc_fee != 0:
        yield result_to_point('quote_btc_fee', r.btc_fee, r)
    if r.amount_out_min != 0:
        yield result_to_point('quote_amount_out_min', r.amount_out_min, r)


def result_to_point(metric: str, value: float, r: Result) -> Point:
    assert(r.time is not None)
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
    def __init__(self, limit: int = 1000, url: str|None = None):
        if url is None:
            url = os.getenv('VICTORIAMETRICS_URL')
            assert(url is not None)
        self.url = url
        self.points = []
        self.limit = limit
        self.t = tqdm(total=self.limit, desc="Victoria buffer")

    async def push(self, point: Point):
        self.points.append(point)
        self.t.update(1)
        if len(self.points) == self.limit:
            await self.push_points(self.points)
            self.points = []
            self.t.reset()

    async def push_points(self, points: list[Point]):
        logging.debug("pushing to victoria %d points", len(points))
        data = '\n'.join(json.dumps(point_to_victoria(p)) for p in points)
        async with httpx.AsyncClient() as client:
            resp = await client.post(self.url+"/import", data=data)
            resp.raise_for_status()

    @asynccontextmanager
    @staticmethod
    async def use(limit: int = 1000, url: str|None = None):
        v = Victoria(limit, url)
        yield v
        await v.push_points(v.points)
