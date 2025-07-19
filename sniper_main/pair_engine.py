# -*- coding: utf-8 -*-
"""
pair_engine – parowanie dwóch niezależnych OW w pseudo-RT.
Warunek STEAL-pair (strict):
  price_out <= avg30(out) * (1 - threshold)
  AND
  price_in  <= avg30(in)  * (1 - threshold)
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import List

from .config import Config
from .db import find_returns, get_last_30d_avg, insert_pair
from .models import FlightOffer

CFG = Config.from_json()

logger = logging.getLogger(__name__)


def process_outbound(out_offer: FlightOffer, out_id: int) -> List[int]:
    """Buduje pary dla jednego nowego biletu OW; zwraca listę id par STEAL."""
    if not CFG.combine_ow:
        return []

    steals_created: List[int] = []

    window_start = out_offer.depart_date + timedelta(days=CFG.min_trip_days)
    window_end = out_offer.depart_date + timedelta(days=CFG.max_trip_days)

    returns = find_returns(
        out_offer_id=out_id,
        dest=out_offer.destination,
        orig=out_offer.origin,
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        max_stops=CFG.max_stops,
    )

    base_thr = (
        CFG.pair_steal_threshold
        if CFG.pair_steal_threshold is not None
        else CFG.steal_threshold
    )

    for ret in returns:
        ret_id, price_in, ret_date = ret
        price_out = out_offer.price_pln

        avg_out = get_last_30d_avg(out_offer.origin, out_offer.destination)
        avg_in = get_last_30d_avg(out_offer.destination, out_offer.origin)

        # Brak historii -> nie uznajemy za STEAL
        if not avg_out or not avg_in:
            steal = False
        else:
            limit_out = avg_out * (1 - base_thr)
            limit_in = avg_in * (1 - base_thr)
            steal = (price_out <= limit_out) and (price_in <= limit_in)

        pair_id = insert_pair(
            out_id=out_id,
            in_id=ret_id,
            price_total=price_out + price_in,
            origin=out_offer.origin,
            dest=out_offer.destination,
            depart=out_offer.depart_date.isoformat(),
            ret=ret_date,
            steal=steal,
        )
        if pair_id != -1:
            logger.debug(
                "PAIR %s-%s %s→%s total=%.0f steal=%s",
                out_offer.origin,
                out_offer.destination,
                out_offer.depart_date,
                ret_date,
                price_out + price_in,
                steal,
            )

        if steal and pair_id != -1 and CFG.alert_pair and CFG.telegram_instant:
            from .notifier import send_telegram

            msg = (
                "💥 STEAL PAIR\n"
                f"{out_offer.origin}→{out_offer.destination} {out_offer.depart_date}  "
                f"{out_offer.destination}→{out_offer.origin} {ret_date}\n"
                f"OUT {price_out:.0f} PLN | IN {price_in:.0f} PLN | TOTAL {(price_out+price_in):.0f} PLN"
            )
            send_telegram(msg)
            steals_created.append(pair_id)

    return steals_created
