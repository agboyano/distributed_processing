import argparse
import datetime
from math import exp, sqrt
import numpy as np
from distributed_processing.serializers import JsonSerializer
from distributed_processing.worker import Worker
from distributed_processing.redis_connector import RedisConnector

# prueba
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
NAMESPACE = "montecarlo"


def new_price(S_t, v, r, t1, t2):
    T = (t2 - t1) / 365.0
    # no hay diferencia con gauss
    return S_t * exp((r - 0.5 * v * v) * T + v * sqrt(T) * np.random.randn())


def df(r, t1, t2):
    return exp(-r * (t2 - t1) / 365.0)


def mc_autocall_mp(simulations,
                   v,
                   r,
                   coupon_barrier,
                   kickout_barrier,
                   protection_barrier,
                   coupon_rate,
                   dates):

    simulations = int(simulations)  # Cuidado con los enteros en json son todo flotantes

    assert(coupon_barrier >= protection_barrier)

    S = 1.0
    C = 1000.0

    start_date = dates[0]

    autocall_prices = [0.0] * simulations  # Cuidado con los enteros en json son todo flotantes

    coupons_discounted = [C * coupon_rate * df(r, start_date, dates[j]) for j in range(len(dates))]
    principal_discounted = [C * df(r, start_date, dates[j]) for j in range(len(dates))]

    def p_exp(t1, t2):
        T = (t2 - t1) / 365.0
        return ((r - 0.5 * v**2) * T, v * sqrt(T))

    partial_exp = [p_exp(dates[j], dates[j+1]) for j in range(len(dates)-1)]

    rnd = np.random.RandomState().randn(simulations, len(dates)-1)

    # muy importante RandomState() para multiproceso ya que reinicializa la semilla

    for i in range(simulations):
        autocall_price = 0.0

        for j in range(1, len(dates)):
            S = S * exp(partial_exp[j-1][0] + partial_exp[j-1][1] * rnd[i, j-1])

            if S > coupon_barrier:
                autocall_price += coupons_discounted[j]

            if S >= kickout_barrier:
                break

        if S < protection_barrier:
            autocall_price += S * principal_discounted[j]

        else:
            autocall_price += principal_discounted[j]

        autocall_prices[i] = autocall_price

    return sum(autocall_prices), float(simulations)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default=REDIS_HOST, help="Redis server address")
    parser.add_argument('-p', '--port', type=int, default=REDIS_PORT, help="Redis server port")
    parser.add_argument('-db', type=int, default=REDIS_DB, help="Redis server DB")
    parser.add_argument('-n', '--namespace', type=str, default=NAMESPACE, help="Namespace to use")
    parser.add_argument('--clean', action='store_true', help="Clean namespace")

    args = parser.parse_args()

    print(
        f"Connecting to Redis server in {args.host}: {args.port}, DB {args.db} and Namespace {args.namespace}")

    redis_connector = RedisConnector(redis_host=args.host, redis_port=args.port,
                                     redis_db=args.db, namespace=args.namespace)

    if args.clean:
        print(f"Cleaning namespace {args.namespace}")
        redis_connector.clean_namespace()

    server = Worker(JsonSerializer(), redis_connector)

    server.add_requests_queue("cola_1", {"mc_autocall_mp": mc_autocall_mp})
    server.update_methods_registry()

    server.run()
