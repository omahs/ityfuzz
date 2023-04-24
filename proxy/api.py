import sys
import flask
import peewee
from peewee import fn
import flask
from retry import retry
from ratelimit import limits
import time
import functools
import requests

app = flask.Flask(__name__)

# Connect to the database
db = peewee.MySQLDatabase(
     'pairs',
     host='10.0.0.71',
     port=3306,
     user='shou',
     password='Shou@123',
)

SRC = "pancake"

class Uniswap(peewee.Model):
    token0 = peewee.CharField()
    token1 = peewee.CharField()
    pair = peewee.CharField()
    idx = peewee.IntegerField()
    token0_decimals = peewee.IntegerField()
    token1_decimals = peewee.IntegerField()

    class Meta:
        database = db
        indexes = (
            (('token0',), False),
            (('token1',), False),
        )
        table_name = SRC



class UpdatesInfo(peewee.Model):
    src = peewee.CharField()
    block = peewee.IntegerField()

    class Meta:
        database = db


def get_rpc(network):
    if network == "eth":
        return "https://white-dry-replica.quiknode.pro/0864a1bd29d3befccdf5cb38a2ed100f7852ca59/" #"https://eth.llamarpc.com"
    elif network == "bsc":
        # BSC mod to geth make it no longer possible to use debug_storageRangeAt
        # so, we use our own node that supports eth_getStorageAll
        # return "https://blue-damp-glitter.bsc.discover.quiknode.pro/8364ed151b17ed4619e9effc6237600241c2e65c/"
        return "http://10.0.0.71/" # "http://bsc.node1.infra.fuzz.land:4949"
    elif network == "polygon":
        return "https://polygon-rpc.com/"
    elif network == "mumbai":
        return "https://rpc-mumbai.maticvigil.com"
    else:
        raise Exception("Unknown network")

def get_pegged_token(network):
    if network == "eth":
        return {
            "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "DAI": "0x6b175474e89094c44da98b954eedeac495271d0f",
            "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
            "WMATIC": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",
        }
    elif network == "bsc":
        return {
            "WBNB": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
            "USDC": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
            "USDT": "0x55d398326f99059ff775485246999027b3197955",
            "DAI": "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
            "WBTC": "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",
            "WETH": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
            "BUSD": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
            "CAKE": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
        }
    elif network == "polygon":
        return {
            "WMATIC": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
            "USDC": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            "USDT": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
            "DAI": "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
            "WBTC": "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6",
            "WETH": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
        }
    elif network == "mumbai":
        raise Exception("Not supported")
    else:
        raise Exception("Unknown network")


def get_weth(network):
    t = get_pegged_token(network)
    if network == "eth":
        return t["WETH"]
    elif network == "bsc":
        return t["WBNB"]
    elif network == "polygon":
        return t["WMATIC"]
    elif network == "mumbai":
        raise Exception("Not supported")
    else:
        raise Exception("Unknown network")


def get_router(network, source):
    if network == "eth":
        if source == "uniswapv2":
            return "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
        elif source == "uniswapv3":
            return "0xe592427a0aece92de3edee1f18e0157c05861564"
        else:
            raise Exception("Unknown source")
    elif network == "bsc":
        if source == "pancakeswap":
            return "0x05ff2b0db69458a0750badebc4f9e13add608c7f"
        elif source == "biswap":
            return "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506"
        else:
            raise Exception("Unknown source")
    elif network == "polygon":
        if source == "uniswapv3":
            return "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506"
        else:
            raise Exception("Unknown source")
    elif network == "mumbai":
        raise Exception("Not supported")


def get_token_name_from_address(network, address):
    data = get_pegged_token(network)

    for k, v in data.items():
        if v == address:
            return k
    raise Exception("Unknown token address")


@functools.lru_cache(maxsize=1000)
def fetch_reserve(pair, network, block):
    url = f"{get_rpc(network)}"
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": pair,
            "data": "0x0902f1ac"
        }, block],
        "id": 1
    }
    print(pair)

    response = requests.post(url, json=payload)
    response.raise_for_status()
    result = response.json()["result"]

    return result[2:66], result[66:130]


def fetch_balance(token, address, network, block):
    url = f"{get_rpc(network)}"
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": token,
            "data": "0x70a08231000000000000000000000000" + address[2:]
        }, block],
        "id": 1
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    result = response.json()["result"]
    return result


def get_latest_block(network):
    url = f"{get_rpc(network)}"
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["result"]


def get_pair(token, network, block):
    # -50 account for delay in api indexing
    block_int = int(block, 16) if block != "latest" else int(get_latest_block(network), 16) - 50
    next_tokens = []
    # find all pairs given token
    pairs = Uniswap.select().where(((Uniswap.token0) == token) | ((Uniswap.token1) == token)).limit(20)
    for pair in pairs:
        next_tokens.append({
            "src": "v2",
            "in": 0 if pair.token0 == token else 1,
            "pair": pair.pair,
            "next": pair.token0 if pair.token0 != token else pair.token1,
            "decimals0": pair.token0_decimals,
            "decimals1": pair.token1_decimals,
            "src_exact": SRC,
        })
                
    return next_tokens


@functools.lru_cache(maxsize=1000)
def get_pair_pegged(token, network, block):
    # -50 account for delay in api indexing
    block_int = int(block, 16) if block != "latest" else int(get_latest_block(network), 16) - 50
    next_tokens = []

    pairs = (Uniswap
                 .select()
                 .where(
                     (((Uniswap.token0) == token) & ((Uniswap.token1) == get_weth(network))) |
                     (((Uniswap.token0) == get_weth(network)) & ((Uniswap.token1) == token))
                 ).limit(20))
    for pair in pairs:
        next_tokens.append({
            "in": 0 if pair.token0 == token else 1,
            "pair": pair.pair,
            "next": pair.token0 if pair.token0 != token else pair.token1,
            "decimals0": pair.token0_decimals,
            "decimals1": pair.token1_decimals,
            "src_exact": SRC,
        })
    return next_tokens


# max 1 hops
MAX_HOPS = 0

def add_info(pair_data, network, block):
    if "src" in pair_data and pair_data["src"] == "pegged_weth":
        return
    reserves = fetch_reserve(pair_data["pair"], network, block)
    pair_data["initial_reserves_0"] = reserves[0]
    pair_data["initial_reserves_1"] = reserves[1]


def get_all_hops(token, network, block, hop=0, known=set()):
    known.add(token)
    if hop > MAX_HOPS:
        return {}
    hops = {}
    hops[token] = get_pair(token, network, block)
    print(hops, hops[token])

    for i in hops[token]:
        if i["next"] in get_pegged_token(network).values():
            continue
        if i["next"] in known:
            continue
        hops = {**hops, **get_all_hops(i["next"], network, block, hop + 1, known)}
    return hops


def scale(price, decimals):
    # scale price to 18 decimals
    price = int(price, 16)
    if int(decimals) > 18:
        return float(price) / (10 ** (int(decimals) - 18))
    else:
        return float(price) * (10 ** (18 - int(decimals)))


@functools.lru_cache(maxsize=1000)
def get_pegged_next_hop(token, network, block):
    if token == get_weth(network):
        return {"src": "pegged_weth", "rate": int(1e6), "token": token}
    peg_info = get_pair_pegged(token, network, block)[0]
    # calculate price using reserves
    src = peg_info["in"]
    add_info(peg_info, network, block)
    p0 = int(peg_info["initial_reserves_0"], 16)
    p1 = int(peg_info["initial_reserves_1"], 16)
    if src == 0:
        peg_info["rate"] = int(p1 / p0 * 1e6)
    else:
        peg_info["rate"] = int(p0 / p1 * 1e6)
    return {"src": "pegged", **peg_info}


def with_info(routes, network, token):
    return {
        "routes": routes,
        "basic_info": {
            "weth": get_weth(network),
            "is_weth": token == get_weth(network),
        },
    }


def find_path_subgraph(network, token, block):
    if token in get_pegged_token(network).values():
        return with_info([[get_pegged_next_hop(token, network, block)]], network, token)
    hops = get_all_hops(token, network, block, known=set())
    print("init!")
    routes = []

    # do a DFS to find all routes
    def dfs(token, path, visited):
        if token in get_pegged_token(network).values():
            routes.append(path + [get_pegged_next_hop(token, network, block)])
            return
        visited.add(token)
        if token not in hops:
            return
        for i in hops[token]:
            if i["next"] in visited:
                continue
            dfs(i["next"], path + [i], visited.copy())

    dfs(token, [], set())

    print("dfs ok!")

    for route in routes:
        for p in route:
            print(p)
            add_info(p, network, block)

    return with_info(routes, network, token)



@app.route("/swap_path/<network>/<token_address>/<block>", methods=["GET"])
def swap_path(network, token_address, block):
    return flask.jsonify(find_path_subgraph(network, token_address.lower(), block))


if __name__ == "__main__":
    app.run(port=5004)
