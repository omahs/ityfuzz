import peewee
import math
from peewee import fn
import subprocess


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

    res0 = peewee.TextField()
    res1 = peewee.TextField()

    class Meta:
        database = db
        indexes = (
            (('token0',), False),
            (('token1',), False),
        )
        table_name = SRC


def get_query():
    return Uniswap.select().where(
        (
            (Uniswap.token0 == "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".lower()) & (20 < fn.LENGTH(Uniswap.res0))
        )
        # (Uniswap.token0_decimals < fn.LENGTH(Uniswap.res0) | Uniswap.token0 != "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".lower()) &
        # (Uniswap.token1_decimals < fn.LENGTH(Uniswap.res1) | Uniswap.token1 != "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".lower())
    )

def fetch_chunk(chunk_size=100):
    tt = get_query().count()
    total_pages = math.ceil(tt / chunk_size)
    print(total_pages)

    for page in range(1, total_pages + 1):
        cc = get_query().paginate(page, chunk_size)
        for c in cc:
            yield c


def clip(content):
    if len(content) > 10000:
        return content[-9999:]
    return content

import web3

w3 = web3.Web3(web3.HTTPProvider('http://10.0.0.71'))

import retry

@retry.retry(tries=3, delay=0.5)
def get_block():
    return w3.eth.block_number - 5

def run(data):
    pair = data.pair
    blk = get_block()
    cmd = f"./target/release/cli -o -t 0x10ED43C718714eb63d5aA57B78B54704E256024E,{pair} -c BSC --onchain-block-number {blk} -f -i --onchain-local-proxy-addr http://localhost:5003"
    cmd = f'timeout 30m {cmd}'
    print(pair, cmd)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    print("exit code: ", process.returncode)
    # print("stderr: ", stderr.decode('utf-8'))

    exec_sec = []
    for i in stdout.decode('utf-8').split('\n'):
        if 'exec/sec: ' in i:
            exec_sec.append(int(i.split('exec/sec: ')[1].split(' ')[0]))

    if len(exec_sec) != 0:
        mean_exec_sec = sum(exec_sec) / len(exec_sec)
        if mean_exec_sec < 500:
            with open("slow.txt", "a") as f:
                f.write('------------------------\n')
                # f.write(cmd + '\n')
                f.write(target + '\n')
                f.write(clip(stdout.decode('utf-8')) + '\n')
                f.write('------------------------\n')

    if "Found a solution" in stdout.decode('utf-8'):
        with open("solution.txt", "a") as f:
            f.write('------------------------\n')
            # f.write(cmd + '\n')
            f.write(target + '\n')
            f.write(clip(stdout.decode('utf-8')) + '\n')
            f.write('------------------------\n')

    if "`RUST_BACKTRACE=1`" in stderr.decode('utf-8'):
        with open("crash.txt", "a") as f:
            f.write('------------------------\n')
            # f.write(cmd + '\n')
            f.write(target + '\n')
            f.write(clip(stderr.decode('utf-8')) + '\n')
            f.write('------------------------\n')

import multiprocessing


if __name__ == "__main__":
    with multiprocessing.Pool(2) as p:
        p.map(run, fetch_chunk())
