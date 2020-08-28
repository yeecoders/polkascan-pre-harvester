#  Polkascan PRE Harvester
#
#  Copyright 2018-2019 openAware BV (NL).
#  This file is part of Polkascan.
#
#  Polkascan is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Polkascan is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Polkascan. If not, see <http://www.gnu.org/licenses/>.
#
#  tasks.py
import celery
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker, scoped_session
from app.models.data import Extrinsic, Block, Log, Event
from app.processors.converters import PolkascanHarvesterService, HarvesterCouldNotAddBlock, \
    HarvesterNotshardParamsError, BlockAlreadyAdded
from substrateinterface import SubstrateInterface

from app.settings import DB_CONNECTION, DEBUG, TYPE_REGISTRY, SHARDS_TABLE, NUM

CELERY_BROKER = 'redis://redis:6379/0'
CELERY_BACKEND = 'redis://redis:6379/0'

app = celery.Celery('tasks', broker=CELERY_BROKER, backend=CELERY_BACKEND)

app.conf.beat_schedule = {
    'shard0-check-head-10-seconds': {
        'task': 'app.tasks.start_harvester',
        'schedule': 13.0,
        'args': ("shard.0",)
    },
    'shard1-check-head-10-seconds': {
        'task': 'app.tasks.start_harvester',
        'schedule': 13.0,
        'args': ("shard.1",)
    },
    'shard2-check-head-10-seconds': {
        'task': 'app.tasks.start_harvester',
        'schedule': 13.0,
        'args': ("shard.2",)
    },
    'shard3-check-head-10-seconds': {
        'task': 'app.tasks.start_harvester',
        'schedule': 13.0,
        'args': ("shard.3",)
    },
    # 'start_init-5-seconds': {
    #     'task': 'app.tasks.start_init',
    #     'schedule': 6.0,
    #     'args': ()
    # },
}
#
# app.conf.beat_schedule = {
#     'shard0-check-head-10-seconds': {
#         'task': 'app.tasks.start_harvester',
#         'schedule': 50.0,
#         'args': ("shard.0",)
#     },
#     #     'start_sequencer-500-seconds': {
#     #         'task': 'app.tasks.start_sequencer',
#     #         'schedule': 50.0,
#     #         'args': ()
#     #     },
# }

app.conf.timezone = 'UTC'


class BaseTask(celery.Task):

    def __init__(self):
        self.metadata_store = {}
        self.init = False

    def __call__(self, *args, **kwargs):
        self.engine = create_engine(DB_CONNECTION, echo=DEBUG, isolation_level="READ_UNCOMMITTED")
        session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
        self.session = scoped_session(session_factory)

        return super().__call__(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        if hasattr(self, 'session'):
            self.session.remove()
        if hasattr(self, 'engine'):
            self.engine.engine.dispose()


@app.task(base=BaseTask, bind=True)
def accumulate_block_recursive(self, block_hash, end_block_hash=None, substrate_url=None):
    print('start accumulate_block_recursive block_hash {} =='.format(block_hash))
    print('start accumulate_block_recursive substrate_url {} =='.format(substrate_url))

    harvester = PolkascanHarvesterService(self.session, type_registry=TYPE_REGISTRY)
    harvester.metadata_store = self.metadata_store

    # If metadata store isn't initialized yet, perform some tests
    if not substrate_url:
        return
        # shard_num = NUM[substrate_url]
        # print('start accumulate_block_recursive shard_num {} =='.format(shard_num))

    substrate = SubstrateInterface(substrate_url)
    block_nr = substrate.get_block_number(block_hash)

    block = None
    max_sequenced_block_id = False
    block_one = None
    add_count = 0

    try:
        for nr in range(0, block_nr + 1):
            if not block or block.bid > 0:
                # Process block
                blocka = harvester.add_block(block_hash, substrate_url)
                if blocka:
                    print('+ Added {} '.format(block_hash))
                    add_count += 1
                    self.session.commit()
                    block = blocka
                # Break loop if targeted end block hash is reached
                if block_hash == end_block_hash or block.bid == 0:
                    block_one = block
                    break
                # Continue with parent block hash
                block_hash = block.parent_hash

        # Update persistent metadata store in Celery task
        self.metadata_store = harvester.metadata_store
        harvester.process_shard_genesis(block_one, substrate_url)

        # if block_hash != end_block_hash and block and block.bid > 0:
        #     accumulate_block_recursive.delay(block.parent_hash, end_block_hash)

    except BlockAlreadyAdded as e:
        print('. Skipped {} '.format(block_hash))
    except IntegrityError as e:
        print('. Skipped duplicate {}=={} '.format(block_hash, e))
    except Exception as exc:
        print('! ERROR adding {}'.format(block_hash))
        raise HarvesterCouldNotAddBlock(block_hash) from exc

    return {
        'result': '{} blocks added'.format(add_count),
        'lastAddedBlockHash': block_hash,
        'sequencerStartedFrom': max_sequenced_block_id
    }


@app.task(base=BaseTask, bind=True)
def start_harvester(self, check_gaps=False, shard=None):
    shard = self.request.args[0]
    if shard is None:
        raise HarvesterNotshardParamsError('params shard is missing.. stopping harvester ')

    print("start_harvester")
    substrate_url = SHARDS_TABLE[shard]
    print('== start_harvester substrate_url {} =='.format(substrate_url))
    substrate = SubstrateInterface(substrate_url)

    n = Block.query(self.session).filter_by(bid=1).count()

    if n < 4:
        print('waiting init task completed! count().n: {} '.format(n))

        return {'result': 'waiting init task completed! '}

    block_sets = []

    harvester = PolkascanHarvesterService(self.session, type_registry=TYPE_REGISTRY)
    harvester.metadata_store = self.metadata_store

    start_block_hash = substrate.get_chain_head()
    end_block_hash = None
    r = 10
    block_nr = substrate.get_block_number(start_block_hash)

    max_block = Block.query(self.session).filter_by(shard_num=shard.split(".")[1]).order_by(Block.bid.desc()).first()

    print('start block_nr  {} =='.format(block_nr))
    print('start max_block  {} =='.format(max_block.bid))
    if block_nr - max_block.bid < 10:
        r = block_nr - max_block.bid

    print('current range r: {} =='.format(max_block.bid))

    try:
        for nr in range(1, r + 1):
            block_hash = substrate.get_block_hash(max_block.bid + nr)

            if harvester.add_block(block_hash, substrate_url):
                print('start_harvester+ Added {} '.format(block_hash))
                self.session.commit()

        # Update persistent metadata store in Celery task
        self.metadata_store = harvester.metadata_store

    except BlockAlreadyAdded as e:
        print('. Skipped {} '.format(block_hash))
    except IntegrityError as e:
        print('. Skipped duplicate {}=={} '.format(block_hash, e))
    except Exception as exc:
        print('! ERROR adding {}'.format(block_hash))
        raise HarvesterCouldNotAddBlock(block_hash) from exc

    block_sets.append({
        'start_block_hash': start_block_hash,
        'end_block_hash': end_block_hash
    })

    return {
        'result': 'Yee data Synchronization job SUCCESS',
        'block_sets': block_sets,
        'result': 'Synch data  from {} to {} blocks check by shardnum of {}'.format(max_block.bid+1, r + max_block.bid+1, shard)

    }


@app.task(base=BaseTask, bind=True)
def start_init(self):
    if self.init:
        print('start_init  task is running : '.format(self.init))
        return {'result': 'waiting init task completed! '}

    n = Block.query(self.session).filter_by(bid=1).count()

    if n >= 4:
        print(' init task is completed! count().n: {} '.format(n))

        return {'result': ' init task is completed! '}
    self.init = True
    print("start_init")
    for shard in SHARDS_TABLE:
        substrate_url = SHARDS_TABLE[shard]
        substrate = SubstrateInterface(substrate_url)
        start_block_hash = substrate.get_block_hash(3)

        print('== start_init  substrate_url {} ==start_block_hash-{}'.format(substrate_url, start_block_hash))

        end_block_hash = None

        accumulate_block_recursive.delay(start_block_hash, end_block_hash, substrate_url)

    return {
        'result': 'start_init job started',
        'SHARDS_TABLE': SHARDS_TABLE,
        'init_task_id': self.request.id

    }


@app.task(base=BaseTask, bind=True)
def dealWithForks(self, shard_num, bid=None, substrate_url=None):
    shard_num = str(shard_num)
    if len(shard_num) == 2:
        shard_num = int(shard_num[1:2])
        print(shard_num)
    else:
        shard_num = int(shard_num)
    print('== dealWithForks  substrate_url* {} *shardnum=*{} *==start_block_num=*{}*'.format(substrate_url, shard_num,
                                                                                             bid))
    substrate = SubstrateInterface(substrate_url)
    # self.session.execute('delete  from data_block where shard_num = %(shard_num)s ',shard_num=shard_num)
    harvester = PolkascanHarvesterService(self.session, type_registry=TYPE_REGISTRY)
    harvester.metadata_store = self.metadata_store
    try:
        nr = 0
        min_bid = find(bid, shard_num, substrate)
        # min_bid = 5
        # bid = 7
        if (bid - min_bid) <= 1:
            return {
                'result': 'dealWithForks from {} to {} blocks check by shardnum of {}'.format(min_bid, bid, shard_num),
                'status': '(bid - min_bid) <= 1,do nothing!'
            }
        self.session.query(Block).filter(Block.shard_num == shard_num, Block.bid > min_bid, Block.bid < bid).delete()
        self.session.query(Extrinsic).filter(Extrinsic.shard_num == shard_num, Extrinsic.block_id > min_bid, Extrinsic.block_id < bid).delete()
        self.session.query(Log).filter(Log.shard_num == shard_num, Log.block_id > min_bid, Log.block_id < bid).delete()
        self.session.query(Event).filter(Event.shard_num == shard_num, Event.block_id > min_bid, Event.block_id < bid).delete()

        for nr in range(min_bid + 1, bid):
            blocka = harvester.add_block(substrate.get_block_hash(nr), substrate_url)
            if blocka:
                print('== Added sucess dealWithForks  substrate_url* {} *shardnum=*{} *==start_block_num=*{}*'.format(
                    substrate_url,
                    shard_num,
                    nr))
                self.session.commit()

    except BlockAlreadyAdded as e:
        print('. dealWithForks Skipped {} '.format(nr))
    except IntegrityError as e:
        print('.dealWithForks Skipped duplicate {}=={} '.format(nr, e))
    except Exception as exc:
        print('!dealWithForks ERROR adding {}'.format(nr))
        raise self.retry(exc=exc, countdown=60, max_retries=5)

    return {
        'result': 'dealWithForks from {} to {} blocks check by shardnum of {}'.format(min_bid, bid, shard_num),
        'dealWithForks_status': 'true'
    }


@app.task(base=BaseTask, bind=True)
def find(self, bid, shard_num, substrate):
    print('== find params  *shardnum=*{} *==bid=*{}*'.format(shard_num, bid))
    block_before =  Block.query(self.session).filter_by(shard_num=shard_num, bid=(bid-1)).first()
    print('== find sql query  *==by bid-1 hash=*{}*'.format(block_before.hash))
    hash_on = substrate.get_block_hash(bid - 1)
    if hash_on == block_before.hash:
        return bid - 1
    else:
        return find(bid - 1, shard_num, substrate)

