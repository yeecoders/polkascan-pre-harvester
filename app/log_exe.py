import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from app.models.data import Log
from app.processors.converters import BlockAlreadyAdded
from substrateinterface import SubstrateInterface
from app.settings import SHARDS_TABLE, DB_CONNECTION, DEBUG
from scalecodec.base import ScaleBytes, ScaleDecoder
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine, text


def check_healthy():
    for shard in SHARDS_TABLE:
        substrate_url = SHARDS_TABLE[shard]
        substrate = SubstrateInterface(substrate_url)
        block = substrate.get_block_number(None)
        print('== shard--{}  ===substrate_url###{}==block=={} '.format(shard, substrate_url, block))


class BaseLog(object):

    def __init__(self):
        self.engine = create_engine(DB_CONNECTION, echo=DEBUG, isolation_level="READ_UNCOMMITTED")
        session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
        self.session = scoped_session(session_factory)

    def log_processor(self, logs, block_id):
        count_log = len(logs)
        shard_num = None
        if count_log != 0:
            for idx, log_data in enumerate(logs):
                if idx == 1:
                    print('== log_data  ===log_data-{}--{} '.format(log_data, idx))
                    ni = log_data.index('03000000000000000000')
                    final = '0x' + log_data[20 + ni:]
                    oy = ScaleDecoder.get_decoder_class('Vec<(SessionKey, u64)>', ScaleBytes(final))
                    oy.decode()
                    print('== oy.value  ===oy.value-{} '.format(oy.value))

                    for i in range(len(oy.value)):
                        oy.value[i] = "{'authoritiy': '" + oy.value[i]["col1"] + "', 'weight': " + str(oy.value[i]["col2"]) + "}"
                elif idx == 0:
                    print('== log_data  ===log_data-{}--{} '.format(log_data, idx))
                    shard_num = log_data[10:12]
                    print('== shard_num  ===shard_num-{} '.format(shard_num))

            log = Log.query(self.session).filter(Log.block_id == block_id, Log.log_idx == 1, Log.shard_num == shard_num).first()

            print('== log  ===dblog-{} '.format(log.block_id))
            print('== log  ===true-block_id-{} '.format(block_id))
            log.data = oy.value,
            self.session.commit()

    def solve(self, block_id, end_block_id, substrate_url):
        print('== updatelog  ===substrate_url-{} ===start_block_hash-{} ===end-{}'.format(substrate_url, block_id,
                                                                                          end_block_id))
        substrate = SubstrateInterface(substrate_url)
        try:
            for nr in range(int(block_id), int(end_block_id)):
                block_hash = substrate.get_block_hash(hex(nr))
                json_block = substrate.get_chain_block(block_hash)
                l_block_id = json_block['block']['header'].pop('number')
                l_block_id = int(l_block_id, 16)
                digest_logs = json_block['block']['header'].get('digest', {}).pop('logs', None)
                print('== digest_logs  ===digest_logs-{} '.format(digest_logs))
                print('== l_block_id  ===l_block_id-{} '.format(l_block_id))

                self.log_processor(digest_logs, l_block_id)

        except BlockAlreadyAdded as e:
            print('. Skipped {} '.format(block_hash))

    def on_get(self):
        for shard in SHARDS_TABLE:
            substrate_url = SHARDS_TABLE[shard]
            BaseLog.solve(self, '1', '2', substrate_url)


my = BaseLog()

# my.on_get()

check_healthy()
