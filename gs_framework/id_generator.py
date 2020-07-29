# -*- coding: UTF-8 -*-

"""
Copy From
https://github.com/luohaifenglight/snowflakeservice/blob/master/snowflakeservice/snowflake.py
"""

import time
import logging
logger = logging.getLogger(__name__)


class DataConvertError(Exception):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return str(self.value)


class SnowFlake(object):

    def __init__(self, work_num, data_center_num):

        self.work_num = work_num
        self.data_center_num = data_center_num

        self.sequence = 0
        self.generated_ids = 0

        self.work_num_bits = 5
        self.data_center_num_bits = 5
        self.sequence_bits = 12

        self.max_work_num = -1 ^ (-1 << self.work_num_bits)
        self.max_data_center_num = -1 ^ (-1 << (self.data_center_num_bits))

        self.work_num_shift = self.sequence_bits
        self.data_center_num_shift = self.sequence_bits + self.work_num_bits
        self.timestamp_shift = self.data_center_num_shift + self.data_center_num_bits
        self.sequence_mask = -1 ^ (-1 << self.sequence_bits)

        self.last_timestamp = -1

        if self.work_num > self.max_work_num or self.work_num < 0:
            raise DataConvertError("work_num exceed max limit")

        if self.data_center_num > self.max_data_center_num or self.data_center_num < 0:
            raise DataConvertError("data_center_num exceed max limit")

    def _gen_timestamp(self):
        return int(time.time() * 1000)

    def _gen_next_millis_time(self, timestamp):
        c_timestamp = self._gen_timestamp()

        if c_timestamp <= timestamp:
            while c_timestamp >= timestamp:
                c_timestamp = self._gen_timestamp()

        return c_timestamp

    def _next_num(self):
        timestamp = self._gen_timestamp()

        if self.last_timestamp > timestamp:
            raise DataConvertError("clock is moving backwards. please adjust system clock")

        if self.last_timestamp == timestamp:
            self.sequence = (self.sequence + 1) & self.sequence_mask
            if self.sequence == 0:
                logger.info(f"reach limit 4096, gen next millis time")
                timestamp = self._gen_next_millis_time(timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        new_number = (timestamp << self.timestamp_shift) | (self.data_center_num << self.data_center_num_shift) | \
                     (self.work_num << self.work_num_shift) | self.sequence
        self.generated_ids += 1

        return new_number

    def gen_id(self):
        """
        :return:  produce only seq number
        """
        return self._next_num()
