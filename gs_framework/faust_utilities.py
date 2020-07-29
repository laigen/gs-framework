import asyncio
import logging
import os
import time
from typing import Tuple, Any, Mapping, Dict

import dns
import faust
from faust import TopicT, EventT, App
from faust.types import AppT, TP

from confluent_kafka.admin import AdminClient, ConfigSource, ConfigResource
from confluent_kafka.cimpl import KafkaException, NewTopic

from .utilities import bytes_2_object, object_2_bytes


logger = logging.getLogger(__name__)


class FaustUtilities:

    # The topic parameter settings hardcoded here match logic in functions decode_message and send_message below
    # It seems that faust allow multiple TopicT objects be created for same topic name, it's not like agent names
    # which are unique for same app
    @staticmethod
    def create_topic(app: AppT, topic: TP) -> TopicT:
        FaustUtilities.Admin.check_and_create_topic(topic)
        return app.topic(topic.topic, key_type=bytes, value_type=bytes,
                         value_serializer="raw", key_serializer="raw", partitions=topic.partition)

    # for now we only have one serialization method so we don't send the serialization header repeatedly
    # MSG_HEADER_GS_SERIALIZER_FUNC = "serializer"
    # """序列化的方式，目前gs接管了所有 to_bytes 的操作，该值填 'gs_func'"""
    # GS_SERIALIZATION_METHOD = "gs_func"

    @staticmethod
    def decode_message(event: EventT) -> Tuple[Any, Any, Dict[str, Any]]:
        # for now we only have one serialization method so we don't send the serialization header repeatedly
        # assert FaustUtilities.GS_SERIALIZATION_METHOD == \
        #        bytes_2_object(event.headers[FaustUtilities.MSG_HEADER_GS_SERIALIZER_FUNC])
        headers = {k: bytes_2_object(v) for k, v in event.headers.items()} if event.headers is not None else None
        return bytes_2_object(event.message.key), bytes_2_object(event.message.value), headers

    @staticmethod
    def send_message(topic: TopicT, *, key: Any = None, key_bytes: bytes = None, value: Any = None,
                     value_bytes: bytes = None, headers: Mapping[str, Any] = None) -> asyncio.Future:
        # The headers we add:
        #   header[MSG_HEADER_GS_SERIALIZER_FUNC] 转成bytes的序列，目前固定为 "gs_func" 即使用 utilities 下的
        #                                                               object_2_bytes() / bytes_2_object()
        # just modify the header object passed to avoid a copy,
        # since in most cases the passed in header object won't be reused. the assert below check this
        # for now we only have one serialization method so we don't send the serialization header repeatedly
        # headers = headers if headers is not None else dict()
        # assert FaustUtilities.MSG_HEADER_GS_SERIALIZER_FUNC not in headers
        # headers[FaustUtilities.MSG_HEADER_GS_SERIALIZER_FUNC] = FaustUtilities.GS_SERIALIZATION_METHOD

        # Note that None is a valid value which can be serialized to
        # bytes b'\x80\x04\x95\x02\x00\x00\x00\x00\x00\x00\x00N.'
        serialized_headers = [(k, object_2_bytes(v)) for k, v in headers.items()] if headers is not None else None
        return asyncio.ensure_future(topic.send(key=object_2_bytes(key) if key_bytes is None else key_bytes,
                                                value=object_2_bytes(value) if value_bytes is None else value_bytes,
                                                headers=serialized_headers,
                                                value_serializer="raw", key_serializer="raw", force=True))

    @staticmethod
    def get_kafka_broker_address():
		# your kafka server address
        return ''
        

    _DNS_PROBE_RETRIES = 10

    @staticmethod
    def _get_service_port_number(service_port_name):
        remained_retries = FaustUtilities._DNS_PROBE_RETRIES
        while remained_retries > 0:
            remained_retries = remained_retries - 1
            try:
                answer = dns.resolver.query(service_port_name, 'SRV')
                return answer[0].port
            except Exception as e:
                if remained_retries == 0:
                    raise e
                else:
                    time.sleep(0.5)

    @staticmethod
    def create_faust_app(app_id: str) -> App:
        data_dir = f"/opt/gsfaust/{app_id}"
        os.makedirs(data_dir, exist_ok=True)
        return faust.App(app_id, broker=f"kafka://{FaustUtilities.get_kafka_broker_address()}",
                         store="rocksdb://", datadir=data_dir, web_enabled=False)

    @staticmethod
    def set_app_default_config(app: AppT):
        """设置 faust app 在 Dist, NonDist 中相同的配置内容(gs_framework需要进行的区别于 faust 的 default setting)"""
        # 详细的 config see: https://faust.readthedocs.io/en/latest/reference/faust.types.settings.html
        app.conf.web_enabled = False
        app.conf.logging_config = {"level": logging.INFO,
                                   "format": '%(asctime)s - [%(name)s,line:%(lineno)d] - %(levelname)s - %(message)s'}

    class Admin:

        @staticmethod
        def list_topics(a: AdminClient, args):
            """ list topics and cluster metadata """

            if len(args) == 0:
                what = "all"
            else:
                what = args[0]

            md = a.list_topics(timeout=10)

            print("Cluster {} metadata (response from broker {}):".format(md.cluster_id, md.orig_broker_name))

            if what in ("all", "brokers"):
                print(" {} brokers:".format(len(md.brokers)))
                for b in iter(md.brokers.values()):
                    if b.id == md.controller_id:
                        print("  {}  (controller)".format(b))
                    else:
                        print("  {}".format(b))

            if what not in ("all", "topics"):
                return

            print(" {} topics:".format(len(md.topics)))
            for t in iter(md.topics.values()):
                if t.error is not None:
                    err_str = ": {}".format(t.error)
                else:
                    err_str = ""

                print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), err_str))

                for p in iter(t.partitions.values()):
                    if p.error is not None:
                        err_str = ": {}".format(p.error)
                    else:
                        err_str = ""

                    print("    partition {} leader: {}, replicas: {}, isrs: {}".format(
                        p.id, p.leader, p.replicas, p.isrs, err_str))

        @staticmethod
        def _print_config(config, depth):
            # depth = 1 # for debug
            print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
                  ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
                   config.is_read_only, config.is_default,
                   config.is_sensitive, config.is_synonym,
                   ["%s:%s" % (x.name, ConfigSource(x.source))
                    for x in iter(config.synonyms.values())]))

        @staticmethod
        def describe_configs(a: AdminClient, args):
            """ describe configs """

            resources = [ConfigResource(restype, resname) for
                         restype, resname in zip(args[0::2], args[1::2])]

            fs = a.describe_configs(resources)

            # Wait for operation to finish.
            for res, f in fs.items():
                try:
                    configs = f.result()
                    for config in iter(configs.values()):
                        FaustUtilities.Admin._print_config(config, 1)

                except KafkaException as e:
                    print("Failed to describe {}: {}".format(res, e))
                except Exception:
                    raise

        # @staticmethod
        # def create_topics(a: AdminClient, topics):
        #     """ Create topics """
        #
        #     new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
        #     # Call create_topics to asynchronously create topics, a dict
        #     # of <topic,future> is returned.
        #     fs = a.create_topics(new_topics)
        #
        #     # Wait for operation to finish.
        #     # Timeouts are preferably controlled by passing request_timeout=15.0
        #     # to the create_topics() call.
        #     # All futures will finish at the same time.
        #     for topic, f in fs.items():
        #         try:
        #             f.result()
        #             print("Topic {} created".format(topic))
        #         except Exception as e:
        #             print("Failed to create topic {}: {}".format(topic, e))

        @staticmethod
        def create_topic(a: AdminClient, topic: str, num_partitions: int, replication_factor=1):
            new_topic = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)]
            fs = a.create_topics(new_topic)

            for topic, f in fs.items():
                try:
                    f.result()
                    print("Topic {} created".format(topic))
                except Exception as e:
                    print("Failed to create topic {}: {}".format(topic, e))

        @staticmethod
        def delete_topics(a: AdminClient, topics):
            """ delete topics """

            # Call delete_topics to asynchronously delete topics, a future is returned.
            # By default this operation on the broker returns immediately while
            # topics are deleted in the background. But here we give it some time (30s)
            # to propagate in the cluster before returning.
            #
            # Returns a dict of <topic,future>.
            fs = a.delete_topics(topics, operation_timeout=30)

            # Wait for operation to finish.
            for topic, f in fs.items():
                try:
                    f.result()
                    print("Topic {} deleted".format(topic))
                except Exception as e:
                    print("Failed to delete topic {}: {}".format(topic, e))

        @staticmethod
        def is_topic_existed(a: AdminClient, topic: str):
            md = a.list_topics(topic, timeout=10)
            return md.topics[topic].error is None

        @staticmethod
        def check_and_create_topic(topic: TP):
            try:
                kafka_url = FaustUtilities.get_kafka_broker_address()
                a = AdminClient({'bootstrap.servers': kafka_url})

                # debugging code to delete the topic
                # if FaustUtilities.Admin.is_topic_existed(a, topic.topic):
                #     FaustUtilities.Admin.delete_topics(a, [topic.topic])

                topic_details = a.list_topics(topic.topic, timeout=10).topics[topic.topic]
                if topic_details.error is None:
                    assert topic.partition == len(topic_details.partitions)
                else:
                    logger.info(f"topic not exist, create topic '{topic.topic}' with {topic.partition} partitions")
                    FaustUtilities.Admin.create_topic(a, topic.topic, topic.partition)
            except Exception as e:
                logger.error(e)
                print(f"{e}")

        @staticmethod
        def delete_table(app: AppT, table_name: str):
            table = app.tables.pop(table_name, None)
            if table is not None:
                table.reset_state()

                kafka_url = FaustUtilities.get_kafka_broker_address()
                a = AdminClient({'bootstrap.servers': kafka_url})

                change_log_topic_name = table.changelog_topic.get_topic_name()
                FaustUtilities.Admin.delete_topics(a, [change_log_topic_name])
