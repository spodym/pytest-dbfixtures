import os
import subprocess
from tempfile import mkdtemp

import pytest
from path import path
from summon_process.executors import TCPCoordinatedExecutor

from pytest_dbfixtures.utils import get_config, try_import


def redis_proc(executable=None, params=None, config_file=None,
               host=None, port=None):
    """
    Redis process factory.

    :param str executable: path to redis-server
    :param str params: params
    :param str config_file: path to config file
    :param str host: hostname
    :param str port: port
    :rtype: func
    :returns: function which makes a redis process
    """

    @pytest.fixture(scope='session')
    def redis_proc_fixture(request):
        """
        #. Get configs.
        #. Run redis process.
        #. Stop redis process after tests.

        :param FixtureRequest request: fixture request object
        :rtype: summon_process.executors.tcp_coordinated_executor.TCPCoordinatedExecutor
        :returns: tcp executor
        """
        config = get_config(request)

        redis_exec = executable or config.redis.redis_exec
        redis_params = params or config.redis.params
        redis_conf = config_file or request.config.getvalue('redis_conf')
        redis_host = host or config.redis.host
        redis_port = port or config.redis.port

        pidfile = 'redis-server.{port}.pid'.format(port=redis_port)
        unixsocket = 'redis.{port}.sock'.format(port=redis_port)
        dbfilename = 'dump.{port}.rdb'.format(port=redis_port)
        logfile = 'redis-server.{port}.log'.format(port=redis_port)

        redis_executor = TCPCoordinatedExecutor(
            '''{redis_exec} {params} {config}
            --pidfile {pidfile} --unixsocket {unixsocket}
            --dbfilename {dbfilename} --logfile {logfile}
            --port {port}'''.format(redis_exec=redis_exec,
                                    params=redis_params,
                                    config=redis_conf,
                                    pidfile=pidfile,
                                    unixsocket=unixsocket,
                                    dbfilename=dbfilename,
                                    logfile=logfile,
                                    port=redis_port
                                    ),
            host=redis_host,
            port=redis_port,
        )
        redis_executor.start()

        request.addfinalizer(redis_executor.stop)

        return redis_executor

    return redis_proc_fixture


def redisdb(process_fixture_name, host=None, port=None, db=None):
    """
    Redis database factory.

    :param str process_fixture_name: name of the process fixture
    :param str host: hostname
    :param int port: port
    :param int db: number of database
    :rtype: func
    :returns: function which makes a connection to redis
    """

    @pytest.fixture
    def redisdb_factory(request):
        """
        #. Load required process fixture.
        #. Get redis module and config.
        #. Connect to redis.
        #. Flush database after tests.

        :param FixtureRequest request: fixture request object
        :rtype: redis.client.Redis
        :returns: Redis client
        """
        request.getfuncargvalue(process_fixture_name)

        redis, config = try_import('redis', request)

        redis_host = host or config.redis.host
        redis_port = port or config.redis.port
        redis_db = db or config.redis.db

        redis_client = redis.Redis(redis_host, redis_port, redis_db)
        request.addfinalizer(redis_client.flushall)

        return redis_client

    return redisdb_factory


def rabbitmq_proc(config_file=None, server=None, host=None, port=None,
                  node_name=None, rabbit_ctl_file=None):
    '''
        Starts RabbitMQ process.

        :param str config_file: path to config file
        :param str server: path to rabbitmq-server command
        :param str host: server host
        :param int port: server port
        :param str node_name: RabbitMQ node name used for setting environment
                              variable RABBITMQ_NODENAME
        :param str rabbit_ctl_file: path to rabbitmqctl file

        :returns pytest fixture with RabbitMQ process executor
    '''

    @pytest.fixture(scope='session')
    def rabbitmq_proc_fixture(request):

        def get_rabbit_env(name):
            return os.environ.get(name) or os.environ.get(
                name.split('RABBITMQ_')[1]
            )

        def get_rabbit_path(name):
            env = get_rabbit_env(name)
            if not env or not path(env).exists():
                return
            return path(env)

        rabbit_conf = config_file
        if not rabbit_conf:
            rabbit_conf = request.config.getvalue('redis_conf')
            rabbit_conf = open(
                request.config.getvalue('rabbit_conf')
            ).readlines()
            rabbit_conf = dict(line[:-1].split('=') for line in rabbit_conf)

        tmpdir = path(mkdtemp(prefix='rabbitmq_fixture'))
        rabbit_conf['RABBITMQ_LOG_BASE'] = str(tmpdir)
        rabbit_conf['RABBITMQ_MNESIA_BASE'] = str(tmpdir)

        # setup environment variables
        for name, value in rabbit_conf.items():
            # for new versions of rabbitmq-server:
            os.environ[name] = value
            # for older versions of rabbitmq-server:
            prefix, name = name.split('RABBITMQ_')
            os.environ[name] = value

        pika, config = try_import('pika', request)

        rabbit_server = server
        if not rabbit_server:
            rabbit_server = config.rabbit.rabbit_server

        rabbit_host = host
        if not rabbit_host:
            rabbit_host = config.rabbit.host

        rabbit_port = port
        if not rabbit_port:
            rabbit_port = config.rabbit.port
        os.putenv('RABBITMQ_NODE_PORT', str(rabbit_port))

        rabbit_executor = TCPCoordinatedExecutor(
            rabbit_server,
            rabbit_host,
            rabbit_port,
        )

        base_path = get_rabbit_path('RABBITMQ_MNESIA_BASE')
        print '\nbase_path', base_path

        def stop_and_reset():
            print '\nStopping RabbitMQ process on port', rabbit_port
            rabbit_executor.stop()
            print 'Stopped'
            base_path.rmtree()
            tmpdir.exists() and tmpdir.rmtree()
        request.addfinalizer(stop_and_reset)

        rabbit_node_name = node_name
        if rabbit_node_name:
            os.putenv('RABBITMQ_NODENAME', rabbit_node_name)
        else:
            rabbit_node_name = get_rabbit_env('RABBITMQ_NODENAME')
        print 'rabbit_node_name', rabbit_node_name

        rabbit_ctl = rabbit_ctl_file
        if not rabbit_ctl:
            rabbit_ctl = config.rabbit.rabbit_ctl

        print '\nStarting RabbitMQ process on port', rabbit_port
        rabbit_executor.start()
        print 'Started'
        pid_file = base_path / rabbit_node_name + '.pid'
        wait_cmd = rabbit_ctl, '-q', 'wait', pid_file
        subprocess.Popen(wait_cmd).communicate()
        print 'after subprocess'

        return rabbit_executor

    return rabbitmq_proc_fixture


def rabbitmq(process_fixture_name, host=None, port=None):
    '''
        Connects with RabbitMQ server

        :param str process_fixture_name: name of RabbitMQ preocess variable
                                         returned by rabbitmq_proc
        :param str host: RabbitMQ server host
        :param int port: RabbitMQ server port

        :returns RabbitMQ connection
    '''

    @pytest.fixture
    def rabbitmq_factory(request):
        # load required process fixture
        request.getfuncargvalue(process_fixture_name)

        pika, config = try_import('pika', request)

        rabbit_host = host
        if not rabbit_host:
            rabbit_host = config.rabbit.host

        rabbit_port = host
        if not rabbit_port:
            rabbit_port = config.rabbit.port

        rabbit_params = pika.connection.ConnectionParameters(
            host=rabbit_host,
            port=rabbit_port,
            connection_attempts=3,
            retry_delay=2,
        )
        try:
            rabbit_connection = pika.BlockingConnection(rabbit_params)
        except pika.adapters.blocking_connection.exceptions.ConnectionClosed:
            print "Be sure that you're connecting rabbitmq-server >= 2.8.4"
        return rabbit_connection

    return rabbitmq_factory
