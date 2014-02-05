# Copyright (C) 2013 by Clearcode <http://clearcode.cc>
# and associates (see AUTHORS).

# This file is part of pytest-dbfixtures.

# pytest-dbfixtures is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# pytest-dbfixtures is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with pytest-dbfixtures.  If not, see <http://www.gnu.org/licenses/>.

import os
import pytest
import shutil
import subprocess

from path import path
from tempfile import mkdtemp
from summon_process.executors import TCPCoordinatedExecutor

from pytest_dbfixtures import factories
from pytest_dbfixtures.utils import get_config, try_import


ROOT_DIR = path(__file__).parent.parent.abspath()


def pytest_addoption(parser):
    parser.addoption(
        '--dbfixtures-config',
        action='store',
        default=ROOT_DIR / 'pytest_dbfixtures' / 'dbfixtures.conf',
        metavar='path',
        dest='db_conf',
    )

    parser.addoption(
        '--mongo-config',
        action='store',
        default=ROOT_DIR / 'pytest_dbfixtures' / 'mongo.conf',
        metavar='path',
        dest='mongo_conf',
    )

    parser.addoption(
        '--redis-config',
        action='store',
        default=ROOT_DIR / 'pytest_dbfixtures' / 'redis.conf',
        metavar='path',
        dest='redis_conf',
    )

    parser.addoption(
        '--rabbit-config',
        action='store',
        default=ROOT_DIR / 'pytest_dbfixtures' / 'rabbit.conf',
        metavar='path',
        dest='rabbit_conf',
    )


redis_proc = factories.redis_proc()
redisdb = factories.redisdb('redis_proc')

rabbitmq_proc = factories.rabbitmq_proc()
rabbitmq = factories.rabbitmq('rabbitmq_proc')


@pytest.fixture(scope='session')
def mongo_proc(request):
    """
    #. Get config.
    #. Run a ``mongod`` process.
    #. Stop ``mongod`` process after tests.

    .. note::
        `mongod <http://docs.mongodb.org/v2.2/reference/mongod/>`_

    :param FixtureRequest request: fixture request object
    :rtype: summon_process.executors.tcp_coordinated_executor.TCPCoordinatedExecutor
    :returns: tcp executor
    """
    config = get_config(request)
    mongo_conf = request.config.getvalue('mongo_conf')

    mongo_executor = TCPCoordinatedExecutor(
        '{mongo_exec} {params} {config}'.format(
            mongo_exec=config.mongo.mongo_exec,
            params=config.mongo.params,
            config=mongo_conf),
        host=config.mongo.host,
        port=config.mongo.port,
    )
    mongo_executor.start()

    def stop():
        mongo_executor.stop()

    request.addfinalizer(stop)

    return mongo_executor


@pytest.fixture
def mongodb(request, mongo_proc):
    """
    #. Get pymongo module and config.
    #. Get connection to mongo.
    #. Drop collections before and after tests.

    :param FixtureRequest request: fixture request object
    :param TCPCoordinatedExecutor mongo_proc: tcp executor
    :rtype: pymongo.connection.Connection
    :returns: connection to mongo database
    """
    pymongo, config = try_import('pymongo', request)

    mongo_conn = pymongo.Connection(
        config.mongo.host,
        config.mongo.port
    )

    def drop():
        for db in mongo_conn.database_names():
            for collection_name in mongo_conn[db].collection_names():
                if collection_name != 'system.indexes':
                    mongo_conn[db][collection_name].drop()

    request.addfinalizer(drop)

    drop()

    return mongo_conn


def get_rabbit_env(name):
    """
    Get value from environment variable. If does not exists (older version) then
    use older name.

    :param str name: name of environment variable
    :rtype: str
    :returns: path to directory
    """
    return os.environ.get(name) or os.environ.get(name.split('RABBITMQ_')[1])


def get_rabbit_path(name):
    """
    Get a path to directory contains sub-directories for the RabbitMQ
    server's Mnesia database files. `Relocate <http://www.rabbitmq.com/relocate.html>`_

    If environment variable or path to directory do not exist, return ``None``, else
    return path to directory.

    :param str name: name of environment variable
    :rtype: path.path or None
    :returns: path to directory
    """
    env = get_rabbit_env(name)

    if not env or not path(env).exists():
        return

    return path(env)

def remove_mysql_directory(config):
    """
    Checks mysql directory. Recursively delete a directory tree if exist.

    :param pymlconf.ConfigManager config: config
    """
    if os.path.isdir(config.mysql.datadir):
        shutil.rmtree(config.mysql.datadir)


def init_mysql_directory(config):
    """
    #. Remove mysql directory if exist.
    #. `Initialize MySQL data directory <https://dev.mysql.com/doc/refman/5.0/en/mysql-install-db.html>`_

    :param pymlconf.ConfigManager config: config
    """
    remove_mysql_directory(config)
    init_directory = (
        config.mysql.mysql_init,
        '--user=%s' % os.getenv('USER'),
        '--datadir=%s' % config.mysql.datadir,
    )
    subprocess.check_output(' '.join(init_directory), shell=True)


@pytest.fixture(scope='session')
def mysql_proc(request):
    """
    #. Get config.
    #. Initialize MySQL data directory
    #. `Start a mysqld server https://dev.mysql.com/doc/refman/5.0/en/mysqld-safe.html`_
    #. Stop server and remove directory after tests. `<https://dev.mysql.com/doc/refman/5.6/en/mysqladmin.html>`_

    :param FixtureRequest request: fixture request object
    :rtype: summon_process.executors.tcp_coordinated_executor.TCPCoordinatedExecutor
    :returns: tcp executor
    """
    config = get_config(request)
    init_mysql_directory(config)

    mysql_executor = TCPCoordinatedExecutor(
        '''
            {mysql_server} --datadir={datadir} --pid-file={pidfile}
            --port={port} --socket={socket} --log-error={logfile}
            --skip-syslog
        '''.format(
        mysql_server=config.mysql.mysql_server,
        datadir=config.mysql.datadir,
        pidfile=config.mysql.pidfile,
        port=config.mysql.port,
        socket=config.mysql.socket,
        logfile=config.mysql.logfile,
    ),
        host=config.mysql.host,
        port=config.mysql.port,
    )
    mysql_executor.start()

    def shutdown_server():
        return (
            config.mysql.mysql_admin,
            '--socket=%s' % config.mysql.socket,
            '--user=%s' % config.mysql.user,
            'shutdown'
        )

    def stop_server_and_remove_directory():
        subprocess.check_output(' '.join(shutdown_server()), shell=True)
        mysql_executor.stop()
        remove_mysql_directory(config)

    request.addfinalizer(stop_server_and_remove_directory)

    return mysql_executor


def mysqldb_fixture_factory(scope='session'):
    """
    Factory. Create connection to mysql. If you want you can give a scope,
    default is 'session'.

    :param str scope: scope (session, function, module, etc.)
    :rtype: func
    :returns: function ``mysqldb_fixture`` with suit scope
    """

    @pytest.fixture(scope)
    def mysqldb_fixture(request, mysql_proc):
        """
        #. Get config.
        #. Try to import MySQLdb package.
        #. Connect to mysql server.
        #. Create database.
        #. Use proper database.
        #. Drop database after tests.

        :param FixtureRequest request: fixture request object
        :param TCPCoordinatedExecutor mysql_proc: tcp executor
        :rtype: MySQLdb.connections.Connection
        :returns: connection to database
        """

        config = get_config(request)

        MySQLdb, config = try_import(
            'MySQLdb', request, pypi_package='MySQL-python'
        )

        mysql_conn = MySQLdb.connect(
            host=config.mysql.host,
            unix_socket=config.mysql.socket,
            user=config.mysql.user,
            passwd=config.mysql.password,
        )

        mysql_conn.query('CREATE DATABASE %s' % config.mysql.db)
        mysql_conn.query('USE %s' % config.mysql.db)

        def drop_database():
            mysql_conn.query('DROP DATABASE IF EXISTS %s' % config.mysql.db)
            mysql_conn.close()

        request.addfinalizer(drop_database)

        return mysql_conn

    return mysqldb_fixture


mysqldb_session = mysqldb_fixture_factory()
mysqldb = mysqldb_fixture_factory(scope='function')
