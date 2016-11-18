<?php

namespace Phpfox\Mysqli;

use mysqli;
use Phpfox\Db\ConnectionInterface;
use Phpfox\Db\SqlException;

class Connection implements ConnectionInterface
{
    /**
     * @var mysqli
     */
    protected $resource;
    /**
     * @var string
     */
    private $params = [];

    /**
     * @var bool
     */
    private $inTransaction = false;

    /**
     * Connection constructor.
     *
     * @param mixed $params
     */
    public function __construct($params)
    {
        if (is_array($params)) {
            $this->setup($params);
        } else {
            $this->setup([]);
        }

        if ($params instanceof \mysqli) {
            $this->resource = $params;
        }
    }

    /**
     * @param array $params
     *
     * @return $this
     */
    private function setup($params)
    {
        $defaults = [
            'host'        => '127.0.0.1',
            'port'        => 3306,
            'username'    => '',
            'database'    => '',
            'password'    => '',
            'replication' => false,
            'socket'      => null,
            'charset'     => 'utf8',
        ];
        $this->params = array_merge($defaults, $params);

        return $this;
    }

    /**
     * @inheritdoc
     */
    public function disconnect()
    {
        if ($this->resource instanceof \mysqli) {
            $this->resource->close();
        }

        $this->resource = null;
        $this->inTransaction = false;

        return $this;
    }

    /**
     * @inheritdoc
     */
    public function isConnected()
    {
        return ($this->resource instanceof \mysqli);
    }

    /**
     * @inheritdoc
     */
    public function isInTransaction()
    {
        return $this->inTransaction;
    }

    /**
     * @inheritdoc
     */
    public function begin()
    {
        if ($this->inTransaction) {
            return $this;
        }

        $this->ensureConnected();

        $this->resource->autocommit(false);
        $this->inTransaction = true;

        return $this;
    }

    /**
     * @inheritdoc
     */
    public function ensureConnected()
    {
        if (!$this->resource instanceof \mysqli) {
            $this->connect();
        }

        return $this;
    }

    /**
     * @inheritdoc
     */
    public function connect()
    {
        if ($this->resource instanceof \mysqli) {
            return $this;
        }

        $this->inTransaction = false;
        $this->resource = new \mysqli();

        $this->resource->init();

        $params = $this->params;

        $host = $params['host'];

        if (is_array($host)) {
            $host = array_values($host);
            $host = $host[mt_rand(0, count($host) - 1)];
        }

        $result = $this->resource->real_connect($host, $params['username'],
            $params['password'], $params['database'], $params['socket']);

        if (!$result) {
            throw new \RuntimeException("Can not connect to database server");
        }
        $this->resource->set_charset($params['charset']);

        return $this;
    }

    /**
     * @inheritdoc
     */
    public function commit()
    {
        $this->resource->commit();
        $this->inTransaction = false;
        $this->resource->autocommit(true);

        return $this;
    }

    /**
     * @inheritdoc
     */
    public function rollback()
    {
        $this->resource->rollback();
        $this->inTransaction = false;
        $this->resource->autocommit(true);
    }

    /**
     * @inheritdoc
     */
    public function lastId()
    {
        return $this->resource->insert_id;
    }

    /**
     * @inheritdoc
     */
    public function execute($sql)
    {
        $this->ensureConnected();

        $value = $this->resource->query($sql);

        if ($value === false) {
            throw new SqlException($this->resource->error);
        }

        if ($value === true) {
            return new MysqliSqlResult(true, $value, null);
        }

        return new MysqliSqlResult(true, $value, null);
    }

    /**
     * @param string $value
     *
     * @return string
     */
    public function quoteValue($value)
    {
        $this->ensureConnected();
        return $this->resource->escape_string($value);
    }

    /**
     * @param  string $value
     *
     * @return string mixed
     */
    public function quoteIdentifier($value)
    {
        return $value;
    }

    /**
     * @return mysqli
     */
    public function getResource()
    {
        $this->ensureConnected();
        return $this->resource;
    }

    /**
     * @inheritdoc
     */
    public function setResource($resource)
    {
        $this->resource = $resource;

        return $this;
    }

    /**
     * @return string
     */
    public function getPlatformName()
    {
        return 'mysql';
    }

    /**
     * @inheritdoc
     */
    public function getError()
    {
        return $this->resource->error;
    }
}