<?php

namespace Phpfox\Mysqli;

use Phpfox\Db\AdapterInterface;
use Phpfox\Db\ConnectException;
use Phpfox\Db\SqlAdapterTrait;
use Phpfox\Db\SqlException;

/**
 * Class MysqliAdapter
 *
 * @package Phpfox\Db
 */
class MysqliAdapter implements AdapterInterface
{
    use SqlAdapterTrait;

    /**
     * @var array
     */
    protected $checkKeys
        = [
            'host',
            'port',
            'socket',
            'user',
            'password',
            'charset',
            'persistent',
            'database',
            'prefix',
        ];

    /**
     * @var \mysqli|resource
     */
    protected $master;

    /**
     * @var \mysqli|resource
     */
    protected $slave;

    /**
     * @var bool
     */
    protected $replication = false;

    /**
     * @var array
     */
    protected $params = [];

    /**
     * @var bool
     */
    protected $_inTransaction = false;

    /**
     * @param bool  $replicate
     * @param array $params
     */
    public function __construct($replicate, $params)
    {
        $this->replication = $replicate && count($params['slave']) > 0;
        $this->params = (array)$params;
    }


    /**
     * @return bool
     */
    public function inTransaction()
    {
        return $this->_inTransaction;
    }

    public function query($sql, $master = true)
    {
        $connection = $master ? $this->getMaster() : $this->getSlave();

        $result = $connection->query($sql);

        if (false === $result) {
            throw new SqlException($this->getErrorMessage() . PHP_EOL . $sql);
        }

        if (null === $result) {
            throw new SqlException($this->getErrorMessage() . PHP_EOL . $sql);
        }

        return new MysqliSqlResult($result);
    }

    /**
     * @return \mysqli
     */
    public function getMaster()
    {
        if (!$this->master) {

            // cascauding global configuration
            $defs = [];
            foreach ($this->checkKeys as $key) {
                if (isset($this->params[$key])) {
                    $defs[$key] = $this->params[$key];
                }
            }
            $this->master = $this->connect(array_merge($defs,
                $this->params['master']));
        }

        return $this->master;
    }

    public function connect($params)
    {
        $params = array_merge([
            'host'     => 'localhost',
            'port'     => 3306,
            'database' => null,
            'user'     => null,
            'password' => null,
            'socket'   => null,
        ], $params);

        $connection = new \mysqli($params['host'], $params['user'],
            $params['password'], $params['database'], $params['port'],
            $params['socket']);

        if (null == $connection) {
            throw new ConnectException('Could not connect database');
        }

        if ($connection->connect_errno) {
            $msg = strtr('Db connection error #:number: :msg', [
                ':number' => $connection->connect_errno,
                ':msg'    => $connection->connect_error,
            ]);
            throw new ConnectException($msg);
        }

        // set correct charset
        $connection->set_charset('utf8mb4');

        return $connection;
    }

    /**
     * @return \mysqli
     */
    public function getSlave()
    {
        if (null != $this->slave) {
            return $this->slave;
        }

        if (false == $this->replication) {
            $this->slave = $this->getMaster();
        }

        if (!$this->slave) {
            $defs = [];
            foreach ($this->checkKeys as $key) {
                if (isset($this->params[$key])) {
                    $defs[$key] = $this->params[$key];
                }
            }

            $array = $this->params['slave'];
            $length = count($array);
            $params = $length == 1 ? array_shift($array)
                : $array[mt_rand(0, $length - 1)];
            $this->slave = $this->connect(array_merge($defs, $params));
        }

        if (!$this->slave) {
            $this->slave = $this->master;
        }

        return $this->slave;
    }

    public function getErrorMessage($master = true)
    {
        return $this->getMaster()->error;
    }

    public function quoteIdentifier($value)
    {
        return $value;
    }

    /**
     * @param $value
     *
     * @return mixed
     */
    public function quoteValue($value)
    {
        switch (true) {
            case is_bool($value):
                return $value ? 1 : 0;
            case is_null($value):
                return 'NULL';
            case is_array($value):
                return implode(', ', array_map([$this, 'quoteValue'], $value));
            case is_string($value):
                return '\'' . $this->escape($value) . '\'';
            case is_numeric($value):
                return $value;
            default:
                return $value;
        }
    }

    /**
     * @param $string
     *
     * @return string
     */
    public function escape($string)
    {
        return $this->getMaster()->real_escape_string($string);
    }

    /**
     * @return int
     */
    public function lastId()
    {
        return $this->getMaster()->insert_id;
    }

    public function getErrorCode($master = true)
    {
        return 0;
    }


    /**
     * @return array [string, ]
     */
    public function tables()
    {
        $result = $this->getMaster()->query('show tables');

        $response = [];

        while (null != ($row = mysqli_fetch_array($result))) {
            $response[] = $row[0];
        }

        return $response;
    }

    /**
     * @param $table
     *
     * @return string
     */
    public function getCreateTableSql($table)
    {
        $result = $this->exec("SHOW CREATE TABLE `" . $table . "`");

        $row = mysqli_fetch_array($result, MYSQLI_BOTH);

        return $row[1];
    }

    /**
     * @param string    $sql
     * @param bool|true $master
     *
     * @return bool|\mysqli_result
     */
    public function exec($sql, $master = true)
    {
        return $this->getMaster()->query($sql);
    }

    public function begin()
    {
        if ($this->_inTransaction) {
            return;
        }

        $this->exec('START TRANSACTION');

        // set in transaction
        $this->_inTransaction = true;
    }

    public function commit()
    {
        if (!$this->_inTransaction) {
            return false;
        }

        $this->exec('COMMIT');
        $this->_inTransaction = false;
        return $this;
    }

    public function rollback()
    {
        $this->exec('ROLLBACK');
        $this->_inTransaction = false;
    }
}