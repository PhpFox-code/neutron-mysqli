<?php

namespace Phpfox\Mysqli;

use Phpfox\Db\AdapterInterface;

/**
 * Class MysqliAdapter
 *
 * @package Phpfox\Db
 */
class MysqliAdapter implements AdapterInterface
{

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
     * @param string $table
     * @param array  $data
     *
     * @return SqlInsert
     * @throws SqlException
     */
    public function insert($table, $data)
    {
        return (new SqlInsert($this))->insert($table, $data);
    }

    /**
     * @return SqlSelect
     */
    public function select()
    {
        return new SqlSelect($this);
    }

    /**
     * @param  $table
     * @param  $data
     *
     * @return SqlUpdate
     */
    public function update($table, $data)
    {
        return (new SqlUpdate($this))->update($table, $data);
    }

    /**
     * @param string $table
     *
     * @return SqlDelete
     */
    public function delete($table)
    {
        return (new SqlDelete($this))->from($table);
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
            throw new DbException('Could not connect database');
        }

        if ($connection->connect_errno) {
            $msg = strtr('Db connection error #:number: :msg', [
                ':number' => $connection->connect_errno,
                ':msg'    => $connection->connect_error,
            ]);
            throw new DbException($msg);
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
     * Describe table
     *
     * @param  string $table
     *
     * @return array
     */
    public function describe($table)
    {

        $result = $this->getMaster()->query('describe ' . $table);

        $primary = [];
        $column = [];
        $identity = '';

        while (null != ($row = mysqli_fetch_assoc($result))) {
            $column[$row['Field']] = 1;

            if (strtolower($row['Key']) == 'pri') {
                $primary[$row['Field']] = 1;
            }

            if (strtolower($row['Extra']) == 'auto_increment') {
                $identity = $row['Field'];
            }
        }

        return [
            'column'   => $column,
            'identity' => $identity,
            'primary'  => $primary,
            'name'     => $table,
        ];
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
     * @param string $table
     * @param array  $data
     *
     * @return SqlInsert
     * @throws SqlException
     */
    public function insertDelay($table, $data)
    {
        return (new SqlInsert($this))->insert($table, $data)->setDelay(true);
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

    public function startTransaction()
    {
        if ($this->_inTransaction) {
            return;
        }

        $this->exec('START TRANSACTION');

        // set in transaction
        $this->_inTransaction = true;
    }

    /**
     * Commit transaction
     */
    public function commit()
    {
        if (!$this->_inTransaction) {
            return false;
        }

        $this->exec('COMMIT');
        $this->_inTransaction = false;
    }

    public function rollback()
    {
        $this->exec('ROLLBACK');
        $this->_inTransaction = false;
    }
}