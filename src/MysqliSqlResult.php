<?php

namespace Neutron\Mysqli;

use Neutron\Db\SqlResultInterface;

/**
 * Class MysqliSqlResult
 *
 * @package Neutron\Mysqli
 */
class MysqliSqlResult implements SqlResultInterface
{

    /**
     * @var mixed
     */
    private $resource;

    public function __construct($resource)
    {
        $this->resource = $resource;
    }

    public function isValid()
    {
        return $this->resource !== false;
    }


    public function fetch()
    {
        $result = [];

        while (null != ($object = $this->resource->fetch_object())) {
            $result[] = $object;
        }

        return $result;
    }
}