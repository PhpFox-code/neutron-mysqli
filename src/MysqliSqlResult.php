<?php

namespace Phpfox\Mysqli;

use Phpfox\Db\SqlResultInterface;

/**
 * Class MysqliSqlResult
 *
 * @package Phpfox\Mysqli
 */
class MysqliSqlResult implements SqlResultInterface
{
    /**
     * @var bool
     */
    private $valid = false;

    /**
     * @var \mysqli_result
     */
    private $resource;

    /**
     * @var string
     */
    private $error;

    /**
     * SqlResult constructor.
     *
     * @param $valid
     * @param $resource
     * @param $error
     */
    public function __construct($valid, $resource, $error)
    {
        $this->valid = $valid;
        $this->resource = $resource;
        $this->error = $error;
    }

    /**
     * @inheritdoc
     */
    public function isValid()
    {
        return $this->valid;
    }

    /**
     * @inheritdoc
     */
    public function fetch($model = null)
    {
        $result = [];

        if (FETCH_OBJECT == $model || $model == null) {
            while ($object = $this->resource->fetch_object()) {
                $result[] = $object;
            }
        } elseif ($model == FETCH_ARRAY) {
            while ($object = $this->resource->fetch_assoc()) {
                $result[] = $object;
            }
        }
        return $result;
    }
}