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
     * @var \mysqli_result
     */
    private $resource;

    /**
     * SqlResult constructor.
     *
     * @param $resource
     */
    public function __construct($resource)
    {
        $this->resource = $resource;
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