<?php

namespace Phpfox\Mysqli;


use Phpfox\Db\SqlSelect;
use Phpfox\Db\SqlUpdate;

class MysqliAdapterTest extends \PHPUnit_Framework_TestCase
{
    public function testTransaction()
    {
        $adapter = $this->getAdapter();

        $this->assertNotNull($adapter->getMaster());

        $this->assertFalse($adapter->inTransaction());

        $adapter->begin();

        $this->assertTrue($adapter->inTransaction());

        $adapter->commit();

        $this->assertFalse($adapter->inTransaction());

        $adapter->begin();

        $this->assertTrue($adapter->inTransaction());

        $adapter->rollback();

        $this->assertFalse($adapter->inTransaction());
    }

    public function getAdapter()
    {
        return new MysqliAdapter([
            'host'     => '127.0.0.1',
            'port'     => 3306,
            'user'     => 'root',
            'password' => 'namnv123',
            'database' => 'phpfox_unitest',
        ]);
    }

    public function testSqlSelect()
    {
        $adapter = $this->getAdapter();

        $sqlSelect = new SqlSelect($adapter);

        $sqlResult = $sqlSelect->select('*')->select('user_id')
            ->from('phpfox_user')->where('user_id=1')->execute();
        
        $this->assertTrue($sqlResult->isValid());

        $sqlResult->fetch();
    }

    public function testSqlInsert()
    {
        $adapter = $this->getAdapter();

        $sqlUpdate = new SqlUpdate($adapter);

        $sqlUpdate->update('phpfox_user')->values(['username' => 'namnv'])
            ->where(['user_id=?' => 1])->execute();

        echo $sqlUpdate->prepare();


    }
}
