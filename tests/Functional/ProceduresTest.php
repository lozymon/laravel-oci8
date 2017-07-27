<?php

use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Support\Facades\DB;
use Yajra\Oci8\Connectors\OracleConnector;
use Yajra\Oci8\Oci8Connection;

class ProceduresTest extends PHPUnit_Framework_TestCase
{

    public function testSimpleProcedureExecution()
    {
        /** @var Oci8Connection $connection */
        $connection = $this->createConnection();
        $this->assertInstanceOf(Oci8Connection::class, $connection);

        $command = "
            CREATE OR REPLACE PROCEDURE demo(p1 IN NUMBER, p2 OUT NUMBER) AS
            BEGIN
                p2 := p1 * 2;
            END;
        ";

        $connection->getPdo()->exec($command);

        $procedureName = 'demo';

        $input  = 2;
        $output = 0;

        $bindings = [
            'p1' => $input,
            'p2' => &$output,
        ];

        $this->assertTrue($connection->executeProcedure($procedureName, $bindings));
        $this->assertSame(4, $output);
    }

    private function createConnection()
    {
        $capsule = new Capsule;

        $manager = $capsule->getDatabaseManager();
        $manager->extend('oracle', function ($config) {
            $connector  = new OracleConnector();
            $connection = $connector->connect($config);
            $db         = new Oci8Connection($connection, $config["database"], $config["prefix"]);

            // set oracle session variables
            $sessionVars = [
                'NLS_TIME_FORMAT'         => 'HH24:MI:SS',
                'NLS_DATE_FORMAT'         => 'YYYY-MM-DD HH24:MI:SS',
                'NLS_TIMESTAMP_FORMAT'    => 'YYYY-MM-DD HH24:MI:SS',
                'NLS_TIMESTAMP_TZ_FORMAT' => 'YYYY-MM-DD HH24:MI:SS TZH:TZM',
                'NLS_NUMERIC_CHARACTERS'  => '.,',
            ];

            // Like Postgres, Oracle allows the concept of "schema"
            if (isset($config['schema'])) {
                $sessionVars['CURRENT_SCHEMA'] = $config['schema'];
            }

            $db->setSessionVars($sessionVars);

            return $db;
        });

        $capsule->addConnection([
            'driver'       => 'oracle',
            'host'         => 'localhost',
            'database'     => 'xe',
            'service_name' => 'xe',
            'username'     => 'system',
            'password'     => 'oracle',
            'prefix'       => '',
            'port'         => 49161
        ]);

        return $capsule->getConnection();
    }
}
