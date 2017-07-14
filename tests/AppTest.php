<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 13/07/2017
 * Time: 14:37
 */

date_default_timezone_set('UTC');

class AppTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @var  \Keboola\Db\Import\Snowflake\Connection
     */
    private $connection;

    public function setUp()
    {
        $this->connection = new \Keboola\Db\Import\Snowflake\Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
        ]);
        $this->connection->query('alter session set timezone = \'UTC\'');
    }

    public function testFetch()
    {
        $fetcher = new \Keboola\SnowflakeQueryHistory\Fetcher($this->connection);

        // run test queries which should be later fetched
        $query = sprintf("SELECT '%s'", rand());
        $queryRepeatCount = 4;
        for ($i = 0; $i <= $queryRepeatCount; $i++) {
            $this->connection->query($query);
        }

        $results = [];
        $rowFetched = function ($row) use(&$results) {
            $results[] = $row;
        };
        $fetcher->fetchHistory($rowFetched, [
            'limit' => 2,
            'start' => strtotime('-1 minute'),
        ]);

        $matches = array_filter($results, function ($row) use ($query) {
            return $row['QUERY_TEXT'] === $query;
        });
        $this->assertCount($queryRepeatCount, $matches);
    }

}