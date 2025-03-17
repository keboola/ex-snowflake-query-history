<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

use Keboola\SnowflakeDbAdapter\Connection;
use PHPUnit\Framework\TestCase;

class FetcherTest extends TestCase
{
    private Connection $connection;

    public function setUp(): void
    {
        $this->connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD') ?? '',
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'keyPair' => getenv('SNOWFLAKE_KEYPAIR'),
        ]);

        $this->connection->query('alter session set timezone = \'UTC\'');
    }

    public function testFetch(): void
    {
        $fetcher = new Fetcher($this->connection);

        // run test queries which should be later fetched
        $currentTimestamp = $this->connection
            ->fetchAll('select current_timestamp() as current_timestamp')[0]['CURRENT_TIMESTAMP'];
        $query = sprintf("SELECT '%s'", rand());
        $queryRepeatCount = 8;
        for ($i = 0; $i < $queryRepeatCount; $i++) {
            $this->connection->query($query);
        }

        // it looks like there is some delay before queries are searchable
        sleep(190);

        $results = [];
        $rowFetched = function ($row) use (&$results): void {
            $results[] = $row;
        };
        $fetcher->fetchHistory(
            $rowFetched,
            [
            'limit' => 3,
            'start' => $currentTimestamp,
            ],
        );

        $matches = array_filter(
            $results,
            function ($row) use ($query) {
                return $row['QUERY_TEXT'] === $query;
            },
        );

        // there are duplicates of first queries on page
        $ids = array_unique(
            array_map(
                function ($row) {
                    return $row['QUERY_ID'];
                },
                $matches,
            ),
        );

        $this->assertEquals($queryRepeatCount, count($ids));
    }
}
