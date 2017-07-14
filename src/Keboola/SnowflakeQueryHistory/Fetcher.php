<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 14/07/2017
 * Time: 12:36
 */


namespace  Keboola\SnowflakeQueryHistory;


use Keboola\Db\Import\Snowflake\Connection;

class Fetcher
{
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function fetchHistory(callable $rowFetchedCallback, array $options = [])
    {
        $start = date('Y-m-d H:i:s',isset($options['start']) ? (int) $options['start'] : strtotime('-1 days'));

        if (isset($options['end'])) {
            $end = date('Y-m-d H:i:s', (int) $options['end']);
        } else {
            $end = null;
        }

        $limit = isset($options['limit']) ? (int) $options['limit'] : 1000;

        do {
            echo "Fetching $start -> $end\n";
            $results = $this->connection->fetchAll(sprintf(
                "select * from table(information_schema.query_history(
                  END_TIME_RANGE_START => TO_TIMESTAMP_LTZ('%s'),
                  END_TIME_RANGE_END => %s,
                  RESULT_LIMIT => %d))
                  order by end_time asc",
                $start,
                $end === null ? 'current_timestamp()' : sprintf('TO_TIMESTAMP_LTZ(\'%s\')', $end),
                $limit
            ));
            $end = $results[0]['END_TIME'];
            foreach ($results as $row) {
                $rowFetchedCallback($row);
            }
            echo "Fetched " . count($results) . " results\n";
        } while (count($results) === $limit);
    }

}