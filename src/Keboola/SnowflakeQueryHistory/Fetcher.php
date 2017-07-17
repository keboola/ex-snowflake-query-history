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
        if (!isset($options['start'])) {
            throw new \Exception('start must be set');
        }
        $limit = isset($options['limit']) ? (int) $options['limit'] : 1000;

        $end = null;
        $rowNumber = 0;
        do {
            $results = $this->connection->fetchAll(sprintf(
                "select * from table(information_schema.query_history(
                  END_TIME_RANGE_START => TO_TIMESTAMP_LTZ('%s'),
                  END_TIME_RANGE_END => %s,
                  RESULT_LIMIT => %d))
                  order by end_time DESC",
                $options['start'],
                $end === null ? 'current_timestamp()' : sprintf('TO_TIMESTAMP_LTZ(\'%s\')', $end),
                $limit
            ));
            foreach ($results as $row) {
                $rowFetchedCallback($row, $rowNumber);
                $rowNumber++;
            }
            // get the last value with lowest END_TIME
            $end = array_values(array_slice($results, -1))[0]['END_TIME'];
        } while (count($results) === $limit);
    }
}
