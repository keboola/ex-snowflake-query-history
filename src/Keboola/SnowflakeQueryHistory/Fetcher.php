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
                "select  
                  query_id,
                  substr(query_text, 0, 500000) as query_text,
                  database_name,
                  schema_name,
                  query_type,
                  session_id,
                  user_name,
                  role_name,
                  warehouse_name,
                  warehouse_size,
                  warehouse_type,
                  query_tag,
                  execution_status,
                  error_code,
                  error_message,
                  start_time,
                  end_time,
                  total_elapsed_time,
                  compilation_time,
                  execution_time,
                  queued_provisioning_time,
                  queued_repair_time,
                  queued_overload_time,
                  transaction_blocked_time,
                  outbound_data_transfer_cloud,
                  outbound_data_transfer_region,
                  outbound_data_transfer_bytes
                  from table(information_schema.query_history(
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
            $end = array_values(array_slice(self::filterRowsWithValidEndTime($results), -1))[0]['END_TIME'];
        } while (count($results) === $limit);
    }

    public static function filterRowsWithValidEndTime(array $results): array
    {
        return array_filter(
            $results,
            function ($row) {
                return $row['END_TIME'] !== '1970-01-01 00:00:00';
            }
        );
    }
}
