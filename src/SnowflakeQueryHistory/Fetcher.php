<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

use Exception;
use Keboola\SnowflakeDbAdapter\Connection;

abstract class Fetcher
{
    public function __construct(
        private readonly Connection $connection,
    ) {
    }

    /**
     * @param array<string, int|string> $options
     */
    public function fetchHistory(callable $rowFetchedCallback, array $options = []): void
    {
        if (!isset($options['start'])) {
            throw new Exception('start must be set');
        }
        $limit = isset($options['limit']) ? (int) $options['limit'] : 1000;
        $start = (string) $options['start'];
        $end = null;
        $rowNumber = 0;
        do {
            /** @var array<int, array<string, int|string>> $results */
            $results = $this->connection->fetchAll(
                $this->buildQuery($start, $end, $limit),
            );
            if (empty($results)) {
                break;
            }
            foreach ($results as $row) {
                $rowFetchedCallback($row, $rowNumber);
                $rowNumber++;
            }
            // get the last value with lowest END_TIME
            /** @var array<int, array<string, int|string>> $result */
            $result = array_values(array_slice(self::filterRowsWithValidEndTime($results), -1));
            $end = (string) $result[0]['END_TIME'];
        } while (count($results) === $limit);
    }

    abstract protected function buildQuery(string $start, ?string $end, int $limit): string;

    protected function getQueryColumns(): string
    {
        return 'query_id,
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
            CLUSTER_NUMBER,
            query_tag,
            execution_status,
            error_code,
            error_message,
            start_time,
            end_time,
            BYTES_SCANNED,
            ROWS_PRODUCED,
            total_elapsed_time,
            compilation_time,
            execution_time,
            queued_provisioning_time,
            queued_repair_time,
            queued_overload_time,
            transaction_blocked_time,
            outbound_data_transfer_cloud,
            outbound_data_transfer_region,
            outbound_data_transfer_bytes,
            INBOUND_DATA_TRANSFER_CLOUD,
            INBOUND_DATA_TRANSFER_REGION,
            INBOUND_DATA_TRANSFER_BYTES,
            CREDITS_USED_CLOUD_SERVICES';
    }

    /**
     * @param array<int, array<string, int|string>> $results
     * @return array<int, array<string, int|string>>
     */

    public static function filterRowsWithValidEndTime(array $results): array
    {
        return array_filter(
            $results,
            function ($row) {
                return $row['END_TIME'] !== '1970-01-01 00:00:00';
            },
        );
    }
}
