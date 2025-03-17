<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

use Exception;
use http\QueryString;
use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use Keboola\Csv\CsvWriter;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeQueryHistory\Config\Config;
use Keboola\SnowflakeQueryHistory\Config\ConfigDefinition;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Throwable;

class Component extends BaseComponent
{
    private Connection $connection;

    private Fetcher $fetcher;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);

        $this->connection = new Connection(
            $this->getConfig()->getConnectionConfig(),
        );

        $this->fetcher = new Fetcher($this->connection);
    }

    protected function run(): void
    {
        $stateFilePath = $this->getDataDir() . '/in/state.json';
        if (!file_exists($stateFilePath)) {
            throw new Exception("State file not found at path $stateFilePath");
        }

        $decode = new JsonDecode([JsonDecode::ASSOCIATIVE => true]);
        /** @var array<string, string> $stateDecoded */
        $stateDecoded = $decode->decode((string) file_get_contents($stateFilePath), JsonEncoder::FORMAT);

        try {
            $this->connection->query('alter session set timezone = \'UTC\'');
        } catch (Throwable $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }

        $fileName = $this->getDataDir() . '/out/tables/queries.csv';
        $file = fopen($fileName, 'a');
        if ($file === false) {
            throw new Exception("Cannot open file $fileName for writing");
        }

        $csvFile = new CsvWriter($file);

        $stats = [
            'latestEndTime' => null,
            'rowsFetched' => 0,
            'lastProcesssedQueryEndTime' => null,
        ];

        if (isset($stateDecoded['latestEndTime'])) {
            $startTime = $stateDecoded['latestEndTime'];
        } else {
            $startTime = date('Y-m-d H:i:s', strtotime('-1 hour'));
        }

        $this->fetcher->fetchHistory(
            function (array $queryRow, int $rowNumber) use ($csvFile, &$stats): void {
                /** @var array<string, string|int> $queryRow */
                if ($rowNumber === 0) {
                    // most recent query
                    $stats['latestEndTime'] = $queryRow['END_TIME'];
                }

                $stats['rowsFetched'] = $rowNumber;
                $stats['lastProcesssedQueryEndTime'] = $queryRow['END_TIME'];
                $csvFile->writeRow($queryRow);
            },
            [
                'start' => $startTime,
            ],
        );

        // write state
        (new Filesystem())->dumpFile(
            $this->getDataDir() . '/out/state.json',
            (string) json_encode(
                [
                'latestEndTime' => $stats['latestEndTime'],
                ],
            ),
        );

        // write manifest
        (new Filesystem())->dumpFile(
            $this->getDataDir() . '/out/tables/queries.csv.manifest',
            (string) json_encode(
                [
                'primary_key' => ['QUERY_ID'],
                'incremental' => true,
                'columns' => [
                'QUERY_ID',
                'QUERY_TEXT',
                'DATABASE_NAME',
                'SCHEMA_NAME',
                'QUERY_TYPE',
                'SESSION_ID',
                'USER_NAME',
                'ROLE_NAME',
                'WAREHOUSE_NAME',
                'WAREHOUSE_SIZE',
                'WAREHOUSE_TYPE',
                'CLUSTER_NUMBER',
                'QUERY_TAG',
                'EXECUTION_STATUS',
                'ERROR_CODE',
                'ERROR_MESSAGE',
                'START_TIME',
                'END_TIME',
                'BYTES_SCANNED',
                'ROWS_PRODUCED',
                'TOTAL_ELAPSED_TIME',
                'COMPILATION_TIME',
                'EXECUTION_TIME',
                'QUEUED_PROVISIONING_TIME',
                'QUEUED_REPAIR_TIME',
                'QUEUED_OVERLOAD_TIME',
                'TRANSACTION_BLOCKED_TIME',
                'OUTBOUND_DATA_TRANSFER_CLOUD',
                'OUTBOUND_DATA_TRANSFER_REGION',
                'OUTBOUND_DATA_TRANSFER_BYTES',
                'INBOUND_DATA_TRANSFER_CLOUD',
                'INBOUND_DATA_TRANSFER_REGION',
                'INBOUND_DATA_TRANSFER_BYTES',
                'CREDITS_USED_CLOUD_SERVICES',
                ],
                ],
            ),
        );
    }

    public function getConfig(): Config
    {
        /**
 * @var Config $config
*/
        $config = $this->config;

        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
