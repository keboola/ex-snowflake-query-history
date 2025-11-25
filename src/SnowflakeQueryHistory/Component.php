<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

use Exception;
use Keboola\Component\BaseComponent;
use Keboola\Csv\CsvWriter;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
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

    private Fetcher $accountFetcher;

    private ReaderAccountUsageFetcher $readerAccountFetcher;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);

        $this->connection = new Connection(
            $this->getConfig()->getConnectionConfig(),
        );

        $this->accountFetcher = new AccountUsageFetcher($this->connection);
        $this->readerAccountFetcher = new ReaderAccountUsageFetcher($this->connection);
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

        $this->getLogger()->info("Fetching query history from {$this->getConfig()->getHost()}");

        $accountQueries = new CsvWriter($this->getDataDir() . '/out/tables/queries.csv');
        $readerAccountQueries = new CsvWriter($this->getDataDir() . '/out/tables/queries_reader_account.csv');

        $stats = [
            'latestEndTime' => null,
            'rowsFetched' => 0,
            'lastProcesssedQueryEndTime' => null,
            'readerAccountLatestEndTime' => null,
            'readerAccountRowsFetched' => 0,
            'readerAccountLastProcessedQueryEndTime' => null,
        ];

        $stateEncode = [
            'latestEndTime' => null,
            'readerAccountLatestEndTime' => null,
        ];

        // INFORMATION_SCHEMA.QUERY_HISTORY()
        if (isset($stateDecoded['latestEndTime'])) {
            $startTime = $stateDecoded['latestEndTime'];
            $stateEncode['latestEndTime'] = $startTime;

            $this->getLogger()->info(sprintf(
                'INFORMATION_SCHEMA: Fetching queries completed after %s (UTC) set by last execution.',
                $startTime,
            ));
        } else {
            $startTime = date('Y-m-d H:i:s', strtotime('-1 hour'));
            $this->getLogger()->info(sprintf(
                'INFORMATION_SCHEMA: Fetching queries completed in last hour - %s (UTC)',
                $startTime,
            ));
        }

        $this->accountFetcher->fetchHistory(
            function (array $queryRow, int $rowNumber) use ($accountQueries, &$stats): void {
                /** @var array<string, string|int> $queryRow */
                if ($rowNumber === 0) {
                    // most recent query
                    $stats['latestEndTime'] = $queryRow['END_TIME'];
                }

                if ($rowNumber > 0 && $rowNumber % 10000 === 0) {
                    $this->getLogger()->info(sprintf(
                        'INFORMATION_SCHEMA: %d queries fetched total, last processed query end time %s (UTC)',
                        $rowNumber,
                        $queryRow['END_TIME'],
                    ));
                }

                $stats['rowsFetched'] = $rowNumber;
                $stats['lastProcesssedQueryEndTime'] = $queryRow['END_TIME'];
                $accountQueries->writeRow($queryRow);
            },
            [
                'start' => $startTime,
            ],
        );

        $this->getLogger()->info(sprintf(
            'INFORMATION_SCHEMA: %d queries fetched total, last processed query end time %s (UTC)',
            $stats['rowsFetched'],
            $stats['lastProcesssedQueryEndTime'],
        ));

        $this->getLogger()->info(sprintf(
            'INFORMATION_SCHEMA: Latest query end time is %s (UTC). Next execution will fetch queries that have completed later.', // phpcs:ignore
            $stats['latestEndTime'],
        ));

        $stateEncode['latestEndTime'] = $stats['latestEndTime'];
        $this->writeManifest($this->getDataDir() . '/out/tables/queries.csv.manifest');

        // READER_ACCOUNT_USAGE.QUERY_HISTORY
        if (isset($stateDecoded['readerAccountLatestEndTime'])) {
            $readerAccountStartTime = $stateDecoded['readerAccountLatestEndTime'];
            $stateEncode['readerAccountLatestEndTime'] = $readerAccountStartTime;

            $this->getLogger()->info(sprintf(
                'READER_ACCOUNT_USAGE: Fetching queries completed after %s (UTC) set by last execution.',
                $readerAccountStartTime,
            ));
        } else {
            $readerAccountStartTime = date('Y-m-d H:i:s', strtotime('-1 hour'));
            $this->getLogger()->info(sprintf(
                'READER_ACCOUNT_USAGE: Fetching queries completed in last hour - %s (UTC)',
                $readerAccountStartTime,
            ));
        }

        try {
            $this->readerAccountFetcher->fetchHistory(
                function (array $queryRow, int $rowNumber) use ($readerAccountQueries, &$stats): void {
                    /** @var array<string, string|int> $queryRow */
                    if ($rowNumber === 0) {
                        // most recent query
                        $stats['readerAccountLatestEndTime'] = $queryRow['END_TIME'];
                    }

                    if ($rowNumber > 0 && $rowNumber % 10000 === 0) {
                        $this->getLogger()->info(sprintf(
                            'READER_ACCOUNT_USAGE: %d queries fetched total, last processed query end time %s (UTC)',
                            $rowNumber,
                            $queryRow['END_TIME'],
                        ));
                    }

                    $stats['readerAccountRowsFetched'] = $rowNumber;
                    $stats['readerAccountLastProcessedQueryEndTime'] = $queryRow['END_TIME'];
                    $readerAccountQueries->writeRow($queryRow);
                },
                [
                    'start' => $readerAccountStartTime,
                ],
            );

            $this->getLogger()->info(sprintf(
                'READER_ACCOUNT_USAGE: %d queries fetched total, last processed query end time %s (UTC)',
                $stats['readerAccountRowsFetched'],
                $stats['readerAccountLastProcessedQueryEndTime'],
            ));

            $this->getLogger()->info(sprintf(
                'READER_ACCOUNT_USAGE: Latest query end time is %s (UTC). Next execution will fetch queries that have completed later.', // phpcs:ignore
                $stats['readerAccountLatestEndTime'],
            ));
        } catch (RuntimeException $e) {
            $this->getLogger()->error($e->getMessage());
        }

        $stateEncode['readerAccountLatestEndTime'] = $stats['readerAccountLatestEndTime'];
        $this->writeManifest($this->getDataDir() . '/out/tables/queries_reader_account.csv.manifest');

        (new Filesystem())->dumpFile(
            $this->getDataDir() . '/out/state.json',
            (string) json_encode($stateEncode),
        );
    }

    private function writeManifest(string $path): void
    {
        (new Filesystem())->dumpFile(
            $path,
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
        /** @var Config $config */
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
