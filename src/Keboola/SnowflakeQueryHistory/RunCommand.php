<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 17/07/2017
 * Time: 09:36
 */

namespace  Keboola\SnowflakeQueryHistory;

use Keboola\Csv\CsvFile;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\Output;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Keboola\Db\Import\Exception as SnowflakeImportException;

class RunCommand extends Command
{
    protected function configure()
    {
        $this->setName('run');
        $this->setDescription('Runs the App');
        $this->addArgument('data directory', InputArgument::REQUIRED, 'Data directory');
    }


    protected function execute(InputInterface $input, OutputInterface $consoleOutput)
    {
        $dataDirectory = $input->getArgument('data directory');

        try {
            $this->runExtraction($consoleOutput, $dataDirectory);
            return 0;
        } catch (UserException $e) {
            $consoleOutput->writeln($e->getMessage());
            return 1;
        } catch (\Exception $e) {
            $consoleOutput->writeln("{$e->getMessage()}\n{$e->getTraceAsString()}");
            return 2;
        }
    }

    private function runExtraction(OutputInterface $consoleOutput, $dataDirectory)
    {
        $configFilePath = "$dataDirectory/config.json";
        if (!file_exists($configFilePath)) {
            throw new \Exception("Config file not found at path $configFilePath");
        }

        $stateFilePath = "$dataDirectory/in/state.json";
        if (!file_exists($stateFilePath)) {
            throw new \Exception("State file not found at path $stateFilePath");
        }

        $decode = new JsonDecode(true);
        $configDecoded = $decode->decode(file_get_contents($configFilePath), JsonEncoder::FORMAT);
        $stateDecoded = $decode->decode(file_get_contents($stateFilePath), JsonEncoder::FORMAT);

        $configProcessor = new Processor();
        try {
            $parameters = $configProcessor->processConfiguration(new ConfigDefinition(), [isset($configDecoded['parameters']) ? $configDecoded['parameters'] : []]);
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }

        try {
            $connection = new \Keboola\Db\Import\Snowflake\Connection([
                'host' => $parameters['host'],
                'user' => $parameters['user'],
                'password' => $parameters['#password'],
                'database' => $parameters['database'],
                'warehouse' => $parameters['warehouse'],
            ]);
            $connection->query('alter session set timezone = \'UTC\'');
        } catch (SnowflakeImportException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }

        $consoleOutput->writeln("Fetching query history from {$parameters['host']}");
        $fetcher = new \Keboola\SnowflakeQueryHistory\Fetcher($connection);

        (new Filesystem())->mkdir("$dataDirectory/out/tables");
        $queriesCsvFile = new CsvFile("$dataDirectory/out/tables/queries.csv");
        $queriesCsvFile->openFile('w+');

        $stats = [
            'latestEndTime' => null,
            'rowsFetched' => 0,
            'lastProcesssedQueryEndTime' => null,
        ];

        if (isset($stateDecoded['latestEndTime'])) {
            $startTime = $stateDecoded['latestEndTime'];
            $consoleOutput->writeln(sprintf("Fetching queries completed after %s (UTC) set by last execution.", $startTime));
        } else {
            $startTime = date('Y-m-d H:i:s', strtotime('-1 hour'));
            $consoleOutput->writeln(sprintf("Fetching queries completed in last hour - %s (UTC)", $startTime));
        }

        $fetcher->fetchHistory(
            function ($queryRow, $rowNumber) use ($consoleOutput, $queriesCsvFile, $dataDirectory, &$stats) {
                if ($rowNumber === 0) {
                    // write header
                    $queriesCsvFile->writeRow(array_keys($queryRow));
                    // most recent query
                    $stats['latestEndTime'] =$queryRow['END_TIME'];
                }

                if ($rowNumber > 0 && $rowNumber % 10000 === 0) {
                    $consoleOutput->writeln(sprintf("%d queries fetched total, last processed query end time %s (UTC)", $rowNumber, $queryRow['END_TIME']));
                }

                $stats['rowsFetched'] = $rowNumber;
                $stats['lastProcesssedQueryEndTime'] = $queryRow['END_TIME'];
                $queriesCsvFile->writeRow($queryRow);
            },
            [
                'start' => $startTime,
            ]
        );
        $consoleOutput->writeln(sprintf("%d queries fetched total, last processed query end time %s (UTC)", $stats['rowsFetched'], $stats['lastProcesssedQueryEndTime']));
        $consoleOutput->writeln(sprintf("Latest query end time is %s (UTC). Next execution will fetch queries that have completed later.", $stats['latestEndTime']));

        // write state
        (new Filesystem())->dumpFile("$dataDirectory/out/state.json", json_encode([
            'latestEndTime' => $stats['latestEndTime'],
        ]));

        // write manifest
        (new Filesystem())->dumpFile("$dataDirectory/out/tables/queries.csv.manifest", json_encode([
            'primary_key' => ['QUERY_ID'],
            'incremental' => true,
        ]));
    }
}
